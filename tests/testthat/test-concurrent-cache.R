.concurrent_cache_fixture <- function(.local_envir = parent.frame()) {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home,
    dshpc.worker_autostart = FALSE, dshpc.executor_backend = "embedded",
    dshpc.step_cache = TRUE, dshpc.max_parallel_steps = 2L,
    dshpc.max_retries = 0L, dshpc.cpu_slots = 8L,
    dshpc.node_memory_mb = 2048L, dshpc.memory_reserve_mb = 0L,
    dshpc.gpu_count = 0L), .local_envir = .local_envir)
  for (runner in c("cache_seed", "cache_sibling", "cache_consume")) {
    yaml::write_yaml(list(name = runner, command = "/bin/true",
      args_template = character(0), allowed_params = character(0),
      resources = list(memory_mb = 32L, cpu_slots = 1L)),
      file.path(home, "runners", paste0(runner, ".yml")))
  }
  db <- dsHPC:::.db_connect()
  withr::defer({
    dsHPC:::.db_close(db)
    cleanup_test_home(home)
  }, envir = .local_envir)
  list(home = home, db = db)
}

.concurrent_cache_spec <- function(nodes, visibility = "global") {
  spec <- dsHPC:::.validate_job_spec(list(label = "dsHPC_test",
    visibility = visibility, reuse_fingerprint = strrep("a", 64L),
    dag = list(nodes = nodes)))
  spec$.dshpc_provider <- "dsHPC"
  spec$.dshpc_runtime_revision <- dsHPC:::.dshpc_runtime_revision(spec)
  spec$.dshpc_runtime_identity <- dsHPC:::.dshpc_runtime_identity(spec, "dsHPC")
  spec
}

.concurrent_cache_node <- function(runner, inputs = NULL) {
  list(type = "run_artifact", plane = "artifact", runner = runner,
    inputs = inputs)
}

.concurrent_cache_source <- function(db, job_id, spec, complete = FALSE) {
  dsHPC:::.store_create_job(db, job_id, "owner", spec, length(spec$steps))
  dsHPC:::.store_update_job(db, job_id, state = "RUNNING")
  step_hash <- dsHPC:::.step_cache_hash(spec$steps[[1]],
    reuse_fingerprint = spec$reuse_fingerprint)
  dsHPC:::.store_update_step(db, job_id, 1L,
    state = "running", step_hash = step_hash)
  if (complete) .concurrent_cache_complete_source(db, job_id)
  invisible(step_hash)
}

.concurrent_cache_complete_source <- function(db, job_id) {
  step_dir <- dsHPC:::.ensure_step_dir(job_id, 1L)
  path <- file.path(step_dir, "output", "seed.txt")
  writeLines("cached-seed", path)
  dsHPC:::.db_register_output(db, job_id, 1L, "seed.txt", "artifact_file",
    path, size_bytes = file.info(path)$size, safe_for_client = FALSE)
  dsHPC:::.store_update_step(db, job_id, 1L, state = "done",
    output_ref = file.path("artifacts", job_id, "step_001", "output"))
  dsHPC:::.store_update_job(db, job_id, state = "FINISHED")
}

test_that("a DAG cache waiter retains sibling leases and unlocks downstream input", {
  fixture <- .concurrent_cache_fixture()
  db <- fixture$db
  source <- .concurrent_cache_spec(list(seed = .concurrent_cache_node("cache_seed")))
  .concurrent_cache_source(db, "job_cache_source", source)
  target <- .concurrent_cache_spec(list(
    seed = .concurrent_cache_node("cache_seed"),
    sibling = .concurrent_cache_node("cache_sibling"),
    consume = .concurrent_cache_node("cache_consume", "seed")))
  dsHPC:::.store_create_job(db, "job_cache_dag", "owner", target, 3L)
  dsHPC:::.store_update_job(db, "job_cache_dag", state = "RUNNING")
  launched <- list()
  testthat::local_mocked_bindings(.run_artifact_step = function(
    db, job_id, step_index, step, step_dir, input_dir, ...) {
      launched[[length(launched) + 1L]] <<- list(index = step_index,
        input_dir = input_dir)
      invisible(TRUE)
    }, .package = "dsHPC")

  dsHPC:::.executor_run_dag(db, "job_cache_dag", target)
  rows <- dsHPC:::.dag_steps(db, "job_cache_dag")
  expect_identical(rows$state, c("waiting_cache", "running", "pending"))
  expect_identical(dsHPC:::.store_get_job(db, "job_cache_dag")$state, "RUNNING")
  expect_true(is.na(rows$resource_json[1]))
  expect_false(is.na(rows$resource_json[2]))
  expect_true(dsHPC:::.step_cache_waiting_active(db, "job_cache_dag", 1L))
  expect_equal(DBI::dbGetQuery(db,
    "SELECT amount FROM resource_leases
      WHERE job_id = 'job_cache_dag' AND resource = 'cpu_slots'")$amount, 1)
  dsHPC:::.executor_run_dag(db, "job_cache_dag", target)
  expect_length(launched, 1L)
  expect_equal(launched[[1]]$index, 2L)

  .concurrent_cache_complete_source(db, "job_cache_source")
  dsHPC:::.executor_run_dag(db, "job_cache_dag", target)
  rows <- dsHPC:::.dag_steps(db, "job_cache_dag")
  expect_identical(rows$state, c("done", "running", "running"))
  expect_identical(rows$cache_hit, c(1L, 0L, 0L))
  expect_identical(rows$cache_source_job_id[1], "job_cache_source")
  expect_identical(dsHPC:::.store_get_job(db, "job_cache_dag")$step_index, 1L)
  expect_length(launched, 2L)
  expect_equal(launched[[2]]$index, 3L)
  expect_identical(readLines(file.path(launched[[2]]$input_dir, "seed.txt")),
    "cached-seed")
  expect_true(startsWith(launched[[2]]$input_dir,
    normalizePath(file.path(fixture$home, "artifacts", "job_cache_dag"),
      winslash = "/", mustWork = TRUE)))
  outputs <- DBI::dbGetQuery(db,
    "SELECT safe_for_client, reuse_class FROM outputs
      WHERE job_id = 'job_cache_dag'")
  expect_identical(outputs$safe_for_client, 0L)
  expect_identical(outputs$reuse_class, "internal_only")
  expect_equal(DBI::dbGetQuery(db,
    "SELECT amount FROM resource_leases
      WHERE job_id = 'job_cache_dag' AND resource = 'cpu_slots'")$amount, 2)

  for (index in 2:3) {
    dsHPC:::.worker_finalize_artifact_step(db, "job_cache_dag", index,
      target$steps[[index]]$runner, 0L)
  }
  dsHPC:::.executor_run_dag(db, "job_cache_dag", target)
  expect_identical(dsHPC:::.store_get_job(db, "job_cache_dag")$state, "FINISHED")
})

test_that("private DAG steps execute without consuming shared cache entries", {
  fixture <- .concurrent_cache_fixture()
  db <- fixture$db
  nodes <- list(seed = .concurrent_cache_node("cache_seed"))
  source <- .concurrent_cache_spec(nodes)
  .concurrent_cache_source(db, "job_global_seed", source, complete = TRUE)
  private <- .concurrent_cache_spec(nodes, visibility = "private")
  dsHPC:::.store_create_job(db, "job_private_cache", "owner", private, 1L)
  dsHPC:::.store_update_job(db, "job_private_cache", state = "RUNNING")
  launched <- 0L
  testthat::local_mocked_bindings(.run_artifact_step = function(...) {
    launched <<- launched + 1L
    invisible(TRUE)
  }, .package = "dsHPC")
  dsHPC:::.executor_run_dag(db, "job_private_cache", private)
  step <- dsHPC:::.dag_steps(db, "job_private_cache")
  expect_equal(launched, 1L)
  expect_identical(step$state, "running")
  expect_identical(step$cache_hit, 0L)
  expect_true(is.na(step$step_hash))
  expect_true(is.na(step$cache_source_job_id))
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM outputs WHERE job_id = 'job_private_cache'")$n, 0L)
})

test_that("sequential DAG mode waits for cache before dispatching later roots", {
  fixture <- .concurrent_cache_fixture()
  withr::local_options(list(dshpc.max_parallel_steps = 1L))
  db <- fixture$db
  source <- .concurrent_cache_spec(list(seed = .concurrent_cache_node("cache_seed")))
  .concurrent_cache_source(db, "job_sequential_cache_source", source)
  target <- .concurrent_cache_spec(list(
    seed = .concurrent_cache_node("cache_seed"),
    sibling = .concurrent_cache_node("cache_sibling")))
  dsHPC:::.store_create_job(db, "job_sequential_cache", "owner", target, 2L)
  dsHPC:::.store_update_job(db, "job_sequential_cache", state = "RUNNING")
  launched <- 0L
  testthat::local_mocked_bindings(.run_artifact_step = function(...) {
    launched <<- launched + 1L
    invisible(TRUE)
  }, .package = "dsHPC")
  dsHPC:::.executor_run_dag(db, "job_sequential_cache", target)
  expect_equal(launched, 0L)
  expect_identical(dsHPC:::.dag_steps(db, "job_sequential_cache")$state,
    c("waiting_cache", "pending"))
  .concurrent_cache_complete_source(db, "job_sequential_cache_source")
  dsHPC:::.executor_run_dag(db, "job_sequential_cache", target)
  expect_equal(launched, 1L)
  expect_identical(dsHPC:::.dag_steps(db, "job_sequential_cache")$state,
    c("done", "running"))
})

test_that("DAG failure draining releases allocations with an unreadable spec", {
  fixture <- .concurrent_cache_fixture()
  db <- fixture$db
  spec <- .concurrent_cache_spec(list(
    a = .concurrent_cache_node("cache_seed"),
    b = .concurrent_cache_node("cache_sibling")))
  dsHPC:::.store_create_job(db, "job_invalid_drain", "owner", spec, 2L)
  dsHPC:::.store_update_job(db, "job_invalid_drain", state = "RUNNING")
  testthat::local_mocked_bindings(.run_artifact_step = function(...) TRUE,
    .package = "dsHPC")
  dsHPC:::.executor_run_dag(db, "job_invalid_drain", spec)
  DBI::dbExecute(db,
    "UPDATE jobs SET spec_json = '{invalid' WHERE job_id = 'job_invalid_drain'")
  expect_equal(dsHPC:::.scheduler_running_usage(db)$cpu_slots, 2L)
  expect_equal(dsHPC:::.scheduler_running_usage(db,
    unit_key = "another-unit")$memory_mb, 64L)
  expect_no_error(dsHPC:::.dag_fail_step(db, "job_invalid_drain", 1L,
    "Durable job specification is invalid", retry = FALSE))
  expect_identical(dsHPC:::.dag_steps(db, "job_invalid_drain")$state,
    c("failed", "running"))
  expect_equal(dsHPC:::.scheduler_running_usage(db)$cpu_slots, 1L)
  expect_equal(DBI::dbGetQuery(db,
    "SELECT amount FROM resource_leases
      WHERE job_id = 'job_invalid_drain' AND resource = 'cpu_slots'")$amount, 1)
  expect_no_error(dsHPC:::.dag_fail_step(db, "job_invalid_drain", 2L,
    "Second failure", retry = FALSE))
  expect_no_error(dsHPC:::.dag_finish_job(db, "job_invalid_drain"))
  job <- dsHPC:::.store_get_job(db, "job_invalid_drain")
  expect_identical(job$state, "FAILED")
  expect_identical(job$error_message, "Durable job specification is invalid")
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM resource_leases
      WHERE job_id = 'job_invalid_drain'")$n, 0L)
})

test_that("unknown migrated DAG reservations block admission until attempts settle", {
  fixture <- .concurrent_cache_fixture()
  db <- fixture$db
  spec <- .concurrent_cache_spec(list(
    a = .concurrent_cache_node("cache_seed"),
    b = .concurrent_cache_node("cache_sibling")))
  dsHPC:::.store_create_job(db, "job_unknown_allocation", "owner", spec, 2L)
  dsHPC:::.store_update_job(db, "job_unknown_allocation", state = "RUNNING")
  testthat::local_mocked_bindings(.run_artifact_step = function(...) TRUE,
    .package = "dsHPC")
  dsHPC:::.executor_run_dag(db, "job_unknown_allocation", spec)
  DBI::dbExecute(db,
    "UPDATE steps SET resource_json = NULL WHERE job_id = 'job_unknown_allocation'")
  DBI::dbExecute(db,
    "UPDATE jobs SET spec_json = '{invalid' WHERE job_id = 'job_unknown_allocation'")
  expect_identical(dsHPC:::.scheduler_running_usage(db)$cpu_slots, Inf)
  dsHPC:::.store_create_job(db, "job_blocked_allocation", "owner", spec, 2L)
  decision <- dsHPC:::.scheduler_can_start_job(db, "job_blocked_allocation", spec)
  expect_false(decision$ok)
  expect_identical(decision$reason, "unresolved_dag_resources")
  expect_no_error(dsHPC:::.dag_fail_step(db, "job_unknown_allocation", 1L,
    "Invalid legacy specification", retry = FALSE))
  expect_equal(DBI::dbGetQuery(db,
    "SELECT amount FROM resource_leases
      WHERE job_id = 'job_unknown_allocation' AND resource = 'cpu_slots'")$amount, 2)
  expect_no_error(dsHPC:::.dag_fail_step(db, "job_unknown_allocation", 2L,
    "Invalid legacy specification", retry = FALSE))
  dsHPC:::.dag_finish_job(db, "job_unknown_allocation")
  expect_true(dsHPC:::.scheduler_can_start_job(db, "job_blocked_allocation", spec)$ok)
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM resource_leases
      WHERE job_id = 'job_unknown_allocation'")$n, 0L)
})
