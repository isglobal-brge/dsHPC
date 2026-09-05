wait_for_durable_exit <- function(home, job_id, step_index,
                                  timeout_secs = 5) {
  step_dir <- file.path(home, "artifacts", job_id,
    sprintf("step_%03d", as.integer(step_index)))
  deadline <- Sys.time() + timeout_secs
  repeat {
    if (file.exists(file.path(step_dir, "exit_code"))) {
      Sys.sleep(0.05)
      return(step_dir)
    }
    if (Sys.time() > deadline) {
      stop("Timed out waiting for durable runner exit marker.", call. = FALSE)
    }
    Sys.sleep(0.02)
  }
}

forget_in_memory_runner_state <- function() {
  registry <- dsHPC:::.proc_registry
  keys <- ls(registry, all.names = TRUE)
  if (length(keys) > 0L) rm(list = keys, envir = registry)
  invisible(TRUE)
}

test_that("unreadable pending specs fail with a safe durable event", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(
    dshpc.home = home,
    dshpc.worker_autostart = FALSE
  ))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  dsHPC:::.store_create_job(db, "job_unreadable_pending", "owner",
    make_test_spec(), 1L)
  DBI::dbExecute(db,
    "UPDATE jobs SET spec_json = '{invalid json' WHERE job_id = ?",
    params = list("job_unreadable_pending"))

  dsHPC:::.worker_dispatch(db)

  job <- dsHPC:::.store_get_job(db, "job_unreadable_pending")
  leases <- DBI::dbGetQuery(db,
    "SELECT job_id FROM resource_leases WHERE job_id = ?",
    params = list("job_unreadable_pending"))
  event <- DBI::dbGetQuery(db,
    "SELECT event, details_json FROM events
     WHERE job_id = ? AND event = 'durable_spec_invalid'",
    params = list("job_unreadable_pending"))
  expect_identical(job$state, "FAILED")
  expect_identical(job$error_message,
    "Durable job specification is invalid")
  expect_equal(nrow(leases), 0L)
  expect_identical(event$event, "durable_spec_invalid")
  expect_true(is.na(event$details_json))
})

test_that("invalid running unit snapshots fail and release leases", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(
    dshpc.home = home,
    dshpc.worker_autostart = FALSE
  ))

  spec <- make_test_spec()
  spec$.dshpc_unit <- dsHPC:::.dshpc_site_default_snapshot("dsHPC_test")
  spec$.dshpc_unit$config_seal <- strrep("0", 64L)
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  dsHPC:::.store_create_job(db, "job_invalid_running", "owner", spec, 1L)
  dsHPC:::.store_update_job(db, "job_invalid_running", state = "RUNNING",
    step_index = 1L, worker_pid = NA_integer_)
  dsHPC:::.store_update_step(db, "job_invalid_running", 1L,
    state = "running")
  dsHPC:::.scheduler_acquire_leases(db, "job_invalid_running", list(
    plan = list(memory_mb = 1L, cpu_slots = 1L),
    gpu_devices = character(0)))

  dsHPC:::.worker_reap(db)

  job <- dsHPC:::.store_get_job(db, "job_invalid_running")
  leases <- DBI::dbGetQuery(db,
    "SELECT job_id FROM resource_leases WHERE job_id = ?",
    params = list("job_invalid_running"))
  event <- DBI::dbGetQuery(db,
    "SELECT event, details_json FROM events
     WHERE job_id = ? AND event = 'durable_spec_invalid'",
    params = list("job_invalid_running"))
  expect_identical(job$state, "FAILED")
  expect_identical(job$error_message,
    "Durable job specification is invalid")
  expect_equal(nrow(leases), 0L)
  expect_identical(event$event, "durable_spec_invalid")
  expect_true(is.na(event$details_json))
})

test_that("transient spec reads leave pending jobs retryable", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(
    dshpc.home = home,
    dshpc.worker_autostart = FALSE
  ))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  dsHPC:::.store_create_job(db, "job_transient_spec_read", "owner",
    make_test_spec(), 1L)
  testthat::local_mocked_bindings(
    .store_get_spec = function(...) stop("database is locked", call. = FALSE),
    .package = "dsHPC")

  dsHPC:::.worker_dispatch(db)

  job <- dsHPC:::.store_get_job(db, "job_transient_spec_read")
  event <- DBI::dbGetQuery(db,
    "SELECT event FROM events
     WHERE job_id = ? AND event = 'durable_spec_invalid'",
    params = list("job_transient_spec_read"))
  expect_identical(job$state, "PENDING")
  expect_equal(nrow(event), 0L)
})

test_that("private multi-step DAG survives worker and database restarts", {
  skip_on_os("windows")
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(
    dshpc.home = home,
    dshpc.step_cache = TRUE,
    dshpc.executor_backend = "embedded",
    dshpc.max_retries = 0L,
    dshpc.memory_reserve_mb = 0L,
    dshpc.node_memory_mb = 1024L,
    dshpc.cpu_slots = 1L,
    dshpc.gpu_count = 0L
  ))

  trusted_hpc_call(register_dshpc_runner, list(
    name = "persist_seed",
    command = "/bin/sh",
    args_template = c("-c",
      "printf '%s\\n' durable-seed > {output_dir}/value.txt"),
    allowed_params = character(0),
    resources = list(memory_mb = 32L, cpu_slots = 1L)))
  trusted_hpc_call(register_dshpc_runner, list(
    name = "persist_consume",
    command = "/bin/sh",
    args_template = c("-c", paste(
      "test -f {input_dir}/value.txt &&",
      "tr '[:lower:]' '[:upper:]' < {input_dir}/value.txt >",
      "{output_dir}/copied.txt")),
    allowed_params = character(0),
    resources = list(memory_mb = 32L, cpu_slots = 1L)))

  spec <- dsHPC:::.validate_job_spec(list(
    label = "persistence-test",
    visibility = "private",
    dag = list(nodes = list(
      seed = list(type = "run_artifact", plane = "artifact",
        runner = "persist_seed"),
      consume = list(type = "run_artifact", plane = "artifact",
        runner = "persist_consume", inputs = "seed")
    ))
  ))
  capability <- dsHPC:::.generate_job_capability()
  handle <- list(job_id = "job_persistent_dag",
    .dshpc_capability = capability)
  bearer <- dsHPC:::.encode_job_bearer(handle$job_id, capability)

  db <- dsHPC:::.db_connect()
  dsHPC:::.store_create_job(db, handle$job_id, "owner", spec, 2L,
    access_token_hash = dsHPC:::.hash_job_capability(capability))
  dsHPC:::.db_close(db)

  # A newly opened worker connection discovers the persisted pending job.
  db <- dsHPC:::.db_connect()
  dsHPC:::.worker_dispatch(db)
  wait_for_durable_exit(home, handle$job_id, 1L)
  dsHPC:::.db_close(db)
  forget_in_memory_runner_state()

  # Reconcile step 1 from its durable exit marker. Advancing launches step 2,
  # whose explicit DAG input resolves to step 1's persisted output directory.
  db <- dsHPC:::.db_connect()
  dsHPC:::.worker_reap(db)
  wait_for_durable_exit(home, handle$job_id, 2L)
  dsHPC:::.db_close(db)
  forget_in_memory_runner_state()

  # A second restart finalizes the DAG and builds the durable result record.
  db <- dsHPC:::.db_connect()
  dsHPC:::.worker_reap(db)
  dsHPC:::.db_close(db)

  db <- dsHPC:::.db_connect()
  job <- dsHPC:::.store_get_job(db, handle$job_id)
  steps <- DBI::dbGetQuery(db,
    "SELECT step_index, state, output_ref, cache_hit, step_hash
     FROM steps WHERE job_id = ? ORDER BY step_index",
    params = list(handle$job_id))
  outputs <- DBI::dbGetQuery(db,
    "SELECT step_index, name, path_or_ref, safe_for_client
     FROM outputs WHERE job_id = ? ORDER BY step_index, id",
    params = list(handle$job_id))
  stored <- dsHPC:::.store_get_spec(db, handle$job_id)
  dsHPC:::.db_close(db)

  expect_equal(job$state, "FINISHED")
  expect_equal(as.integer(job$step_index), 2L)
  expect_equal(steps$state, c("done", "done"))
  expect_true(all(is.na(steps$step_hash)))
  expect_equal(as.integer(steps$cache_hit), c(0L, 0L))
  expect_equal(vapply(stored$steps, `[[`, character(1), "node_id"),
    c("seed", "consume"))
  expect_equal(stored$steps[[2]]$inputs[[1]]$step, 1L)
  expect_true(all(!as.logical(outputs$safe_for_client)))

  copied <- file.path(home, "artifacts", handle$job_id, "step_002",
    "output", "copied.txt")
  expect_equal(readLines(copied, warn = FALSE), "DURABLE-SEED")
  expect_true(file.exists(file.path(home, "artifacts", handle$job_id,
    "result", "result.rds")))
  expect_equal(hpcStatusDS(bearer)$state, "FINISHED")
  raw_job_id <- handle$job_id
  expect_error(hpcStatusDS(raw_job_id),
    "Job not found or access denied", fixed = TRUE)
})

test_that("a CLONING job is recovered from confined outputs after restart", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home))

  spec <- list(.owner = "source-owner", label = "recovery-test",
    visibility = "global", reuse_fingerprint = strrep("a", 64L),
    steps = list(list(
      type = "emit", plane = "session", output_name = "values",
      value = 1:5)))
  source <- trusted_hpc_call(hpcSubmitInternal, spec)

  target_id <- "job_recover_clone"
  capability <- dsHPC:::.generate_job_capability()
  target <- list(job_id = target_id, .dshpc_capability = capability)
  db <- dsHPC:::.db_connect()
  source_job <- dsHPC:::.store_get_job(db, source$job_id)
  stored_spec <- dsHPC:::.store_get_spec(db, source$job_id)
  dsHPC:::.store_create_job(db, target_id, "target-owner", stored_spec, 1L,
    spec_hash = source_job$spec_hash,
    access_token_hash = dsHPC:::.hash_job_capability(capability),
    initial_state = "CLONING",
    clone_owner = list(node = dsHPC:::.scheduler_node_id(),
      pid = .Machine$integer.max))

  # Simulate a process death after only part of the target tree and registry
  # were written. Recovery must discard these rows/files before cloning again.
  partial_dir <- file.path(dsHPC:::.ensure_step_dir(target_id, 1L), "output")
  partial_path <- file.path(partial_dir, "partial.rds")
  saveRDS("partial", partial_path)
  dsHPC:::.db_register_output(db, target_id, 1L, "partial", "summary",
    partial_path, safe_for_client = TRUE)
  dsHPC:::.store_update_step(db, target_id, 1L, state = "done",
    output_ref = file.path("artifacts", target_id, "step_001", "output"))
  dsHPC:::.db_close(db)

  # A fresh worker/database connection is the restart boundary.
  db <- dsHPC:::.db_connect()
  dsHPC:::.worker_reap(db)
  recovered <- dsHPC:::.store_get_job(db, target_id)
  outputs <- DBI::dbGetQuery(db,
    "SELECT name, path_or_ref FROM outputs WHERE job_id = ? ORDER BY id",
    params = list(target_id))
  events <- DBI::dbGetQuery(db,
    "SELECT event FROM events WHERE job_id = ? ORDER BY id",
    params = list(target_id))
  dsHPC:::.db_close(db)

  expect_equal(recovered$state, "FINISHED")
  expect_equal(outputs$name, "values")
  expect_false(file.exists(partial_path))
  target_root <- normalizePath(file.path(home, "artifacts", target_id),
    winslash = "/", mustWork = TRUE)
  output_path <- normalizePath(outputs$path_or_ref[1], winslash = "/",
    mustWork = TRUE)
  expect_true(startsWith(output_path, paste0(target_root, "/")))
  expect_false(grepl(source$job_id, output_path, fixed = TRUE))
  expect_equal(readRDS(output_path), 1:5)
  expect_true("deduplicated" %in% events$event)

  unlink(file.path(home, "artifacts", source$job_id), recursive = TRUE,
    force = TRUE)
  expect_equal(hpcStatusDS(target)$state, "FINISHED")
  expect_equal(trusted_hpc_call(hpcLoadOutputInternal, target, "values",
    required_label = "recovery-test"), 1:5)
})

test_that("clone recovery requires the current conservative reuse contract", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home))

  sealed_spec <- function(value, fingerprint, step = NULL) {
    if (is.null(step)) {
      step <- list(type = "emit", plane = "session", output_name = "values",
        value = value)
    }
    spec <- dsHPC:::.validate_job_spec(list(label = "recovery-test",
      visibility = "global", reuse_fingerprint = fingerprint,
      steps = list(step)))
    spec$.dshpc_provider <- "dsHPC"
    spec$.dshpc_runtime_revision <- dsHPC:::.dshpc_runtime_revision(spec)
    spec$.dshpc_runtime_identity <- dsHPC:::.dshpc_runtime_identity(
      spec, "dsHPC")
    spec
  }

  missing_fingerprint <- sealed_spec(1L, strrep("a", 64L))
  missing_fingerprint$reuse_fingerprint <- NULL
  malformed_fingerprint <- sealed_spec(2L, strrep("b", 64L))
  malformed_fingerprint$reuse_fingerprint <- "not-a-sha256"
  legacy_runtime <- sealed_spec(3L, strrep("c", 64L))
  legacy_runtime$.dshpc_provider <- NULL
  legacy_runtime$.dshpc_runtime_identity <- NULL
  changed_runtime <- sealed_spec(4L, strrep("d", 64L))
  changed_runtime$.dshpc_runtime_identity <- strrep("0", 64L)
  missing_revision <- sealed_spec(5L, strrep("e", 64L))
  missing_revision$.dshpc_runtime_revision <- NULL
  invalid_provider <- sealed_spec(6L, strrep("e", 64L))
  invalid_provider$.dshpc_provider <- "invalid-provider"
  invalid_provider$.dshpc_runtime_identity <- dsHPC:::.dshpc_runtime_identity(
    invalid_provider, "invalid-provider")
  effectful <- sealed_spec(7L, strrep("f", 64L),
    step = list(type = "assign_expr", plane = "session",
      expr = "1 + 1", symbol = "assigned_value"))
  valid_target <- sealed_spec(8L, paste0(strrep("a", 63L), "b"))
  invalid_source <- valid_target
  invalid_source$.dshpc_runtime_identity <- strrep("0", 64L)

  cases <- list(
    missing_fingerprint = list(source = missing_fingerprint,
      target = missing_fingerprint, provider = "dsHPC"),
    malformed_fingerprint = list(source = malformed_fingerprint,
      target = malformed_fingerprint, provider = "dsHPC"),
    legacy_runtime = list(source = legacy_runtime, target = legacy_runtime,
      provider = "dsHPC"),
    changed_runtime = list(source = changed_runtime, target = changed_runtime,
      provider = "dsHPC"),
    missing_revision = list(source = missing_revision,
      target = missing_revision, provider = "dsHPC"),
    invalid_provider = list(source = invalid_provider,
      target = invalid_provider, provider = "invalid-provider"),
    effectful = list(source = effectful, target = effectful,
      provider = "dsHPC"),
    invalid_source = list(source = invalid_source, target = valid_target,
      provider = "dsHPC")
  )

  db <- dsHPC:::.db_connect()
  for (case_name in names(cases)) {
    case <- cases[[case_name]]
    source_id <- paste0("job_source_", case_name)
    target_id <- paste0("job_target_", case_name)
    spec_hash <- dsHPC:::.dshpc_whole_job_hash(case$target, case$provider)

    dsHPC:::.store_create_job(db, source_id, "source-owner", case$source, 1L,
      spec_hash = spec_hash)
    source_dir <- file.path(dsHPC:::.ensure_step_dir(source_id, 1L), "output")
    source_path <- file.path(source_dir, "value.rds")
    saveRDS(case_name, source_path)
    dsHPC:::.db_register_output(db, source_id, 1L, "values", "server_object",
      source_path, safe_for_client = FALSE, reuse_class = "server_reusable")
    dsHPC:::.store_update_step(db, source_id, 1L, state = "done",
      output_ref = source_dir)
    dsHPC:::.store_update_job(db, source_id, state = "FINISHED",
      step_index = 1L, finished_at = "2026-01-01T00:00:00.000Z")
    dsHPC:::.store_create_job(db, target_id, "target-owner", case$target, 1L,
      spec_hash = spec_hash, initial_state = "CLONING")
  }

  dsHPC:::.recover_deduplicated_job_clones(db)
  targets <- DBI::dbGetQuery(db,
    "SELECT job_id, state FROM jobs WHERE job_id LIKE 'job_target_%'")
  failed_events <- DBI::dbGetQuery(db,
    "SELECT job_id FROM events WHERE event = 'clone_recovery_failed'")
  target_outputs <- DBI::dbGetQuery(db,
    "SELECT job_id FROM outputs WHERE job_id LIKE 'job_target_%'")
  dsHPC:::.db_close(db)

  expect_equal(nrow(targets), length(cases))
  expect_true(all(targets$state == "FAILED"))
  expect_setequal(failed_events$job_id,
    paste0("job_target_", names(cases)))
  expect_equal(nrow(target_outputs), 0L)
})

test_that("clone recovery skips live owners and fails closed without a source", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home))
  spec <- list(label = "recovery-test", visibility = "global",
    resource_class = "default", steps = list(list(
      type = "emit", plane = "session", output_name = "values",
      value = 1:5)))

  db <- dsHPC:::.db_connect()
  dsHPC:::.store_create_job(db, "job_live_clone", "owner", spec, 1L,
    spec_hash = "live-hash", initial_state = "CLONING",
    clone_owner = list(node = dsHPC:::.scheduler_node_id(),
      pid = Sys.getpid()))
  dsHPC:::.worker_reap(db)
  expect_equal(dsHPC:::.store_get_job(db, "job_live_clone")$state,
    "CLONING")

  dsHPC:::.store_create_job(db, "job_cancelled_clone", "owner", spec, 1L,
    spec_hash = "cancelled-hash", initial_state = "CLONING")
  dsHPC:::.store_update_job(db, "job_cancelled_clone", state = "CANCELLED")
  expect_error(dsHPC:::.complete_deduplicated_job_clone(
    db, "job_cancelled_clone", list(job_id = "job_source",
      state = "FINISHED", step_index = 1L, started_at = NA_character_,
      finished_at = NA_character_)), "state changed")
  expect_equal(dsHPC:::.store_get_job(db, "job_cancelled_clone")$state,
    "CANCELLED")

  missing_capability <- dsHPC:::.generate_job_capability()
  dsHPC:::.store_create_job(db, "job_missing_clone", "owner", spec, 1L,
    spec_hash = "missing-hash",
    access_token_hash = dsHPC:::.hash_job_capability(missing_capability),
    initial_state = "CLONING")
  dsHPC:::.worker_reap(db)
  failed <- dsHPC:::.store_get_job(db, "job_missing_clone")
  expect_equal(failed$state, "FAILED")
  missing_handle <- list(job_id = "job_missing_clone",
    .dshpc_capability = missing_capability)
  expect_identical(hpcStatusDS(missing_handle)$error, "Job execution failed.")
  dsHPC:::.db_close(db)
})
