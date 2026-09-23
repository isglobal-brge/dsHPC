.dag_fencing_fixture <- function(session = FALSE, two_nodes = FALSE,
                                 .local_envir = parent.frame()) {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.worker_autostart = FALSE,
    dshpc.executor_backend = "embedded", dshpc.step_cache = FALSE,
    dshpc.max_parallel_steps = 2L, dshpc.max_retries = 0L,
    dshpc.cpu_slots = 4L, dshpc.node_memory_mb = 1024L,
    dshpc.memory_reserve_mb = 0L, dshpc.gpu_count = 0L),
    .local_envir = .local_envir)
  yaml::write_yaml(list(name = "fenced_runner", command = "/bin/true",
    args_template = character(0), allowed_params = character(0),
    resources = list(memory_mb = 32L, cpu_slots = 1L)),
    file.path(home, "runners", "fenced_runner.yml"))
  node <- if (session) list(type = "emit", output_name = "value", value = 1L)
    else list(type = "run_artifact", runner = "fenced_runner")
  nodes <- if (two_nodes) list(a = node, b = node) else list(a = node)
  spec <- dsHPC:::.validate_job_spec(list(label = "dsHPC_test",
    visibility = "private", dag = list(nodes = nodes)))
  db <- dsHPC:::.db_connect()
  withr::defer({
    dsHPC:::.db_close(db)
    cleanup_test_home(home)
  }, envir = .local_envir)
  dsHPC:::.store_create_job(db, "job_fenced_dag", "owner", spec, length(nodes))
  dsHPC:::.store_update_job(db, "job_fenced_dag", state = "RUNNING")
  list(db = db, home = home, spec = spec, job_id = "job_fenced_dag")
}

test_that("a stale dispatch error cannot fail a replacement DAG attempt", {
  fixture <- .dag_fencing_fixture()
  previous_attempt <- NULL
  testthat::local_mocked_bindings(.executor_run_step = function(
    db, job_id, step_index, spec, attempt_id = NULL) {
      previous_attempt <<- attempt_id
      dsHPC:::.store_update_step(db, job_id, step_index,
        attempt_id = "replacement-attempt")
      stop("Late error from retired dispatcher", call. = FALSE)
    }, .package = "dsHPC")
  dsHPC:::.executor_run_dag(fixture$db, fixture$job_id, fixture$spec)
  step <- dsHPC:::.dag_steps(fixture$db, fixture$job_id)
  job <- dsHPC:::.store_get_job(fixture$db, fixture$job_id)
  expect_true(is.character(previous_attempt) && nzchar(previous_attempt))
  expect_identical(step$attempt_id, "replacement-attempt")
  expect_identical(step$state, "running")
  expect_identical(step$retry_count, 0L)
  expect_identical(job$state, "RUNNING")
  expect_true(is.na(job$error_message))
  expect_false(is.na(step$resource_json))
})

test_that("external cancellation reconciliation rejects a stale DAG attempt", {
  fixture <- .dag_fencing_fixture()
  testthat::local_mocked_bindings(.run_artifact_step = function(...) TRUE,
    .package = "dsHPC")
  dsHPC:::.executor_run_dag(fixture$db, fixture$job_id, fixture$spec)
  before <- dsHPC:::.dag_steps(fixture$db, fixture$job_id)
  dsHPC:::.worker_finalize_external_cancellation(fixture$db, fixture$job_id,
    1L, "CANCELLED", attempt_id = "retired-attempt")
  expect_identical(dsHPC:::.dag_steps(fixture$db, fixture$job_id), before)
  expect_true(is.na(dsHPC:::.store_get_job(fixture$db,
    fixture$job_id)$error_message))
  dsHPC:::.store_update_job(fixture$db, fixture$job_id, cancel_requested = 1L)
  dsHPC:::.worker_finalize_external_cancellation(fixture$db, fixture$job_id,
    1L, "CANCELLED", attempt_id = before$attempt_id)
  dsHPC:::.dag_finish_job(fixture$db, fixture$job_id)
  expect_identical(dsHPC:::.dag_steps(fixture$db, fixture$job_id)$state, "failed")
  expect_identical(dsHPC:::.store_get_job(fixture$db, fixture$job_id)$state,
    "CANCELLED")
})

test_that("DAG session cancellation waits for the executing operation to settle", {
  fixture <- .dag_fencing_fixture(session = TRUE, two_nodes = TRUE)
  during <- NULL
  executions <- 0L
  testthat::local_mocked_bindings(.session_emit = function(
    step, step_dir, db, job_id, step_index) {
      executions <<- executions + 1L
      outcome <- dsHPC:::.executor_kill(db, job_id)
      during <<- list(outcome = outcome,
        job = dsHPC:::.store_get_job(db, job_id)$state,
        steps = dsHPC:::.dag_steps(db, job_id)$state)
      list(type = "emit", name = "value")
    }, .package = "dsHPC")
  dsHPC:::.executor_run_dag(fixture$db, fixture$job_id, fixture$spec)
  expect_identical(during$outcome, "requested")
  expect_identical(during$job, "RUNNING")
  expect_identical(during$steps, c("running", "pending"))
  expect_equal(executions, 1L)
  expect_identical(dsHPC:::.dag_steps(fixture$db, fixture$job_id)$state,
    c("failed", "skipped"))
  expect_identical(dsHPC:::.store_get_job(fixture$db, fixture$job_id)$state,
    "CANCELLED")
  expect_equal(DBI::dbGetQuery(fixture$db,
    "SELECT COUNT(*) AS n FROM resource_leases WHERE job_id = ?",
    params = list(fixture$job_id))$n, 0L)
})

test_that("a stale session completion cannot finish a replacement DAG attempt", {
  fixture <- .dag_fencing_fixture(session = TRUE)
  testthat::local_mocked_bindings(.session_emit = function(
    step, step_dir, db, job_id, step_index) {
      dsHPC:::.store_update_step(db, job_id, step_index,
        attempt_id = "replacement-session-attempt")
      list(type = "emit", name = "value")
    }, .package = "dsHPC")
  dsHPC:::.executor_run_dag(fixture$db, fixture$job_id, fixture$spec)
  step <- dsHPC:::.dag_steps(fixture$db, fixture$job_id)
  expect_identical(step$attempt_id, "replacement-session-attempt")
  expect_identical(step$state, "running")
  expect_identical(dsHPC:::.store_get_job(fixture$db, fixture$job_id)$state,
    "RUNNING")
  expect_equal(DBI::dbGetQuery(fixture$db,
    "SELECT COUNT(*) AS n FROM events WHERE job_id = ? AND event = 'step_done'",
    params = list(fixture$job_id))$n, 0L)
})

test_that("DAG cancellation reconciles a remote id acknowledged after the request", {
  fixture <- .dag_fencing_fixture()
  during <- NULL
  cancel_calls <- 0L
  testthat::local_mocked_bindings(
    .run_artifact_step = function(db, job_id, step_index, ...) {
      outcome <- dsHPC:::.executor_kill(db, job_id)
      during <<- list(outcome = outcome,
        state = dsHPC:::.store_get_job(db, job_id)$state,
        step = dsHPC:::.dag_steps(db, job_id)$state)
      dsHPC:::.store_update_step(db, job_id, step_index,
        external_backend = "external", external_id = "remote-acknowledged",
        external_status = "submitted")
      invisible(TRUE)
    },
    .backend_step_status = function(...) {
      if (cancel_calls == 0L) list(state = "running", external_state = "RUNNING")
      else list(state = "cancelled", external_state = "CANCELLED", exit_code = 143L)
    },
    .backend_cancel_step = function(backend, external_id, ...) {
      expect_identical(backend, "external")
      expect_identical(external_id, "remote-acknowledged")
      cancel_calls <<- cancel_calls + 1L
      TRUE
    }, .package = "dsHPC")
  dsHPC:::.executor_run_dag(fixture$db, fixture$job_id, fixture$spec)
  expect_identical(during$outcome, "requested")
  expect_identical(during$state, "RUNNING")
  expect_identical(during$step, "running")
  dsHPC:::.worker_reap(fixture$db)
  expect_equal(cancel_calls, 1L)
  expect_identical(dsHPC:::.store_get_job(fixture$db, fixture$job_id)$state,
    "RUNNING")
  expect_identical(dsHPC:::.dag_steps(fixture$db, fixture$job_id)$external_status,
    "CANCEL_REQUESTED")
  dsHPC:::.worker_reap(fixture$db)
  expect_equal(cancel_calls, 1L)
  expect_identical(dsHPC:::.store_get_job(fixture$db, fixture$job_id)$state,
    "CANCELLED")
  expect_identical(dsHPC:::.dag_steps(fixture$db, fixture$job_id)$state, "failed")
})
