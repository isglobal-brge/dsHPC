local_concurrent_dag <- function(.local_envir = parent.frame()) {
  skip_on_os("windows")
  skip_if_not_installed("ps")
  fixture <- new.env(parent = emptyenv())
  fixture$home <- setup_test_home()
  withr::local_options(list(
    dshpc.home = fixture$home,
    dshpc.worker_autostart = FALSE,
    dshpc.executor_backend = "embedded",
    dshpc.step_cache = FALSE,
    dshpc.max_parallel_steps = 2L,
    dshpc.max_retries = 0L,
    dshpc.cpu_slots = 4L,
    dshpc.node_memory_mb = 2048L,
    dshpc.memory_reserve_mb = 0L,
    dshpc.gpu_count = 0L,
    dshpc.queue_visibility = "shared"
  ), .local_envir = .local_envir)
  fixture$db <- dsHPC:::.db_connect()
  withr::defer({
    if (DBI::dbIsValid(fixture$db)) {
      rows <- DBI::dbGetQuery(fixture$db,
        "SELECT job_id, step_index FROM steps")
      for (i in seq_len(nrow(rows))) {
        key <- paste0(rows$job_id[i], "_", rows$step_index[i])
        proc <- dsHPC:::.proc_registry[[key]]
        if (!is.null(proc)) {
          if (proc$is_alive()) proc$kill_tree()
          proc$wait(5000)
          rm(list = key, envir = dsHPC:::.proc_registry)
        }
      }
      dsHPC:::.db_close(fixture$db)
    }
    cleanup_test_home(fixture$home)
  }, envir = .local_envir)
  fixture
}

concurrent_dag_runner <- function(fixture, name, script,
                                allowed_params = character(0)) {
  yaml::write_yaml(list(name = name, command = "/bin/sh",
    args_template = c("-c", script), allowed_params = allowed_params,
    resources = list(memory_mb = 32L, cpu_slots = 1L)),
    file.path(fixture$home, "runners", paste0(name, ".yml")))
  name
}

concurrent_dag_node <- function(runner, config = list(), inputs = NULL) {
  node <- list(type = "run_artifact", plane = "artifact",
    runner = runner, config = config)
  if (!is.null(inputs)) node$inputs <- inputs
  node
}

concurrent_dag_create <- function(fixture, job_id, nodes) {
  spec <- dsHPC:::.validate_job_spec(list(label = "concurrent-dag-test",
    visibility = "private", dag = list(nodes = nodes)))
  dsHPC:::.store_create_job(fixture$db, job_id, "owner", spec,
    length(spec$steps))
  spec
}

concurrent_dag_steps <- function(fixture, job_id) {
  DBI::dbGetQuery(fixture$db,
    "SELECT * FROM steps WHERE job_id = ? ORDER BY step_index",
    params = list(job_id))
}

concurrent_dag_wait <- function(job_id, indexes, timeout = 10000) {
  for (index in indexes) {
    proc <- dsHPC:::.proc_registry[[paste0(job_id, "_", index)]]
    expect_false(is.null(proc), info = paste("Missing process", job_id, index))
    if (is.null(proc)) stop("Runner was not dispatched.", call. = FALSE)
    proc$wait(timeout)
    expect_false(proc$is_alive(), info = paste("Runner did not exit", index))
    if (proc$is_alive()) stop("Runner did not exit.", call. = FALSE)
  }
}

# FIFOs make assertions about in-flight steps independent of machine speed.
# processx imposes a deadline on every blocking reader and writer.
concurrent_dag_gate <- function(fixture, name, exit_code = 0L) {
  started <- file.path(fixture$home, paste0(name, "-started"))
  release <- file.path(fixture$home, paste0(name, "-release"))
  processx::run(Sys.which("mkfifo"), c(started, release), timeout = 5)
  runner <- concurrent_dag_runner(fixture, name, paste(
    "printf '%s\\n' \"$$\" >", shQuote(started), ";",
    "read release <", shQuote(release), ";",
    "printf '%s\\n' released > \"$DSHPC_OUTPUT_DIR/value.txt\";",
    "exit", as.integer(exit_code)))
  list(runner = runner, started = started, release = release)
}

concurrent_dag_started <- function(gate) {
  result <- processx::run("/bin/cat", gate$started, timeout = 10)
  as.integer(trimws(result$stdout))
}

concurrent_dag_release <- function(gate) {
  processx::run("/bin/sh", c("-c",
    paste("printf '%s\\n' release >", shQuote(gate$release))), timeout = 10)
  invisible(TRUE)
}

concurrent_dag_worker <- function(fixture, pause_launch = NULL,
                                  pause_prepare = NULL) {
  config_path <- tempfile("worker-config-", tmpdir = fixture$home,
    fileext = ".rds")
  script <- tempfile("worker-", tmpdir = fixture$home, fileext = ".R")
  saveRDS(list(package = getNamespaceInfo(asNamespace("dsHPC"), "path"),
    libraries = .libPaths(), pause_launch = pause_launch,
    pause_prepare = pause_prepare,
    options = options()[startsWith(names(options()), "dshpc.")]), config_path)
  writeLines(c(
    "config <- readRDS(commandArgs(TRUE)[1L])",
    ".libPaths(config$libraries)",
    "options(config$options)",
    "if (file.exists(file.path(config$package, 'R', 'executor_dag.R'))) {",
    "  pkgload::load_all(config$package, quiet = TRUE)",
    "} else library(dsHPC, lib.loc = dirname(config$package))",
    "if (!is.null(config$pause_launch)) {",
    "  ns <- asNamespace('dsHPC')",
    "  original <- get('.store_update_step', ns)",
    "  paused <- function(db, job_id, step_index, ...) {",
    "    updates <- list(...)",
    "    if (!is.null(updates$worker_pid) && !is.na(updates$worker_pid)) {",
    "      writeLines(as.character(updates$worker_pid), config$pause_launch$started)",
    "      readLines(config$pause_launch$release, n = 1L)",
    "    }",
    "    original(db, job_id, step_index, ...)",
    "  }",
    "  unlockBinding('.store_update_step', ns)",
    "  assign('.store_update_step', paused, ns)",
    "  lockBinding('.store_update_step', ns)",
    "}",
    "if (!is.null(config$pause_prepare)) {",
    "  ns <- asNamespace('dsHPC')",
    "  original <- get('.prepare_artifact_command', ns)",
    "  paused <- function(...) {",
    "    writeLines(as.character(Sys.getpid()), config$pause_prepare$started)",
    "    readLines(config$pause_prepare$release, n = 1L)",
    "    original(...)",
    "  }",
    "  unlockBinding('.prepare_artifact_command', ns)",
    "  assign('.prepare_artifact_command', paused, ns)",
    "  lockBinding('.prepare_artifact_command', ns)",
    "}",
    "dsHPC:::.worker_main()"
  ), script)
  processx::process$new(file.path(R.home("bin"), "Rscript"),
    c("--vanilla", script, config_path),
    stdout = paste0(script, ".log"), stderr = paste0(script, ".log"),
    env = c("current", R_LIBS = paste(.libPaths(),
      collapse = .Platform$path.sep)), cleanup = FALSE, cleanup_tree = FALSE)
}

concurrent_dag_stop_worker <- function(proc) {
  if (proc$is_alive()) dsHPC:::.signal_pid(proc$get_pid(), 9L)
  proc$wait(5000)
  invisible(TRUE)
}

concurrent_dag_stop_orphans <- function(fixture, job_id) {
  rows <- concurrent_dag_steps(fixture, job_id)
  for (i in which(rows$state == "running")) {
    step_dir <- file.path(fixture$home, "artifacts", job_id,
      sprintf("step_%03d", rows$step_index[i]))
    pids <- c(rows$worker_pid[i],
      dsHPC:::.dag_read_pid(file.path(step_dir, "runner.pid")),
      dsHPC:::.dag_read_pid(file.path(step_dir, "child.pid")))
    handles <- lapply(unique(stats::na.omit(pids)), function(pid) {
      tryCatch(ps::ps_handle(pid), error = function(e) NULL)
    })
    handles <- Filter(Negate(is.null), handles)
    for (handle in handles) try(ps::ps_kill(handle), silent = TRUE)
    if (length(handles)) ps::ps_wait(handles, timeout = 5000)
  }
  invisible(TRUE)
}

test_that("independent DAG runners overlap and parallelism one is sequential", {
  fixture <- local_concurrent_dag()
  runner <- concurrent_dag_runner(fixture, "parallel_sleep", paste(
    "test -z \"${DSHPC_INPUT_DIR:-}\" || exit 41;",
    "sleep \"$DSHPC_CFG_SECONDS\";",
    "printf '%s\\n' \"$DSHPC_CFG_VALUE\" > \"$DSHPC_OUTPUT_DIR/value.txt\""
  ), c("seconds", "value"))
  nodes <- list(
    a = concurrent_dag_node(runner, list(seconds = 2, value = "a")),
    b = concurrent_dag_node(runner, list(seconds = 2, value = "b")))
  concurrent_dag_create(fixture, "job_parallel_sleep", nodes)

  started <- proc.time()[["elapsed"]]
  dsHPC:::.worker_dispatch(fixture$db)
  expect_equal(concurrent_dag_steps(fixture, "job_parallel_sleep")$state,
    c("running", "running"))
  concurrent_dag_wait("job_parallel_sleep", 1:2)
  dsHPC:::.worker_reap(fixture$db)
  parallel <- proc.time()[["elapsed"]] - started
  expect_identical(dsHPC:::.store_get_job(fixture$db,
    "job_parallel_sleep")$state, "FINISHED")
  expect_lt(parallel, 3.5,
    label = sprintf("Two 2-second runners took %.3f seconds", parallel))

  withr::local_options(list(dshpc.max_parallel_steps = 1L))
  concurrent_dag_create(fixture, "job_sequential_sleep", nodes)
  started <- proc.time()[["elapsed"]]
  dsHPC:::.worker_dispatch(fixture$db)
  expect_equal(concurrent_dag_steps(fixture, "job_sequential_sleep")$state,
    c("running", "pending"))
  concurrent_dag_wait("job_sequential_sleep", 1L)
  dsHPC:::.worker_reap(fixture$db)
  expect_equal(concurrent_dag_steps(fixture, "job_sequential_sleep")$state,
    c("done", "running"))
  concurrent_dag_wait("job_sequential_sleep", 2L)
  dsHPC:::.worker_reap(fixture$db)
  sequential <- proc.time()[["elapsed"]] - started
  expect_identical(dsHPC:::.store_get_job(fixture$db,
    "job_sequential_sleep")$state, "FINISHED")
  expect_gte(sequential, 4)
  expect_lt(parallel, sequential * 0.8)
  message(sprintf("DAG timing: parallel %.3fs; sequential %.3fs; runners 2s each",
    parallel, sequential))
})

test_that("a diamond DAG stages both successful upstream outputs", {
  fixture <- local_concurrent_dag()
  a <- concurrent_dag_gate(fixture, "diamond_a")
  b <- concurrent_dag_gate(fixture, "diamond_b")
  join <- concurrent_dag_runner(fixture, "diamond_join", paste(
    "test -f \"$DSHPC_INPUT_DIR/left/value.txt\" &&",
    "test -f \"$DSHPC_INPUT_DIR/right/value.txt\" &&",
    "cat \"$DSHPC_INPUT_DIR/left/value.txt\"",
    "\"$DSHPC_INPUT_DIR/right/value.txt\" > \"$DSHPC_OUTPUT_DIR/joined.txt\""))
  concurrent_dag_create(fixture, "job_diamond", list(
    a = concurrent_dag_node(a$runner),
    b = concurrent_dag_node(b$runner),
    c = concurrent_dag_node(join, inputs = list(left = "a", right = "b"))))
  dsHPC:::.worker_dispatch(fixture$db)
  concurrent_dag_started(a)
  concurrent_dag_started(b)
  expect_equal(concurrent_dag_steps(fixture, "job_diamond")$state,
    c("running", "running", "pending"))

  concurrent_dag_release(a)
  concurrent_dag_wait("job_diamond", 1L)
  dsHPC:::.worker_reap(fixture$db)
  expect_equal(concurrent_dag_steps(fixture, "job_diamond")$state,
    c("done", "running", "pending"))
  expect_equal(dsHPC:::.store_get_job(fixture$db, "job_diamond")$step_index, 1L)
  concurrent_dag_release(b)
  concurrent_dag_wait("job_diamond", 2L)
  dsHPC:::.worker_reap(fixture$db)
  concurrent_dag_wait("job_diamond", 3L)
  dsHPC:::.worker_reap(fixture$db)

  expect_identical(dsHPC:::.store_get_job(fixture$db, "job_diamond")$state,
    "FINISHED")
  expect_equal(concurrent_dag_steps(fixture, "job_diamond")$state,
    rep("done", 3L))
  expect_equal(readLines(file.path(fixture$home, "artifacts", "job_diamond",
    "step_003", "output", "joined.txt")), c("released", "released"))
  inputs <- jsonlite::read_json(file.path(fixture$home, "artifacts",
    "job_diamond", "step_003", "input", "inputs.json"))
  expect_named(inputs, c("left", "right"))
  expect_equal(c(inputs$left$step, inputs$right$step), 1:2)
})

test_that("DAG failures drain in-flight branches and retain the first error", {
  fixture <- local_concurrent_dag()
  failure <- concurrent_dag_runner(fixture, "branch_failure", "exit 17")
  for (other_exit in c(0L, 23L)) {
    other <- concurrent_dag_gate(fixture, paste0("drain_", other_exit),
      exit_code = other_exit)
    job_id <- paste0("job_drain_", other_exit)
    concurrent_dag_create(fixture, job_id, list(
      a = concurrent_dag_node(failure),
      b = concurrent_dag_node(other$runner),
      c = concurrent_dag_node(failure, inputs = c("a", "b"))))
    dsHPC:::.worker_dispatch(fixture$db)
    concurrent_dag_started(other)
    concurrent_dag_wait(job_id, 1L)
    dsHPC:::.worker_reap(fixture$db)
    draining <- dsHPC:::.store_get_job(fixture$db, job_id)
    expect_identical(draining$state, "RUNNING")
    expect_true(nzchar(draining$error_message))
    expect_equal(concurrent_dag_steps(fixture, job_id)$state,
      c("failed", "running", "pending"))

    concurrent_dag_release(other)
    concurrent_dag_wait(job_id, 2L)
    dsHPC:::.worker_reap(fixture$db)
    failed <- dsHPC:::.store_get_job(fixture$db, job_id)
    expect_identical(failed$state, "FAILED")
    expect_identical(failed$error_message, draining$error_message)
    expect_false(any(concurrent_dag_steps(fixture, job_id)$state == "running"))
    expect_false(dir.exists(file.path(fixture$home, "artifacts", job_id,
      "step_003")))
  }
})

test_that("a ready descendant starts while an unrelated branch is running", {
  fixture <- local_concurrent_dag()
  a <- concurrent_dag_gate(fixture, "unlock_a")
  b <- concurrent_dag_gate(fixture, "unlock_b")
  c <- concurrent_dag_gate(fixture, "unlock_c")
  job_id <- "job_unlock_descendant"
  concurrent_dag_create(fixture, job_id, list(
    a = concurrent_dag_node(a$runner),
    b = concurrent_dag_node(b$runner),
    c = concurrent_dag_node(c$runner, inputs = "a")))
  dsHPC:::.worker_dispatch(fixture$db)
  concurrent_dag_started(a)
  concurrent_dag_started(b)
  concurrent_dag_release(a)
  concurrent_dag_wait(job_id, 1L)
  dsHPC:::.worker_reap(fixture$db)
  concurrent_dag_started(c)
  expect_equal(concurrent_dag_steps(fixture, job_id)$state,
    c("done", "running", "running"))
  concurrent_dag_release(c)
  concurrent_dag_wait(job_id, 3L)
  dsHPC:::.worker_reap(fixture$db)
  expect_identical(dsHPC:::.store_get_job(fixture$db, job_id)$state, "RUNNING")
  concurrent_dag_release(b)
  concurrent_dag_wait(job_id, 2L)
  dsHPC:::.worker_reap(fixture$db)
  expect_identical(dsHPC:::.store_get_job(fixture$db, job_id)$state, "FINISHED")
})

test_that("SIGKILL recovery retries two lost attempts without rerunning a sibling", {
  fixture <- local_concurrent_dag()
  withr::local_options(list(dshpc.max_retries = 1L))
  counter <- file.path(fixture$home, "completed-launches")
  completed <- concurrent_dag_runner(fixture, "completed_sibling", paste(
    "printf '%s\\n' launched >>", shQuote(counter), ";",
    "printf '%s\\n' stable > \"$DSHPC_OUTPUT_DIR/value.txt\""))
  b <- concurrent_dag_gate(fixture, "recover_b")
  c <- concurrent_dag_gate(fixture, "recover_c")
  job_id <- "job_two_lost_attempts"
  concurrent_dag_create(fixture, job_id, list(
    a = concurrent_dag_node(completed),
    b = concurrent_dag_node(b$runner),
    c = concurrent_dag_node(c$runner)))
  dsHPC:::.worker_dispatch(fixture$db)
  child_b <- concurrent_dag_started(b)
  concurrent_dag_wait(job_id, 1L)
  dsHPC:::.worker_reap(fixture$db)
  child_c <- concurrent_dag_started(c)
  before <- concurrent_dag_steps(fixture, job_id)
  expect_equal(before$state, c("done", "running", "running"))
  expect_true(all(!is.na(before$worker_pid[2:3])))
  completed_output <- before$output_ref[1L]

  # Kill wrappers first so no durable exit marker can be written, then kill
  # their blocked shell runners. ps_wait is bounded and does not poll/sleep.
  pids <- c(before$worker_pid[2:3], child_b, child_c)
  handles <- lapply(pids, ps::ps_handle)
  for (pid in pids) dsHPC:::.signal_pid(pid, 9L)
  expect_true(all(ps::ps_wait(handles, timeout = 5000)))
  for (index in 2:3) {
    expect_false(file.exists(file.path(fixture$home, "artifacts", job_id,
      sprintf("step_%03d", index), "exit_code")))
    key <- paste0(job_id, "_", index)
    rm(list = key, envir = dsHPC:::.proc_registry)
  }
  dsHPC:::.db_close(fixture$db)
  fixture$db <- dsHPC:::.db_connect()
  dsHPC:::.worker_reap(fixture$db)
  dsHPC:::.worker_dispatch(fixture$db)
  concurrent_dag_started(b)
  concurrent_dag_started(c)
  retried <- concurrent_dag_steps(fixture, job_id)
  expect_equal(retried$state, c("done", "running", "running"))
  expect_equal(retried$retry_count, c(0L, 1L, 1L))
  expect_identical(retried$attempt_id[1L], before$attempt_id[1L])
  expect_true(all(retried$attempt_id[2:3] != before$attempt_id[2:3]))
  expect_identical(retried$output_ref[1L], completed_output)

  concurrent_dag_release(b)
  concurrent_dag_release(c)
  concurrent_dag_wait(job_id, 2:3)
  dsHPC:::.worker_reap(fixture$db)
  expect_identical(dsHPC:::.store_get_job(fixture$db, job_id)$state, "FINISHED")
  expect_equal(concurrent_dag_steps(fixture, job_id)$state, rep("done", 3L))
  expect_equal(readLines(counter), "launched")
})

test_that("fresh worker state adopts two live durable DAG attempts", {
  fixture <- local_concurrent_dag()
  a <- concurrent_dag_gate(fixture, "adopt_a")
  b <- concurrent_dag_gate(fixture, "adopt_b")
  job_id <- "job_live_attempts"
  concurrent_dag_create(fixture, job_id, list(
    a = concurrent_dag_node(a$runner), b = concurrent_dag_node(b$runner)))
  dsHPC:::.worker_dispatch(fixture$db)
  concurrent_dag_started(a)
  concurrent_dag_started(b)
  before <- concurrent_dag_steps(fixture, job_id)
  processes <- lapply(1:2, function(index) {
    key <- paste0(job_id, "_", index)
    proc <- dsHPC:::.proc_registry[[key]]
    rm(list = key, envir = dsHPC:::.proc_registry)
    proc
  })
  withr::defer(for (proc in processes) if (proc$is_alive()) proc$kill_tree())
  dsHPC:::.db_close(fixture$db)
  fixture$db <- dsHPC:::.db_connect()
  dsHPC:::.worker_reap(fixture$db)
  adopted <- concurrent_dag_steps(fixture, job_id)
  expect_equal(adopted$state, c("running", "running"))
  expect_identical(adopted$worker_pid, before$worker_pid)
  expect_identical(adopted$attempt_id, before$attempt_id)
  expect_equal(adopted$retry_count, c(0L, 0L))
  concurrent_dag_release(a)
  concurrent_dag_release(b)
  for (proc in processes) {
    proc$wait(10000)
    expect_false(proc$is_alive())
  }
  dsHPC:::.worker_reap(fixture$db)
  expect_identical(dsHPC:::.store_get_job(fixture$db, job_id)$state, "FINISHED")
})

test_that("SIGKILL of the worker preserves two attempts for a new leader", {
  fixture <- local_concurrent_dag()
  withr::local_options(list(dshpc.worker_poll_secs = 0.1))
  a <- concurrent_dag_gate(fixture, "leader_a")
  b <- concurrent_dag_gate(fixture, "leader_b")
  c <- concurrent_dag_gate(fixture, "leader_c")
  job_id <- "job_leader_takeover"
  concurrent_dag_create(fixture, job_id, list(
    a = concurrent_dag_node(a$runner), b = concurrent_dag_node(b$runner),
    c = concurrent_dag_node(c$runner, inputs = c("a", "b"))))
  withr::defer(concurrent_dag_stop_orphans(fixture, job_id))
  first <- concurrent_dag_worker(fixture)
  withr::defer(concurrent_dag_stop_worker(first))
  concurrent_dag_started(a)
  concurrent_dag_started(b)
  before <- concurrent_dag_steps(fixture, job_id)
  old_leader <- dsHPC:::.scheduler_worker_leader(fixture$db)$holder
  expect_equal(before$state, c("running", "running", "pending"))
  concurrent_dag_stop_worker(first)
  expect_true(all(vapply(before$worker_pid[1:2],
    dsHPC:::.pid_is_alive, logical(1))))

  # Advance only the lease clock; neither runner's state or PID is rewritten.
  DBI::dbExecute(fixture$db,
    "UPDATE scheduler_locks SET expires_at = '2000-01-01T00:00:00.000Z'
      WHERE name = 'worker_leader'")
  second <- concurrent_dag_worker(fixture)
  withr::defer(concurrent_dag_stop_worker(second))
  concurrent_dag_release(a)
  concurrent_dag_release(b)
  concurrent_dag_started(c)
  after <- concurrent_dag_steps(fixture, job_id)
  expect_false(identical(dsHPC:::.scheduler_worker_leader(fixture$db)$holder,
    old_leader))
  expect_equal(after$state, c("done", "done", "running"))
  expect_identical(after$attempt_id[1:2], before$attempt_id[1:2])
  expect_equal(after$retry_count, c(0L, 0L, 0L))
  last <- ps::ps_handle(after$worker_pid[3L])
  concurrent_dag_release(c)
  expect_true(ps::ps_wait(last, timeout = 10000))
  concurrent_dag_stop_worker(second)
  dsHPC:::.db_close(fixture$db)
  fixture$db <- dsHPC:::.db_connect()
  dsHPC:::.worker_reap(fixture$db)
  expect_identical(dsHPC:::.store_get_job(fixture$db, job_id)$state, "FINISHED")
})

test_that("a worker killed before PID commit cannot launch unrecorded runner code", {
  fixture <- local_concurrent_dag()
  withr::local_options(list(dshpc.max_retries = 1L))
  gate <- concurrent_dag_gate(fixture, "handshake_runner")
  pause <- list(started = file.path(fixture$home, "launch-paused"),
    release = file.path(fixture$home, "launch-release"))
  processx::run(Sys.which("mkfifo"), unlist(pause), timeout = 5)
  job_id <- "job_launch_handshake"
  concurrent_dag_create(fixture, job_id,
    list(a = concurrent_dag_node(gate$runner)))
  withr::defer(concurrent_dag_stop_orphans(fixture, job_id))
  worker <- concurrent_dag_worker(fixture, pause_launch = pause)
  withr::defer(concurrent_dag_stop_worker(worker))
  wrapper_pid <- concurrent_dag_started(pause)
  wrapper <- ps::ps_handle(wrapper_pid)
  expect_true(is.na(concurrent_dag_steps(fixture, job_id)$worker_pid[1L]))
  concurrent_dag_stop_worker(worker)
  expect_true(ps::ps_wait(wrapper, timeout = 10000))
  step_dir <- file.path(fixture$home, "artifacts", job_id, "step_001")
  expect_false(file.exists(file.path(step_dir, "child.pid")))
  expect_false(file.exists(file.path(step_dir, "exit_code")))
  expect_false(file.exists(file.path(step_dir, "output", "value.txt")))

  dsHPC:::.db_close(fixture$db)
  fixture$db <- dsHPC:::.db_connect()
  dsHPC:::.worker_reap(fixture$db)
  concurrent_dag_started(gate)
  expect_equal(concurrent_dag_steps(fixture, job_id)$retry_count, 1L)
  concurrent_dag_release(gate)
  concurrent_dag_wait(job_id, 1L)
  dsHPC:::.worker_reap(fixture$db)
  expect_identical(dsHPC:::.store_get_job(fixture$db, job_id)$state, "FINISHED")
})

test_that("a new leader retires an expired live daemon before retrying its launch", {
  fixture <- local_concurrent_dag()
  withr::local_options(list(dshpc.max_retries = 1L))
  gate <- concurrent_dag_gate(fixture, "expired_launch_runner")
  pause <- list(started = file.path(fixture$home, "prepare-paused"),
    release = file.path(fixture$home, "prepare-release"))
  processx::run(Sys.which("mkfifo"), unlist(pause), timeout = 5)
  job_id <- "job_expired_launch_owner"
  concurrent_dag_create(fixture, job_id,
    list(a = concurrent_dag_node(gate$runner)))
  withr::defer(concurrent_dag_stop_orphans(fixture, job_id))
  old_worker <- concurrent_dag_worker(fixture, pause_prepare = pause)
  withr::defer(concurrent_dag_stop_worker(old_worker))
  expect_equal(concurrent_dag_started(pause), old_worker$get_pid())
  before <- concurrent_dag_steps(fixture, job_id)
  expect_identical(before$state, "running")
  expect_true(is.na(before$worker_pid))
  expect_false(file.exists(file.path(fixture$home, "artifacts", job_id,
    "step_001", "runner.pid")))

  DBI::dbExecute(fixture$db,
    "UPDATE scheduler_locks SET expires_at = '2000-01-01T00:00:00.000Z'
      WHERE name = 'worker_leader'")
  runtime <- dsHPC:::.dshpc_env
  original_worker_id <- runtime$.worker_id
  replacement_id <- paste0("replacement-", Sys.getpid())
  runtime$.worker_id <- replacement_id
  withr::defer({
    dsHPC:::.scheduler_release_worker_leader(fixture$db, replacement_id)
    runtime$.worker_id <- original_worker_id
  })
  expect_true(dsHPC:::.scheduler_renew_worker_leader(fixture$db,
    worker_id = replacement_id))
  dsHPC:::.worker_reap(fixture$db)
  old_worker$wait(5000)
  expect_false(old_worker$is_alive())
  concurrent_dag_started(gate)
  after <- concurrent_dag_steps(fixture, job_id)
  expect_identical(after$state, "running")
  expect_equal(after$retry_count, 1L)
  expect_false(identical(after$attempt_id, before$attempt_id))
  expect_identical(after$launch_owner_pid, Sys.getpid())

  concurrent_dag_release(gate)
  concurrent_dag_wait(job_id, 1L)
  dsHPC:::.worker_reap(fixture$db)
  expect_identical(dsHPC:::.store_get_job(fixture$db, job_id)$state, "FINISHED")
})

test_that("launch retirement does not kill session owners or reused worker PIDs", {
  fixture <- local_concurrent_dag()
  session <- processx::process$new("/bin/cat", stdin = "|",
    stdout = "/dev/null", stderr = "/dev/null",
    cleanup = FALSE, cleanup_tree = FALSE)
  withr::defer(concurrent_dag_stop_worker(session))
  runtime <- dsHPC:::.dshpc_env
  original_worker_id <- runtime$.worker_id
  replacement_id <- paste0("retirement-check-", Sys.getpid())
  runtime$.worker_id <- replacement_id
  withr::defer({
    dsHPC:::.scheduler_release_worker_leader(fixture$db, replacement_id)
    runtime$.worker_id <- original_worker_id
  })
  expect_true(dsHPC:::.scheduler_renew_worker_leader(fixture$db,
    worker_id = replacement_id))
  expect_false(dsHPC:::.dag_retire_launch_owner(fixture$db, session$get_pid()))
  expect_true(session$is_alive())

  # A historical catalog entry must not turn a recycled PID into a kill target.
  dsHPC:::.scheduler_register_worker(fixture$db, worker_id = "stopped-worker")
  DBI::dbExecute(fixture$db,
    "UPDATE worker_nodes SET pid = ?, state = 'stopped'
      WHERE worker_id = 'stopped-worker'", params = list(session$get_pid()))
  expect_false(dsHPC:::.dag_retire_launch_owner(fixture$db, session$get_pid()))
  expect_true(session$is_alive())
  DBI::dbExecute(fixture$db,
    "UPDATE worker_nodes SET state = 'running', details_json = ?
      WHERE worker_id = 'stopped-worker'",
    params = list('{"process_start_time":1}'))
  expect_false(dsHPC:::.dag_retire_launch_owner(fixture$db, session$get_pid()))
  expect_true(session$is_alive())
})

test_that("shared tracking reveals no DAG fan-out while two steps run", {
  fixture <- local_concurrent_dag()
  testthat::local_mocked_bindings(
    .dshpc_worker_start = function() invisible(NULL), .package = "dsHPC")
  a <- concurrent_dag_gate(fixture, "tracking_a")
  b <- concurrent_dag_gate(fixture, "tracking_b")
  handle <- trusted_hpc_call(hpcSubmitInternal, list(
    label = "dsHPC_test", visibility = "global",
    reuse_fingerprint = strrep("b", 64L),
    dag = list(nodes = list(a = concurrent_dag_node(a$runner),
      b = concurrent_dag_node(b$runner)))))
  tracking_id <- handle$tracking_id
  queued <- hpcTrackingStatusDS(tracking_id)
  dsHPC:::.worker_dispatch(fixture$db)
  concurrent_dag_started(a)
  concurrent_dag_started(b)
  expect_equal(concurrent_dag_steps(fixture, handle$job_id)$state,
    c("running", "running"))
  running <- hpcTrackingStatusDS(tracking_id)
  expect_named(queued, c("tracking_id", "state", "is_done", "kind"))
  expect_named(running, names(queued))
  expect_identical(running$state, "running")
  expect_false(running$is_done)
  page <- hpcTrackingListDS()
  expect_named(page$items, c("tracking_id", "state", "is_done", "kind"))
  expect_equal(nrow(page$items), 1L)
  expect_identical(page$items$tracking_id, tracking_id)
  expect_equal(nrow(hpcTrackingOutputsDS(tracking_id)), 0L)

  concurrent_dag_release(a)
  concurrent_dag_wait(handle$job_id, 1L)
  dsHPC:::.worker_reap(fixture$db)
  expect_identical(hpcTrackingStatusDS(tracking_id), running)
  concurrent_dag_release(b)
  concurrent_dag_wait(handle$job_id, 2L)
  dsHPC:::.worker_reap(fixture$db)
  terminal <- hpcTrackingStatusDS(tracking_id)
  expect_named(terminal, names(queued))
  expect_identical(terminal$state, "terminal")
  expect_true(terminal$is_done)
  expect_equal(nrow(hpcTrackingOutputsDS(tracking_id)), 0L)
  expect_identical(hpcTrackingResultDS(tracking_id), list(
    ready = TRUE, summaries = list(), available_outputs = list()))
})
