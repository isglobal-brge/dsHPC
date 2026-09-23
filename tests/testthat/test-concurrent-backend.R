local_concurrent_backend <- function(.local_envir = parent.frame()) {
  skip_on_os("windows")
  fixture <- new.env(parent = emptyenv())
  fixture$home <- setup_test_home()
  withr::local_options(list(dshpc.home = fixture$home,
    dshpc.worker_autostart = FALSE, dshpc.step_cache = FALSE,
    dshpc.max_parallel_steps = 2L, dshpc.max_retries = 0L,
    dshpc.executor_backend = "embedded", dshpc.cpu_slots = 4L,
    dshpc.node_memory_mb = 2048L, dshpc.memory_reserve_mb = 0L,
    dshpc.gpu_count = 0L), .local_envir = .local_envir)
  fixture$db <- dsHPC:::.db_connect()
  withr::defer({
    rows <- dsHPC:::.store_list_jobs(fixture$db, owner_id = NULL)
    for (job_id in rows$job_id) {
      for (index in 1:2) {
        key <- paste0(job_id, "_", index)
        proc <- dsHPC:::.proc_registry[[key]]
        if (!is.null(proc)) {
          if (proc$is_alive()) proc$kill_tree()
          proc$wait(5000)
          rm(list = key, envir = dsHPC:::.proc_registry)
        }
      }
    }
    dsHPC:::.db_close(fixture$db)
    cleanup_test_home(fixture$home)
  }, envir = .local_envir)
  fixture
}

concurrent_backend_create <- function(fixture, job_id, runner) {
  node <- list(type = "run_artifact", plane = "artifact", runner = runner,
    config = list())
  spec <- dsHPC:::.validate_job_spec(list(label = "concurrent-backend-test",
    visibility = "private", dag = list(nodes = list(a = node, b = node))))
  dsHPC:::.store_create_job(fixture$db, job_id, "owner", spec, 2L)
  spec
}

concurrent_backend_external <- function(fixture, .local_envir = parent.frame()) {
  bin <- file.path(fixture$home, "bin")
  dir.create(bin)
  state <- file.path(fixture$home, "backend-state")
  dir.create(state)
  submit <- file.path(bin, "submit")
  status <- file.path(bin, "status")
  cancel <- file.path(bin, "cancel")
  writeLines(c("#!/bin/sh",
    paste0("state=", shQuote(state)),
    "id=unit-${DSHPC_STEP_INDEX}",
    "printf '%s\\n' RUNNING > \"$state/$id.status\"",
    "printf '%s\\n' \"$DSHPC_STEP_SCRIPT\" > \"$state/$id.script\"",
    "printf '%s\\n' \"$id\" >> \"$state/submitted\"",
    "printf '%s\\n' \"$id\""), submit)
  writeLines(c("#!/bin/sh", paste0("state=", shQuote(state)),
    "cat \"$state/$DSHPC_EXTERNAL_ID.status\""), status)
  writeLines(c("#!/bin/sh", paste0("state=", shQuote(state)),
    "printf '%s\\n' \"$DSHPC_EXTERNAL_ID\" >> \"$state/cancelled\""), cancel)
  Sys.chmod(c(submit, status, cancel), "0755")
  yaml::write_yaml(list(name = "remote_branch", command = "/bin/sh",
    args_template = c("-c", paste(
      "printf '%s\\n' \"$DSHPC_STEP_INDEX\" > \"$DSHPC_OUTPUT_DIR/value.txt\"")),
    resources = list(memory_mb = 32L, cpu_slots = 1L)),
    file.path(fixture$home, "runners", "remote_branch.yml"))
  withr::local_options(list(dshpc.executor_backend = "external",
    dshpc.external_submit_cmd = submit, dshpc.external_status_cmd = status,
    dshpc.external_cancel_cmd = cancel), .local_envir = .local_envir)
  state
}

concurrent_backend_complete <- function(state, index) {
  id <- paste0("unit-", index)
  script <- readLines(file.path(state, paste0(id, ".script")))
  result <- processx::run("/bin/sh", script, timeout = 10,
    error_on_status = FALSE)
  expect_equal(result$status, 0L)
  writeLines("SUCCEEDED 0", file.path(state, paste0(id, ".status")))
}

test_that("external DAG units accept concurrent independent ids and reap separately", {
  fixture <- local_concurrent_backend()
  state <- concurrent_backend_external(fixture)
  job_id <- "job_parallel_external"
  concurrent_backend_create(fixture, job_id, "remote_branch")
  dsHPC:::.worker_dispatch(fixture$db)
  rows <- dsHPC:::.dag_steps(fixture$db, job_id)
  expect_equal(rows$state, c("running", "running"))
  expect_equal(rows$external_id, c("unit-1", "unit-2"))
  expect_equal(readLines(file.path(state, "submitted")), rows$external_id)
  expect_equal(dsHPC:::.store_get_job(fixture$db, job_id)$step_index, 0L)

  # Lose one SQLite backend acknowledgement and recover it from that step's
  # durable marker while its sibling keeps a separate accepted backend id.
  dsHPC:::.store_update_step(fixture$db, job_id, 2L,
    external_backend = NA_character_, external_id = NA_character_)
  dsHPC:::.worker_reap(fixture$db)
  expect_equal(dsHPC:::.dag_steps(fixture$db, job_id)$external_id,
    c("unit-1", "unit-2"))

  concurrent_backend_complete(state, 1L)
  dsHPC:::.worker_reap(fixture$db)
  expect_equal(dsHPC:::.dag_steps(fixture$db, job_id)$state,
    c("done", "running"))
  expect_equal(dsHPC:::.store_get_job(fixture$db, job_id)$step_index, 1L)
  expect_equal(dsHPC:::.store_get_job(fixture$db, job_id)$state, "RUNNING")
  concurrent_backend_complete(state, 2L)
  dsHPC:::.worker_reap(fixture$db)
  expect_equal(dsHPC:::.dag_steps(fixture$db, job_id)$state, c("done", "done"))
  expect_equal(dsHPC:::.store_get_job(fixture$db, job_id)$state, "FINISHED")
  outputs <- DBI::dbGetQuery(fixture$db,
    "SELECT step_index FROM outputs WHERE job_id = ? ORDER BY step_index",
    params = list(job_id))
  expect_equal(outputs$step_index, 1:2)
  expect_equal(readLines(file.path(state, "submitted")), c("unit-1", "unit-2"))
})

test_that("external DAG cancellation waits for the remaining branch acknowledgement", {
  fixture <- local_concurrent_backend()
  state <- concurrent_backend_external(fixture)
  job_id <- "job_cancel_external_branch"
  concurrent_backend_create(fixture, job_id, "remote_branch")
  dsHPC:::.worker_dispatch(fixture$db)
  concurrent_backend_complete(state, 1L)
  dsHPC:::.worker_reap(fixture$db)
  expect_identical(dsHPC:::.executor_kill(fixture$db, job_id), "requested")
  expect_equal(readLines(file.path(state, "cancelled")), "unit-2")
  dsHPC:::.worker_reap(fixture$db)
  expect_equal(dsHPC:::.dag_steps(fixture$db, job_id)$state,
    c("done", "running"))
  expect_equal(dsHPC:::.store_get_job(fixture$db, job_id)$state, "RUNNING")
  writeLines("CANCELLED 143", file.path(state, "unit-2.status"))
  dsHPC:::.worker_reap(fixture$db)
  expect_equal(dsHPC:::.store_get_job(fixture$db, job_id)$state, "CANCELLED")
  expect_equal(dsHPC:::.dag_steps(fixture$db, job_id)$state, c("done", "failed"))
  expect_equal(dsHPC:::.dag_steps(fixture$db, job_id)$exit_code, c(0L, 143L))
  expect_equal(readLines(file.path(state, "cancelled")), "unit-2")
})

test_that("embedded DAG containers use container commands and overlap", {
  fixture <- local_concurrent_backend()
  bin <- file.path(fixture$home, "bin")
  dir.create(bin)
  runtime <- file.path(bin, "docker")
  writeLines(c("#!/bin/sh",
    "printf '%s\\n' \"$@\" > \"$DSHPC_STEP_DIR/docker-args\"",
    "while [ \"$#\" -gt 0 ] && [ \"$1\" != fixture:latest ]; do shift; done",
    "[ \"$#\" -gt 0 ] || exit 54", "shift", "exec \"$@\""), runtime)
  Sys.chmod(runtime, "0755")
  withr::local_envvar(c(PATH = paste(bin, Sys.getenv("PATH"), sep = .Platform$path.sep)))
  withr::local_options(list(dshpc.container_run_as_current_user = FALSE,
    dshpc.container_network = "none"))
  started <- file.path(fixture$home, paste0("started-", 1:2))
  release <- file.path(fixture$home, paste0("release-", 1:2))
  processx::run(Sys.which("mkfifo"), c(started, release), timeout = 5)
  payload <- paste(
    paste0("gate_home=", shQuote(fixture$home)), ";",
    "printf '%s\\n' \"$DSHPC_STEP_INDEX\" > \"$gate_home/started-$DSHPC_STEP_INDEX\";",
    "read release < \"$gate_home/release-$DSHPC_STEP_INDEX\";",
    "printf '%s\\n' container > \"$DSHPC_OUTPUT_DIR/value.txt\"")
  yaml::write_yaml(list(name = "embedded_container", command = "/missing/native/runner",
    args_template = "native-args-must-not-run",
    container = list(image = "fixture:latest", runtime = "docker", pull = "never",
      command = "/bin/sh", args_template = c("-c", payload)),
    resources = list(memory_mb = 32L, cpu_slots = 1L)),
    file.path(fixture$home, "runners", "embedded_container.yml"))
  job_id <- "job_parallel_containers"
  concurrent_backend_create(fixture, job_id, "embedded_container")
  dsHPC:::.worker_dispatch(fixture$db)
  expect_equal(dsHPC:::.dag_steps(fixture$db, job_id)$state, c("running", "running"))
  for (index in 1:2) {
    output <- processx::run("/bin/cat", started[index], timeout = 10)
    expect_equal(trimws(output$stdout), as.character(index))
    args <- readLines(file.path(fixture$home, "artifacts", job_id,
      sprintf("step_%03d", index), "docker-args"))
    expect_equal(args[match("--network", args) + 1L], "none")
    expect_equal(args[match("fixture:latest", args) + 1L], "/bin/sh")
    expect_false(any(grepl("missing/native|native-args-must-not-run", args)))
  }
  for (path in release) {
    processx::run("/bin/sh", c("-c",
      paste("printf '%s\\n' release >", shQuote(path))), timeout = 10)
  }
  for (index in 1:2) {
    proc <- dsHPC:::.proc_registry[[paste0(job_id, "_", index)]]
    proc$wait(10000)
    expect_false(proc$is_alive())
  }
  dsHPC:::.worker_reap(fixture$db)
  expect_equal(dsHPC:::.store_get_job(fixture$db, job_id)$state, "FINISHED")
  expect_equal(dsHPC:::.dag_steps(fixture$db, job_id)$state, c("done", "done"))
})
