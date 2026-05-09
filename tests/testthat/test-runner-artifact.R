test_that("blocked env vars are rejected in validation spec", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  # The BLOCKED_ENV_VARS list exists
  expect_true("LD_PRELOAD" %in% dsJobs:::.BLOCKED_ENV_VARS)
  expect_true("PATH" %in% dsJobs:::.BLOCKED_ENV_VARS)
  expect_true("PYTHONPATH" %in% dsJobs:::.BLOCKED_ENV_VARS)
})

test_that("env var names are prefixed with DSJOBS_CFG_", {
  env_vars <- character(0)
  config <- list(setting1 = "value1", setting2 = "42")
  for (nm in names(config)) {
    upper_nm <- toupper(nm)
    if (upper_nm %in% dsJobs:::.BLOCKED_ENV_VARS) stop("blocked")
    env_vars <- c(env_vars, paste0("DSJOBS_CFG_", upper_nm, "=",
                                    as.character(config[[nm]])))
  }
  expect_equal(env_vars[1], "DSJOBS_CFG_SETTING1=value1")
  expect_equal(env_vars[2], "DSJOBS_CFG_SETTING2=42")
})

test_that("runner argument templating is literal and vector-safe", {
  cfg <- list(args_template = list(
    "--input", "{input_dir}",
    "--output", "{output_dir}",
    "--path", "{settings_file}",
    "--spacing", "{resampled_spacing}",
    "--missing", "{nested}"
  ))
  step <- list(config = list(
    settings_file = "/tmp/a\\b/settings.yml",
    resampled_spacing = c(1, 1, 2),
    nested = list(skip = TRUE)
  ))

  args <- dsJobs:::.build_runner_args(cfg, step, "/tmp/step", "/tmp/input")
  expect_equal(args[2], "/tmp/input")
  expect_equal(args[4], "/tmp/step/output")
  expect_equal(args[6], "/tmp/a\\b/settings.yml")
  expect_equal(args[8], "1,1,2")
  expect_equal(args[10], "{nested}")
})

test_that("blocked env var in config list is caught", {
  config <- list(LD_PRELOAD = "/tmp/evil.so")
  expect_error({
    for (nm in names(config)) {
      if (toupper(nm) %in% dsJobs:::.BLOCKED_ENV_VARS)
        stop("Config key '", nm, "' is blocked for security.", call. = FALSE)
    }
  }, "blocked for security")
})

test_that("missing exit_code is not treated as success for durable wrappers", {
  step_dir <- tempfile("step_")
  dir.create(file.path(step_dir, "output"), recursive = TRUE)
  writeLines("partial", file.path(step_dir, "output", "partial.dat"))
  writeLines(c("#!/bin/sh", "write_exit_code 0"), file.path(step_dir, "run.sh"))

  expect_true(is.na(dsJobs:::.read_exit_code(step_dir)))

  writeLines("0", file.path(step_dir, "exit_code"))
  expect_equal(dsJobs:::.read_exit_code(step_dir), 0L)

  unlink(step_dir, recursive = TRUE)
})

test_that("legacy output-only jobs remain recoverable", {
  step_dir <- tempfile("step_")
  dir.create(file.path(step_dir, "output"), recursive = TRUE)
  writeLines("legacy", file.path(step_dir, "output", "result.dat"))

  expect_equal(dsJobs:::.read_exit_code(step_dir), 0L)

  unlink(step_dir, recursive = TRUE)
})

test_that("interrupted running jobs are requeued without losing state", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home, dsjobs.max_retries = 2))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  spec <- list(
    steps = list(list(type = "artifact", plane = "artifact",
                      runner = "dummy")),
    resource_class = "default")
  dsJobs:::.store_create_job(db, "job_interrupted", "user_a", spec, 1L)
  dsJobs:::.store_update_job(db, "job_interrupted",
    state = "RUNNING", step_index = 1L, worker_pid = 999999L)
  dsJobs:::.store_update_step(db, "job_interrupted", 1L, state = "running")

  dsJobs:::.worker_requeue_interrupted_step(db, "job_interrupted", 1L,
    "lost runner")

  job <- dsJobs:::.store_get_job(db, "job_interrupted")
  step <- DBI::dbGetQuery(db,
    "SELECT state, error_message FROM steps
     WHERE job_id = 'job_interrupted' AND step_index = 1")

  expect_equal(job$state, "PENDING")
  expect_equal(as.integer(job$retry_count), 1L)
  expect_true(is.na(job$worker_pid))
  expect_equal(step$state[1], "failed")
  expect_match(step$error_message[1], "lost runner")
})

test_that("worker recovers jobs committed after step completion before advance", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  spec <- make_test_spec(1)
  dsJobs:::.store_create_job(db, "job_done_needs_advance", "user_a", spec, 1L)
  dsJobs:::.store_update_job(db, "job_done_needs_advance",
    state = "RUNNING", step_index = 1L, worker_pid = NA_integer_)
  dsJobs:::.store_update_step(db, "job_done_needs_advance", 1L,
    state = "done", finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z",
                                          tz = "UTC"))

  dsJobs:::.worker_reap(db)

  job <- dsJobs:::.store_get_job(db, "job_done_needs_advance")
  expect_equal(job$state, "FINISHED")
  expect_true(is.na(job$worker_pid))
})
