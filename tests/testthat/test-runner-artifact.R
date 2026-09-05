test_that("blocked env vars are rejected in validation spec", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  # The BLOCKED_ENV_VARS list exists
  expect_true("LD_PRELOAD" %in% dsHPC:::.BLOCKED_ENV_VARS)
  expect_true("PATH" %in% dsHPC:::.BLOCKED_ENV_VARS)
  expect_true("PYTHONPATH" %in% dsHPC:::.BLOCKED_ENV_VARS)
})

test_that("env var names are prefixed with DSHPC_CFG_", {
  env_vars <- character(0)
  config <- list(setting1 = "value1", setting2 = "42")
  for (nm in names(config)) {
    upper_nm <- toupper(nm)
    if (upper_nm %in% dsHPC:::.BLOCKED_ENV_VARS) stop("blocked")
    env_vars <- c(env_vars, paste0("DSHPC_CFG_", upper_nm, "=",
                                    as.character(config[[nm]])))
  }
  expect_equal(env_vars[1], "DSHPC_CFG_SETTING1=value1")
  expect_equal(env_vars[2], "DSHPC_CFG_SETTING2=42")
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

  args <- dsHPC:::.build_runner_args(cfg, step, "/tmp/step", "/tmp/input")
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
      if (toupper(nm) %in% dsHPC:::.BLOCKED_ENV_VARS)
        stop("Config key '", nm, "' is blocked for security.", call. = FALSE)
    }
  }, "blocked for security")
})

test_that("missing exit_code is not treated as success for durable wrappers", {
  step_dir <- tempfile("step_")
  dir.create(file.path(step_dir, "output"), recursive = TRUE)
  writeLines("partial", file.path(step_dir, "output", "partial.dat"))
  writeLines(c("#!/bin/sh", "write_exit_code 0"), file.path(step_dir, "run.sh"))

  expect_true(is.na(dsHPC:::.read_exit_code(step_dir)))

  writeLines("0", file.path(step_dir, "exit_code"))
  expect_equal(dsHPC:::.read_exit_code(step_dir), 0L)

  unlink(step_dir, recursive = TRUE)
})

test_that("legacy output-only jobs remain recoverable", {
  step_dir <- tempfile("step_")
  dir.create(file.path(step_dir, "output"), recursive = TRUE)
  writeLines("legacy", file.path(step_dir, "output", "result.dat"))

  expect_equal(dsHPC:::.read_exit_code(step_dir), 0L)

  unlink(step_dir, recursive = TRUE)
})

test_that("interrupted running jobs are requeued without losing state", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.max_retries = 2))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  spec <- list(
    steps = list(list(type = "artifact", plane = "artifact",
                      runner = "dummy")),
    resource_class = "default")
  dsHPC:::.store_create_job(db, "job_interrupted", "user_a", spec, 1L)
  dsHPC:::.store_update_job(db, "job_interrupted",
    state = "RUNNING", step_index = 1L, worker_pid = 999999L)
  dsHPC:::.store_update_step(db, "job_interrupted", 1L, state = "running")

  dsHPC:::.worker_requeue_interrupted_step(db, "job_interrupted", 1L,
    "lost runner")

  job <- dsHPC:::.store_get_job(db, "job_interrupted")
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
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  spec <- make_test_spec(1)
  dsHPC:::.store_create_job(db, "job_done_needs_advance", "user_a", spec, 1L)
  dsHPC:::.store_update_job(db, "job_done_needs_advance",
    state = "RUNNING", step_index = 1L, worker_pid = NA_integer_)
  dsHPC:::.store_update_step(db, "job_done_needs_advance", 1L,
    state = "done", finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z",
                                          tz = "UTC"))

  dsHPC:::.worker_reap(db)

  job <- dsHPC:::.store_get_job(db, "job_done_needs_advance")
  expect_equal(job$state, "FINISHED")
  expect_true(is.na(job$worker_pid))
})

test_that("worker GC never expires domain-package workflows", {
  gc_body <- paste(deparse(body(dsHPC:::.worker_gc)), collapse = "\n")
  expect_false(grepl("dsImaging", gc_body, fixed = TRUE))
  expect_false(grepl("cleanup_stale_generations", gc_body, fixed = TRUE))
})

test_that("embedded runners inherit only the explicit confined environment", {
  skip_on_os("windows")
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  env_names <- c("DSHPC_ADMIN_KEY", "DSHPC_PARENT_SECRET")
  old_env <- Sys.getenv(env_names, unset = NA_character_)
  on.exit({
    Sys.unsetenv(env_names)
    restore <- !is.na(old_env)
    if (any(restore)) do.call(Sys.setenv, as.list(old_env[restore]))
  }, add = TRUE)
  do.call(Sys.setenv, as.list(c(
    DSHPC_ADMIN_KEY = "admin-secret-marker",
    DSHPC_PARENT_SECRET = "parent-secret-marker")))
  on.exit(cleanup_test_home(home), add = TRUE)

  yaml::write_yaml(list(
    name = "environment_probe",
    command = Sys.which("env"),
    args_template = character(0),
    allowed_params = character(0),
    env = list(RUNNER_DECLARED = "declared-value")),
    file.path(home, "runners", "environment_probe.yml"))
  step <- list(type = "run", plane = "artifact",
    runner = "environment_probe", config = list())
  spec <- list(label = "dsHPC_test", steps = list(step))
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  dsHPC:::.store_create_job(db, "job_clean_env", "owner", spec, 1L)
  step_dir <- dsHPC:::.ensure_step_dir("job_clean_env", 1L)

  prepared <- dsHPC:::.prepare_artifact_command(db, "job_clean_env", 1L,
    step, step_dir, NULL)
  expect_false(any(unname(prepared$env_vars) == "current"))
  expect_true(startsWith(prepared$env_vars[["HOME"]], step_dir))
  expect_true(startsWith(prepared$env_vars[["TMPDIR"]], step_dir))
  expect_true(dir.exists(prepared$env_vars[["HOME"]]))
  expect_true(dir.exists(prepared$env_vars[["TMPDIR"]]))

  dsHPC:::.launch_artifact_process(prepared, step_dir)
  deadline <- Sys.time() + 10
  exit_file <- file.path(step_dir, "exit_code")
  while (!file.exists(exit_file) && Sys.time() < deadline) Sys.sleep(0.05)
  expect_true(file.exists(exit_file))
  expect_equal(readLines(exit_file, n = 1L, warn = FALSE), "0")
  output <- paste(readLines(file.path(step_dir, "stdout.log"), warn = FALSE),
    collapse = "\n")
  expect_match(output, "RUNNER_DECLARED=declared-value", fixed = TRUE)
  expect_match(output, paste0("HOME=", prepared$env_vars[["HOME"]]),
    fixed = TRUE)
  expect_false(grepl("admin-secret-marker", output, fixed = TRUE))
  expect_false(grepl("parent-secret-marker", output, fixed = TRUE))
  expect_false(grepl("DSHPC_ADMIN_KEY", output, fixed = TRUE))
  expect_false(grepl("DSHPC_PARENT_SECRET", output, fixed = TRUE))
  diagnostic <- paste(readLines(file.path(step_dir, "env.log"), warn = FALSE),
    collapse = "\n")
  expect_false(grepl("admin-secret-marker|parent-secret-marker", diagnostic))
})

test_that("execution revalidates persisted runner parameters", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))
  yaml::write_yaml(list(name = "closed_runtime", command = "/bin/echo",
    args_template = character(0)),
    file.path(home, "runners", "closed_runtime.yml"))
  step <- list(type = "run", plane = "artifact", runner = "closed_runtime",
    config = list(secret = "must-not-run"))
  spec <- list(label = "dsHPC_test", steps = list(step))
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  dsHPC:::.store_create_job(db, "job_closed_runtime", "owner", spec, 1L)
  step_dir <- dsHPC:::.ensure_step_dir("job_closed_runtime", 1L)

  expect_error(dsHPC:::.prepare_artifact_command(db, "job_closed_runtime",
    1L, step, step_dir, NULL), "does not allow: secret", fixed = TRUE)
  expect_false(file.exists(file.path(step_dir, "run.sh")))
})

test_that("execution refuses a runner definition changed after submission", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))
  runner_path <- file.path(home, "runners", "sealed_runtime.yml")
  yaml::write_yaml(list(name = "sealed_runtime", command = "/bin/echo",
    args_template = "first", allowed_params = character(0)), runner_path)
  catalogue_before <- trusted_hpc_call(hpcRuntimeIdentityInternal)
  step <- list(type = "run", plane = "artifact", runner = "sealed_runtime",
    config = list())
  spec <- dsHPC:::.validate_job_spec(list(label = "dsHPC_test",
    visibility = "global", reuse_fingerprint = strrep("a", 64L),
    steps = list(step)))
  spec$.dshpc_provider <- "dsHPC"
  spec$.dshpc_runtime_revision <- dsHPC:::.dshpc_runtime_revision(spec)
  spec$.dshpc_runtime_identity <- dsHPC:::.dshpc_runtime_identity(
    spec, "dsHPC")

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  dsHPC:::.store_create_job(db, "job_sealed_runtime", "owner", spec, 1L)
  step_dir <- dsHPC:::.ensure_step_dir("job_sealed_runtime", 1L)
  yaml::write_yaml(list(name = "sealed_runtime", command = "/bin/echo",
    args_template = "second", allowed_params = character(0)), runner_path)
  catalogue_after <- trusted_hpc_call(hpcRuntimeIdentityInternal)

  expect_false(identical(catalogue_before, catalogue_after))
  expect_error(dsHPC:::.prepare_artifact_command(db, "job_sealed_runtime",
    1L, step, step_dir, NULL), "runtime contract changed", fixed = TRUE)
  expect_error(dsHPC:::.executor_run_step(db, "job_sealed_runtime", 1L,
    spec), "runtime contract changed", fixed = TRUE)
  expect_false(file.exists(file.path(step_dir, "run.sh")))
})

test_that("runtime drift during advance terminalizes the job and its leases", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))
  runner_path <- file.path(home, "runners", "advance_runtime.yml")
  yaml::write_yaml(list(name = "advance_runtime", command = "/bin/echo",
    args_template = "first", allowed_params = character(0)), runner_path)
  step <- list(type = "run", plane = "artifact", runner = "advance_runtime",
    config = list())
  spec <- dsHPC:::.validate_job_spec(list(label = "dsHPC_test",
    visibility = "global", reuse_fingerprint = strrep("b", 64L),
    steps = list(step, step)))
  spec$.dshpc_provider <- "dsHPC"
  spec$.dshpc_runtime_revision <- dsHPC:::.dshpc_runtime_revision(spec)
  spec$.dshpc_runtime_identity <- dsHPC:::.dshpc_runtime_identity(
    spec, "dsHPC")

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  dsHPC:::.store_create_job(db, "job_advance_runtime", "owner", spec, 2L)
  dsHPC:::.store_update_job(db, "job_advance_runtime", state = "RUNNING",
    step_index = 1L)
  dsHPC:::.store_update_step(db, "job_advance_runtime", 1L, state = "done")
  DBI::dbExecute(db,
    "INSERT INTO resource_leases
       (job_id, resource, amount, acquired_at) VALUES (?, ?, ?, ?)",
    params = list("job_advance_runtime", "cpu", 1,
      format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")))
  yaml::write_yaml(list(name = "advance_runtime", command = "/bin/echo",
    args_template = "second", allowed_params = character(0)), runner_path)

  dsHPC:::.worker_reap(db)
  job <- dsHPC:::.store_get_job(db, "job_advance_runtime")
  expect_identical(job$state, "FAILED")
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM resource_leases WHERE job_id = ?",
    params = list("job_advance_runtime"))$n, 0)
  expect_true("advance_failed" %in% DBI::dbGetQuery(db,
    "SELECT event FROM events WHERE job_id = ?",
    params = list("job_advance_runtime"))$event)
})
