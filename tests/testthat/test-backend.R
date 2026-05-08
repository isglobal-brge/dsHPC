test_that("executor backend status reports embedded and missing slurm", {
  withr::local_options(list(dsjobs.executor_backend = "embedded"))
  embedded <- dsJobs:::.executor_backend_status()
  expect_equal(embedded$backend, "embedded")
  expect_true(embedded$available)
  expect_false(embedded$delegates_resources)

  withr::local_options(list(
    dsjobs.executor_backend = "slurm",
    dsjobs.slurm_sbatch = "/definitely/not/sbatch"))
  slurm <- dsJobs:::.executor_backend_status()
  expect_equal(slurm$backend, "slurm")
  expect_false(slurm$available)
  expect_equal(slurm$reason, "sbatch_not_found")
})

test_that("external backends delegate local resources unless configured otherwise", {
  home <- setup_test_home()
  writeLines(c(
    "name: heavy",
    "plane: artifact",
    "command: /bin/sh",
    "args_template: ['-c', 'true']",
    "resources:",
    "  memory_mb: 8192",
    "  cpu_slots: 4",
    "  max_concurrent: 1",
    "  concurrency_group: heavy_group"
  ), file.path(home, "runners", "heavy.yml"))
  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.executor_backend = "slurm",
    dsjobs.node_memory_mb = 4096,
    dsjobs.memory_reserve_mb = 0,
    dsjobs.cpu_slots = 1
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "heavy", config = list())))

  d1 <- dsJobs:::.scheduler_can_start_job(db, "job_ext_a", spec)
  expect_true(d1$ok)
  expect_lt(d1$budget$memory_mb, d1$plan$memory_mb)

  dsJobs:::.store_create_job(db, "job_ext_running", "user", spec, 1L)
  dsJobs:::.store_update_job(db, "job_ext_running", state = "RUNNING")
  d2 <- dsJobs:::.scheduler_can_start_job(db, "job_ext_b", spec)
  expect_true(d2$ok)

  withr::local_options(list(dsjobs.external_enforce_runner_concurrency = TRUE))
  d3 <- dsJobs:::.scheduler_can_start_job(db, "job_ext_c", spec)
  expect_false(d3$ok)
  expect_match(d3$reason, "concurrency")
})

test_that("external command backend can submit and reap an artifact step", {
  home <- setup_test_home()
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  submit <- file.path(bin, "submit")
  status <- file.path(bin, "status")
  writeLines(c(
    "#!/bin/sh",
    "sh \"$DSJOBS_STEP_SCRIPT\" >/dev/null 2>&1",
    "echo ext-123"
  ), submit)
  writeLines(c(
    "#!/bin/sh",
    "echo SUCCEEDED 0"
  ), status)
  Sys.chmod(c(submit, status), "0755")

  writeLines(c(
    "name: shell_ok",
    "plane: artifact",
    "command: /bin/sh",
    "args_template:",
    "  - -c",
    "  - mkdir -p {output_dir}; echo ok > {output_dir}/ok.txt",
    "resources:",
    "  memory_mb: 64",
    "  cpu_slots: 1"
  ), file.path(home, "runners", "shell_ok.yml"))

  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.executor_backend = "external",
    dsjobs.external_submit_cmd = submit,
    dsjobs.external_status_cmd = status,
    dsjobs.max_retries = 0
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "shell_ok", config = list())))
  dsJobs:::.store_create_job(db, "job_external", "user", spec, 1L)
  dsJobs:::.store_update_job(db, "job_external", state = "RUNNING", step_index = 1L)
  dsJobs:::.executor_run_step(db, "job_external", 1L, spec)

  step <- DBI::dbGetQuery(db,
    "SELECT external_backend, external_id FROM steps WHERE job_id = ?",
    params = list("job_external"))
  expect_equal(step$external_backend, "external")
  expect_equal(step$external_id, "ext-123")

  dsJobs:::.worker_reap(db)
  job <- dsJobs:::.store_get_job(db, "job_external")
  expect_equal(job$state, "FINISHED")
  outputs <- DBI::dbGetQuery(db,
    "SELECT name FROM outputs WHERE job_id = ?",
    params = list("job_external"))
  expect_true("ok.txt" %in% outputs$name)
})

test_that("slurm backend submits with runner resources and reaps completion", {
  home <- setup_test_home()
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  sbatch <- file.path(bin, "sbatch")
  squeue <- file.path(bin, "squeue")
  sacct <- file.path(bin, "sacct")
  args_file <- file.path(home, "sbatch_args.txt")
  withr::local_envvar(c(DSJOBS_FAKE_SLURM_ARGS = args_file))
  writeLines(c(
    "#!/bin/sh",
    "printf '%s\\n' \"$@\" > \"$DSJOBS_FAKE_SLURM_ARGS\"",
    "for last do :; done",
    "sh \"$last\" >/dev/null 2>&1",
    "echo 12345"
  ), sbatch)
  writeLines(c("#!/bin/sh", "exit 0"), squeue)
  writeLines(c("#!/bin/sh", "echo COMPLETED\\|0:0"), sacct)
  Sys.chmod(c(sbatch, squeue, sacct), "0755")

  writeLines(c(
    "name: shell_slurm",
    "plane: artifact",
    "command: /bin/sh",
    "args_template:",
    "  - -c",
    "  - mkdir -p {output_dir}; echo ok > {output_dir}/ok.txt",
    "resources:",
    "  memory_mb: 4096",
    "  cpu_slots: 2"
  ), file.path(home, "runners", "shell_slurm.yml"))

  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.executor_backend = "slurm",
    dsjobs.slurm_sbatch = sbatch,
    dsjobs.slurm_squeue = squeue,
    dsjobs.slurm_sacct = sacct,
    dsjobs.max_retries = 0
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "shell_slurm", config = list())))
  dsJobs:::.store_create_job(db, "job_slurm", "user", spec, 1L)
  dsJobs:::.store_update_job(db, "job_slurm", state = "RUNNING", step_index = 1L)
  dsJobs:::.executor_run_step(db, "job_slurm", 1L, spec)

  args <- readLines(args_file, warn = FALSE)
  expect_true("--mem=4096" %in% args)
  expect_true("--cpus-per-task=2" %in% args)

  dsJobs:::.worker_reap(db)
  job <- dsJobs:::.store_get_job(db, "job_slurm")
  expect_equal(job$state, "FINISHED")
})

options(
  dsjobs.home = NULL,
  dsjobs.executor_backend = NULL,
  dsjobs.slurm_sbatch = NULL,
  dsjobs.slurm_squeue = NULL,
  dsjobs.slurm_sacct = NULL,
  dsjobs.external_submit_cmd = NULL,
  dsjobs.external_status_cmd = NULL,
  dsjobs.external_enforce_runner_concurrency = NULL)
