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
  marker <- dsJobs:::.backend_read_external_marker(file.path(
    home, "artifacts", "job_external", "step_001"))
  expect_equal(marker$backend, "external")
  expect_equal(marker$external_id, "ext-123")

  # Simulate a worker crash after backend submission but before the DB row is
  # fully durable. The recovery marker lets the next worker resume status sync.
  dsJobs:::.store_update_step(db, "job_external", 1L,
    external_backend = NA_character_,
    external_id = NA_character_)

  dsJobs:::.worker_reap(db)
  job <- dsJobs:::.store_get_job(db, "job_external")
  expect_equal(job$state, "FINISHED")
  outputs <- DBI::dbGetQuery(db,
    "SELECT name FROM outputs WHERE job_id = ?",
    params = list("job_external"))
  expect_true("ok.txt" %in% outputs$name)
})

test_that("external status command failures do not create duplicate retries", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  status <- file.path(home, "status")
  writeLines(c("#!/bin/sh", "echo temporary scheduler outage >&2", "exit 2"),
    status)
  Sys.chmod(status, "0755")

  withr::local_options(list(
    dsjobs.executor_backend = "external",
    dsjobs.external_status_cmd = status
  ))

  state <- dsJobs:::.backend_status_external("ext-unknown",
    file.path(home, "artifacts", "job_x", "step_001"))
  expect_equal(state$state, "running")
  expect_equal(state$external_state, "STATUS_UNKNOWN")
  expect_true(is.na(state$exit_code))
})

test_that("backend step scripts write exit_code atomically", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  script <- file.path(home, "artifacts", "run_step.sh")
  prepared <- list(
    env_vars = c(DSJOBS_OUTPUT_DIR = file.path(home, "out")),
    command = "/bin/sh",
    args = c("-c", "true"),
    step_dir = dirname(script),
    output_dir = file.path(home, "out"),
    runner_config = list())
  dsJobs:::.backend_write_step_script(script, prepared)
  lines <- readLines(script, warn = FALSE)
  expect_true(any(grepl("exit_code.tmp", lines, fixed = TRUE)))
  expect_true(any(grepl("mv exit_code.tmp exit_code", lines, fixed = TRUE)))
})

test_that("backend path mappings support alternate host/container views", {
  withr::local_options(list(dsjobs.backend_path_mappings = c(
    "/srv/dsjobs" = "/host/dsjobs",
    "/srv/dsjobs/artifacts/special" = "/fast/artifacts/special"
  )))
  maps <- dsJobs:::.backend_path_mappings()
  expect_equal(maps$local[1], "/srv/dsjobs/artifacts/special")
  expect_equal(dsJobs:::.backend_map_path(
    "/srv/dsjobs/artifacts/job_x", "local_to_backend"), "/host/dsjobs/artifacts/job_x")
  expect_equal(dsJobs:::.backend_map_path(
    "/host/dsjobs/artifacts/job_x", "backend_to_local"), "/srv/dsjobs/artifacts/job_x")
  expect_equal(dsJobs:::.backend_map_text(
    "write /srv/dsjobs/artifacts/job_x/out", "local_to_backend"),
    "write /host/dsjobs/artifacts/job_x/out")
})

test_that("external backend can execute through a mapped backend path", {
  home <- setup_test_home()
  backend_home <- file.path(tempdir(), paste0("dsjobs_backend_view_", Sys.getpid()))
  if (!file.symlink(home, backend_home))
    skip("filesystem does not support symlinks for backend path mapping test")
  on.exit(unlink(backend_home, recursive = TRUE), add = TRUE)

  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  submit <- file.path(bin, "submit")
  status <- file.path(bin, "status")
  writeLines(c(
    "#!/bin/sh",
    "printf '%s\\n' \"$DSJOBS_STEP_SCRIPT\" > \"$DSJOBS_LOCAL_STEP_DIR/submit_script.txt\"",
    "printf '%s\\n' \"$DSJOBS_LOCAL_STEP_SCRIPT\" > \"$DSJOBS_LOCAL_STEP_DIR/local_script.txt\"",
    "sh \"$DSJOBS_STEP_SCRIPT\" >/dev/null 2>&1",
    "echo ext-map-123"
  ), submit)
  writeLines(c("#!/bin/sh", "echo SUCCEEDED 0"), status)
  Sys.chmod(c(submit, status), "0755")

  writeLines(c(
    "name: shell_map",
    "plane: artifact",
    "command: /bin/sh",
    "args_template:",
    "  - -c",
    "  - mkdir -p {output_dir}; echo mapped > {output_dir}/mapped.txt",
    "resources:",
    "  memory_mb: 64",
    "  cpu_slots: 1"
  ), file.path(home, "runners", "shell_map.yml"))

  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.executor_backend = "external",
    dsjobs.external_submit_cmd = submit,
    dsjobs.external_status_cmd = status,
    dsjobs.backend_path_mappings = c(stats::setNames(backend_home, home)),
    dsjobs.max_retries = 0
  ))
  on.exit(cleanup_test_home(home), add = TRUE)

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "shell_map", config = list())))
  dsJobs:::.store_create_job(db, "job_mapped", "user", spec, 1L)
  dsJobs:::.store_update_job(db, "job_mapped", state = "RUNNING", step_index = 1L)
  dsJobs:::.executor_run_step(db, "job_mapped", 1L, spec)

  local_step <- file.path(home, "artifacts", "job_mapped", "step_001")
  backend_step <- file.path(backend_home, "artifacts", "job_mapped", "step_001")
  expect_equal(readLines(file.path(local_step, "submit_script.txt"), warn = FALSE),
    file.path(backend_step, "run_step.sh"))
  expect_equal(readLines(file.path(local_step, "local_script.txt"), warn = FALSE),
    file.path(local_step, "run_step.sh"))

  dsJobs:::.worker_reap(db)
  job <- dsJobs:::.store_get_job(db, "job_mapped")
  expect_equal(job$state, "FINISHED")
  expect_true(file.exists(file.path(local_step, "output", "mapped.txt")))
})

test_that("container runners use container command and backend paths", {
  home <- setup_test_home()
  writeLines(c(
    "name: containerized",
    "plane: artifact",
    "command: /missing/on/backend",
    "args_template: ['--would-fail']",
    "container:",
    "  image: alpine:latest",
    "  runtime: docker",
    "  pull: never",
    "  command: /bin/sh",
    "  args_template:",
    "    - -c",
    "    - mkdir -p {output_dir}; echo from_container > {output_dir}/container.txt",
    "resources:",
    "  memory_mb: 64",
    "  cpu_slots: 1"
  ), file.path(home, "runners", "containerized.yml"))
  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.executor_backend = "external",
    dsjobs.container_runtime = "docker",
    dsjobs.backend_path_mappings = c(stats::setNames("/hpc/dsjobs", home))
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  step <- list(type = "run", plane = "artifact", runner = "containerized",
    config = list())
  local_step <- file.path(home, "artifacts", "job_container", "step_001")
  dir.create(file.path(local_step, "output"), recursive = TRUE)
  prepared <- dsJobs:::.prepare_artifact_command(db, "job_container", 1L,
    step, local_step, NULL)
  prepared$script_path <- file.path(local_step, "run_step.sh")
  prepared <- dsJobs:::.backend_map_prepared(prepared)
  dsJobs:::.backend_write_step_script(file.path(local_step, "run_step.sh"), prepared)
  script <- readLines(file.path(local_step, "run_step.sh"), warn = FALSE)

  expect_true(any(grepl("docker.*run", script)))
  expect_true(any(grepl("alpine:latest", script, fixed = TRUE)))
  expect_true(any(grepl("/bin/sh", script, fixed = TRUE)))
  expect_false(any(grepl("/missing/on/backend", script, fixed = TRUE)))
  expect_true(any(grepl("/hpc/dsjobs", script, fixed = TRUE)))
})

test_that("external docker container runner can execute without backend deps", {
  if (!identical(Sys.getenv("DSJOBS_RUN_DOCKER_TESTS", unset = ""), "1"))
    skip("set DSJOBS_RUN_DOCKER_TESTS=1 to run Docker integration test")
  docker <- Sys.which("docker")
  if (!nzchar(docker)) skip("docker CLI is not available")
  has_alpine <- identical(system2(docker, c("image", "inspect", "alpine:latest"),
    stdout = FALSE, stderr = FALSE), 0L)
  if (!has_alpine) skip("alpine:latest image is not available locally")

  home <- setup_test_home()
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  submit <- file.path(bin, "submit")
  status <- file.path(bin, "status")
  writeLines(c(
    "#!/bin/sh",
    "sh \"$DSJOBS_STEP_SCRIPT\" >/dev/null 2>&1 || exit $?",
    "echo ext-container-123"
  ), submit)
  writeLines(c("#!/bin/sh", "echo SUCCEEDED 0"), status)
  Sys.chmod(c(submit, status), "0755")

  writeLines(c(
    "name: container_shell",
    "plane: artifact",
    "command: /missing/on/backend",
    "args_template: ['--would-fail']",
    "container:",
    "  image: alpine:latest",
    "  runtime: docker",
    "  pull: never",
    "  command: /bin/sh",
    "  args_template:",
    "    - -c",
    "    - mkdir -p {output_dir}; echo container_ok > {output_dir}/container.txt",
    "resources:",
    "  memory_mb: 64",
    "  cpu_slots: 1"
  ), file.path(home, "runners", "container_shell.yml"))

  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.executor_backend = "external",
    dsjobs.external_submit_cmd = submit,
    dsjobs.external_status_cmd = status,
    dsjobs.container_runtime = "docker",
    dsjobs.container_pull = "never",
    dsjobs.max_retries = 0
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "container_shell", config = list())))
  dsJobs:::.store_create_job(db, "job_container_external", "user", spec, 1L)
  dsJobs:::.store_update_job(db, "job_container_external",
    state = "RUNNING", step_index = 1L)
  dsJobs:::.executor_run_step(db, "job_container_external", 1L, spec)
  dsJobs:::.worker_reap(db)

  job <- dsJobs:::.store_get_job(db, "job_container_external")
  expect_equal(job$state, "FINISHED")
  out <- file.path(home, "artifacts", "job_container_external",
    "step_001", "output", "container.txt")
  expect_equal(readLines(out, warn = FALSE), "container_ok")
})

test_that("slurm backend submits with runner resources and reaps completion", {
  home <- setup_test_home()
  backend_home <- file.path(tempdir(), paste0("dsjobs_slurm_backend_view_", Sys.getpid()))
  if (!file.symlink(home, backend_home))
    skip("filesystem does not support symlinks for slurm path mapping test")
  on.exit(unlink(backend_home, recursive = TRUE), add = TRUE)
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
    dsjobs.backend_path_mappings = c(stats::setNames(backend_home, home)),
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
  expect_true(paste0("--chdir=", file.path(backend_home, "artifacts",
    "job_slurm", "step_001")) %in% args)
  expect_equal(args[length(args)], file.path(backend_home, "artifacts",
    "job_slurm", "step_001", "run_step.sh"))

  dsJobs:::.worker_reap(db)
  job <- dsJobs:::.store_get_job(db, "job_slurm")
  expect_equal(job$state, "FINISHED")
})

test_that("optional backend GPUs are requested independently of Rock GPUs", {
  home <- setup_test_home()
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  sbatch <- file.path(bin, "sbatch")
  args_file <- file.path(home, "sbatch_gpu_args.txt")
  withr::local_envvar(c(DSJOBS_FAKE_SLURM_ARGS = args_file))
  writeLines(c(
    "#!/bin/sh",
    "printf '%s\\n' \"$@\" > \"$DSJOBS_FAKE_SLURM_ARGS\"",
    "echo 54321"
  ), sbatch)
  Sys.chmod(sbatch, "0755")
  writeLines(c(
    "name: gpu_optional",
    "plane: artifact",
    "command: /bin/sh",
    "args_template: ['-c', 'true']",
    "resources:",
    "  memory_mb: 64",
    "  cpu_slots: 1",
    "  optional_gpus: 1"
  ), file.path(home, "runners", "gpu_optional.yml"))

  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.executor_backend = "slurm",
    dsjobs.slurm_sbatch = sbatch,
    dsjobs.node_memory_mb = 1024,
    dsjobs.memory_reserve_mb = 0,
    dsjobs.cpu_slots = 1,
    dsjobs.gpu_count = 0,
    dsjobs.backend_gpu_count = 1,
    dsjobs.backend_request_optional_gpus = "auto"
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "gpu_optional", config = list())))
  decision <- dsJobs:::.scheduler_can_start_job(db, "job_gpu_backend", spec)
  expect_true(decision$ok)
  dsJobs:::.store_create_job(db, "job_gpu_backend", "user", spec, 1L)
  dsJobs:::.store_update_job(db, "job_gpu_backend", state = "RUNNING", step_index = 1L)
  dsJobs:::.executor_run_step(db, "job_gpu_backend", 1L, spec)
  args <- readLines(args_file, warn = FALSE)
  expect_true("--gres=gpu:1" %in% args)
})

test_that("slurm capabilities auto-detect backend GPUs for optional requests", {
  home <- setup_test_home()
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  sbatch <- file.path(bin, "sbatch")
  sinfo <- file.path(bin, "sinfo")
  args_file <- file.path(home, "sbatch_auto_gpu_args.txt")
  withr::local_envvar(c(DSJOBS_FAKE_SLURM_ARGS = args_file))
  writeLines(c(
    "#!/bin/sh",
    "printf '%s\\n' \"$@\" > \"$DSJOBS_FAKE_SLURM_ARGS\"",
    "echo 98765"
  ), sbatch)
  writeLines(c("#!/bin/sh", "echo gpu:a100:4"), sinfo)
  Sys.chmod(c(sbatch, sinfo), "0755")
  writeLines(c(
    "name: gpu_optional_auto",
    "plane: artifact",
    "command: /bin/sh",
    "args_template: ['-c', 'true']",
    "resources:",
    "  memory_mb: 64",
    "  cpu_slots: 1",
    "  optional_gpus: 1"
  ), file.path(home, "runners", "gpu_optional_auto.yml"))

  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.executor_backend = "slurm",
    dsjobs.slurm_sbatch = sbatch,
    dsjobs.slurm_sinfo = sinfo,
    dsjobs.node_memory_mb = 1024,
    dsjobs.memory_reserve_mb = 0,
    dsjobs.cpu_slots = 1,
    dsjobs.gpu_count = 0,
    dsjobs.backend_gpu_count = "auto",
    dsjobs.backend_request_optional_gpus = "auto",
    dsjobs.backend_capabilities_ttl_secs = 0
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "gpu_optional_auto", config = list())))
  decision <- dsJobs:::.scheduler_can_start_job(db, "job_gpu_auto", spec)
  expect_true(decision$ok)
  dsJobs:::.store_create_job(db, "job_gpu_auto", "user", spec, 1L)
  dsJobs:::.store_update_job(db, "job_gpu_auto", state = "RUNNING", step_index = 1L)
  dsJobs:::.executor_run_step(db, "job_gpu_auto", 1L, spec)
  args <- readLines(args_file, warn = FALSE)
  expect_true("--gres=gpu:1" %in% args)
  expect_equal(dsJobs:::.executor_backend_status()$capabilities$gpus, 4L)
})

test_that("external capabilities command drives optional GPU requests", {
  home <- setup_test_home()
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  submit <- file.path(bin, "submit")
  status <- file.path(bin, "status")
  capabilities <- file.path(bin, "capabilities")
  writeLines(c(
    "#!/bin/sh",
    "printf '%s\\n' \"$DSJOBS_GPUS_REQUESTED\" > \"$DSJOBS_LOCAL_STEP_DIR/gpus_requested.txt\"",
    "printf '%s\\n' \"$DSJOBS_BACKEND_GPU_SOURCE\" > \"$DSJOBS_LOCAL_STEP_DIR/gpu_source.txt\"",
    "sh \"$DSJOBS_STEP_SCRIPT\" >/dev/null 2>&1",
    "echo ext-gpu-auto"
  ), submit)
  writeLines(c("#!/bin/sh", "echo SUCCEEDED 0"), status)
  writeLines(c("#!/bin/sh", "printf '{\"gpus\":2}\\n'"), capabilities)
  Sys.chmod(c(submit, status, capabilities), "0755")

  writeLines(c(
    "name: external_gpu_optional",
    "plane: artifact",
    "command: /bin/sh",
    "args_template: ['-c', 'mkdir -p {output_dir}; echo ok > {output_dir}/ok.txt']",
    "resources:",
    "  memory_mb: 64",
    "  cpu_slots: 1",
    "  optional_gpus: 1"
  ), file.path(home, "runners", "external_gpu_optional.yml"))

  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.executor_backend = "external",
    dsjobs.external_submit_cmd = submit,
    dsjobs.external_status_cmd = status,
    dsjobs.backend_capabilities_cmd = capabilities,
    dsjobs.backend_capabilities_ttl_secs = 0,
    dsjobs.gpu_count = 0,
    dsjobs.backend_gpu_count = "auto",
    dsjobs.backend_request_optional_gpus = "auto",
    dsjobs.max_retries = 0
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "external_gpu_optional", config = list())))
  dsJobs:::.store_create_job(db, "job_external_gpu", "user", spec, 1L)
  dsJobs:::.store_update_job(db, "job_external_gpu", state = "RUNNING", step_index = 1L)
  dsJobs:::.executor_run_step(db, "job_external_gpu", 1L, spec)

  step_dir <- file.path(home, "artifacts", "job_external_gpu", "step_001")
  expect_equal(readLines(file.path(step_dir, "gpus_requested.txt"), warn = FALSE), "1")
  expect_equal(readLines(file.path(step_dir, "gpu_source.txt"), warn = FALSE),
    "external_capabilities_cmd")
  expect_equal(dsJobs:::.executor_backend_status()$capabilities$gpus, 2L)
})

options(
  dsjobs.home = NULL,
  dsjobs.executor_backend = NULL,
  dsjobs.slurm_sbatch = NULL,
  dsjobs.slurm_squeue = NULL,
  dsjobs.slurm_sacct = NULL,
  dsjobs.slurm_sinfo = NULL,
  dsjobs.external_submit_cmd = NULL,
  dsjobs.external_status_cmd = NULL,
  dsjobs.backend_capabilities_cmd = NULL,
  dsjobs.backend_capabilities_ttl_secs = NULL,
  dsjobs.external_enforce_runner_concurrency = NULL)
options(dsjobs.backend_path_mappings = NULL,
        dsjobs.container_runtime = NULL,
        dsjobs.container_pull = NULL,
        dsjobs.backend_gpu_count = NULL,
        dsjobs.backend_request_optional_gpus = NULL)
