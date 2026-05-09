test_that("external backend can delegate to a dockerized HPC unit", {
  if (!identical(Sys.getenv("DSHPC_RUN_DOCKER_TESTS", unset = ""), "1"))
    skip("set DSHPC_RUN_DOCKER_TESTS=1 to run Docker integration tests")

  docker <- Sys.which("docker")
  if (!nzchar(docker)) skip("docker CLI is not available")
  has_alpine <- identical(system2(docker, c("image", "inspect", "alpine:latest"),
    stdout = FALSE, stderr = FALSE), 0L)
  if (!has_alpine) skip("alpine:latest image is not available locally")

  home <- tempfile("dshpc-docker-hpc-", tmpdir = normalizePath(getwd()))
  dir.create(file.path(home, "runners"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(home, "artifacts"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(home, "bin"), recursive = TRUE, showWarnings = FALSE)
  backend_home <- "/hpc/dshpc"
  container <- paste0("dshpc-hpc-unit-smoke-", Sys.getpid())

  on.exit({
    system2(docker, c("rm", "-f", container), stdout = FALSE, stderr = FALSE)
    cleanup_test_home(home)
  }, add = TRUE)

  started <- system2(docker, c(
    "run", "-d", "--name", container,
    "-v", paste0(home, ":", backend_home),
    "alpine:latest", "sleep", "300"),
    stdout = TRUE, stderr = TRUE)
  expect_true(is.null(attr(started, "status")) || identical(as.integer(attr(started, "status")), 0L))

  submit <- file.path(home, "bin", "submit")
  status <- file.path(home, "bin", "status")
  cancel <- file.path(home, "bin", "cancel")
  capabilities <- file.path(home, "bin", "capabilities")

  writeLines(c(
    "#!/bin/sh",
    "set -eu",
    "id=\"${DSHPC_JOB_ID}-${DSHPC_STEP_INDEX}\"",
    "printf \"%s\\n\" \"$DSHPC_STEP_SCRIPT\" > \"$DSHPC_LOCAL_STEP_DIR/backend_script.txt\"",
    "printf \"%s\\n\" \"$DSHPC_LOCAL_STEP_SCRIPT\" > \"$DSHPC_LOCAL_STEP_DIR/local_script.txt\"",
    "printf \"%s\\n\" \"${DSHPC_GPUS_REQUESTED:-0}\" > \"$DSHPC_LOCAL_STEP_DIR/gpu_requested.txt\"",
    "docker exec -d \"$DSHPC_DOCKER_CONTAINER\" /bin/sh -c \"cd \\\"$DSHPC_STEP_DIR\\\" && /bin/sh \\\"$DSHPC_STEP_SCRIPT\\\" > hpc.stdout 2> hpc.stderr\" >/dev/null",
    "printf \"%s\\n\" \"$id\""
  ), submit)
  writeLines(c(
    "#!/bin/sh",
    "set -eu",
    "if docker exec \"$DSHPC_DOCKER_CONTAINER\" test -f \"$DSHPC_STEP_DIR/exit_code\" >/dev/null 2>&1; then",
    "  code=$(docker exec \"$DSHPC_DOCKER_CONTAINER\" cat \"$DSHPC_STEP_DIR/exit_code\")",
    "  if [ \"$code\" = \"0\" ]; then echo \"SUCCEEDED 0\"; else echo \"FAILED $code\"; fi",
    "else",
    "  echo RUNNING",
    "fi"
  ), status)
  writeLines(c(
    "#!/bin/sh",
    "set -eu",
    "echo \"cancel requested for ${DSHPC_EXTERNAL_ID:-$1}\" >> \"${DSHPC_LOCAL_STEP_DIR:-/tmp}/cancel.log\" 2>/dev/null || true",
    "exit 0"
  ), cancel)
  writeLines(c(
    "#!/bin/sh",
    "cat <<EOF",
    "{\"available\":true,\"resources\":{\"gpus\":1,\"gpu_memory_mb\":4096}}",
    "EOF"
  ), capabilities)
  Sys.chmod(c(submit, status, cancel, capabilities), "0755")

  writeLines(c(
    "name: hpc_unit_echo",
    "plane: artifact",
    "command: /bin/sh",
    "args_template:",
    "  - -c",
    "  - >",
    "    mkdir -p {output_dir};",
    "    printf \"sample=%s\\n\" \"{sample_id}\" > {output_dir}/result.txt;",
    "    printf \"message=%s\\n\" \"{message}\" >> {output_dir}/result.txt;",
    "    printf \"output_dir=%s\\n\" \"$DSHPC_OUTPUT_DIR\" >> {output_dir}/result.txt",
    "resources:",
    "  memory_mb: 64",
    "  cpu_slots: 1",
    "  optional_gpus: 1"
  ), file.path(home, "runners", "hpc_unit_echo.yml"))

  withr::local_envvar(c(DSHPC_DOCKER_CONTAINER = container))
  withr::local_options(list(
    dshpc.home = home,
    dshpc.executor_backend = "external",
    dshpc.external_submit_cmd = submit,
    dshpc.external_status_cmd = status,
    dshpc.external_cancel_cmd = cancel,
    dshpc.backend_capabilities_cmd = capabilities,
    dshpc.backend_capabilities_ttl_secs = 0,
    dshpc.backend_path_mappings = stats::setNames(backend_home, home),
    dshpc.max_retries = 0,
    dshpc.scheduler_scan_limit = 10,
    dshpc.worker_poll_secs = 1
  ))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  spec <- list(
    label = "docker-hpc-smoke",
    tags = c("docker-hpc-smoke"),
    resource_class = "default",
    steps = list(list(
      type = "run", plane = "artifact", runner = "hpc_unit_echo",
      config = list(sample_id = "case001", message = "hello-from-rock")
    ))
  )
  job_id <- "job_docker_hpc_smoke"
  dsHPC:::.store_create_job(db, job_id, "smoke-user", spec, length(spec$steps))
  dsHPC:::.worker_dispatch(db)

  for (i in seq_len(30)) {
    dsHPC:::.worker_reap(db)
    job <- dsHPC:::.store_get_job(db, job_id)
    if (job$state %in% c("FINISHED", "FAILED", "CANCELLED")) break
    Sys.sleep(1)
  }

  job <- dsHPC:::.store_get_job(db, job_id)
  step_dir <- file.path(home, "artifacts", job_id, "step_001")
  result_file <- file.path(step_dir, "output", "result.txt")

  expect_equal(job$state, "FINISHED")
  expect_match(readLines(file.path(step_dir, "backend_script.txt"), warn = FALSE),
    paste0("^", backend_home))
  expect_match(readLines(file.path(step_dir, "local_script.txt"), warn = FALSE),
    paste0("^", home))
  expect_equal(readLines(file.path(step_dir, "gpu_requested.txt"), warn = FALSE), "1")
  expect_true(file.exists(result_file))
  expect_true("sample=case001" %in% readLines(result_file, warn = FALSE))
  expect_true("message=hello-from-rock" %in% readLines(result_file, warn = FALSE))
})
