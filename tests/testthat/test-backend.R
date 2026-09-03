test_that("executor backend status reports embedded and missing slurm", {
  withr::local_options(list(dshpc.executor_backend = "embedded"))
  embedded <- dsHPC:::.executor_backend_status()
  expect_equal(embedded$backend, "embedded")
  expect_true(embedded$available)
  expect_false(embedded$delegates_resources)

  withr::local_options(list(
    dshpc.executor_backend = "slurm",
    dshpc.slurm_sbatch = "/definitely/not/sbatch"))
  slurm <- dsHPC:::.executor_backend_status()
  expect_equal(slurm$backend, "slurm")
  expect_false(slurm$available)
  expect_equal(slurm$reason, "sbatch_not_found")

  withr::local_options(list(
    dshpc.executor_backend = "kubernetes",
    dshpc.kubernetes_kubectl = "/definitely/not/kubectl"))
  k8s <- dsHPC:::.executor_backend_status()
  expect_equal(k8s$backend, "kubernetes")
  expect_false(k8s$available)
  expect_equal(k8s$reason, "kubectl_not_found")
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
    dshpc.home = home,
    dshpc.executor_backend = "slurm",
    dshpc.node_memory_mb = 4096,
    dshpc.memory_reserve_mb = 0,
    dshpc.cpu_slots = 1
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "heavy", config = list())))

  d1 <- dsHPC:::.scheduler_can_start_job(db, "job_ext_a", spec)
  expect_true(d1$ok)
  expect_lt(d1$budget$memory_mb, d1$plan$memory_mb)

  dsHPC:::.store_create_job(db, "job_ext_running", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_ext_running", state = "RUNNING")
  d2 <- dsHPC:::.scheduler_can_start_job(db, "job_ext_b", spec)
  expect_true(d2$ok)

  withr::local_options(list(dshpc.external_enforce_runner_concurrency = TRUE))
  d3 <- dsHPC:::.scheduler_can_start_job(db, "job_ext_c", spec)
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
    "sh \"$DSHPC_STEP_SCRIPT\" >/dev/null 2>&1",
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
    dshpc.home = home,
    dshpc.executor_backend = "external",
    dshpc.external_submit_cmd = submit,
    dshpc.external_status_cmd = status,
    dshpc.max_retries = 0
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "shell_ok", config = list())))
  dsHPC:::.store_create_job(db, "job_external", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_external", state = "RUNNING", step_index = 1L)
  dsHPC:::.executor_run_step(db, "job_external", 1L, spec)

  step <- DBI::dbGetQuery(db,
    "SELECT external_backend, external_id FROM steps WHERE job_id = ?",
    params = list("job_external"))
  expect_equal(step$external_backend, "external")
  expect_equal(step$external_id, "ext-123")
  marker <- dsHPC:::.backend_read_external_marker(file.path(
    home, "artifacts", "job_external", "step_001"))
  expect_equal(marker$backend, "external")
  expect_equal(marker$external_id, "ext-123")

  # Simulate a worker crash after backend submission but before the DB row is
  # fully durable. The recovery marker lets the next worker resume status sync.
  dsHPC:::.store_update_step(db, "job_external", 1L,
    external_backend = NA_character_,
    external_id = NA_character_)

  dsHPC:::.worker_reap(db)
  job <- dsHPC:::.store_get_job(db, "job_external")
  expect_equal(job$state, "FINISHED")
  outputs <- DBI::dbGetQuery(db,
    "SELECT name FROM outputs WHERE job_id = ?",
    params = list("job_external"))
  expect_true("ok.txt" %in% outputs$name)
})

test_that("external submit accepts only one bounded safe stdout id", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  submit <- file.path(home, "submit")
  step_dir <- file.path(home, "artifacts", "job_submit_contract", "step_001")
  dir.create(step_dir, recursive = TRUE)
  prepared <- list(
    env_vars = character(0),
    command = "/bin/sh",
    args = c("-c", "true"),
    step_dir = step_dir,
    output_dir = file.path(step_dir, "output"),
    script_path = file.path(step_dir, "run_step.sh"),
    local_step_dir = step_dir,
    local_output_dir = file.path(step_dir, "output"),
    local_script_path = file.path(step_dir, "run_step.sh"),
    runner_config = list())
  step <- list(runner = NULL)
  withr::local_options(list(dshpc.home = home,
    dshpc.external_submit_cmd = submit))

  run_submit <- function(body) {
    writeLines(c("#!/bin/sh", body), submit)
    Sys.chmod(submit, "0755")
    dsHPC:::.backend_submit_external("job_submit_contract", 1L, step,
      step_dir, prepared)
  }

  expect_equal(run_submit(c("echo gateway-warning >&2", "echo ext.Valid:123")),
    "ext.Valid:123")
  expect_error(run_submit(c("echo ext-1", "echo ext-2")), "valid job id")
  expect_error(run_submit("printf ' ext-1\\n'"), "valid job id")
  expect_error(run_submit("echo 'ext-1;touch'"), "valid job id")
  expect_error(run_submit(paste0("echo ", paste(rep("a", 257), collapse = ""))),
    "valid job id")
})

test_that("external status and recovery validate their backend id contracts", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  status <- file.path(home, "status")
  cancel <- file.path(home, "cancel")
  called <- file.path(home, "called")
  withr::local_envvar(c(DSHPC_EXTERNAL_CONTRACT_CALLED = called))
  writeLines(c(
    "#!/bin/sh",
    "echo status-warning >&2",
    "echo RUNNING"
  ), status)
  writeLines(c(
    "#!/bin/sh",
    "touch \"$DSHPC_EXTERNAL_CONTRACT_CALLED\""
  ), cancel)
  Sys.chmod(c(status, cancel), "0755")
  withr::local_options(list(
    dshpc.home = home,
    dshpc.external_status_cmd = status,
    dshpc.external_cancel_cmd = cancel
  ))

  state <- dsHPC:::.backend_status_external("ext-123", home)
  expect_equal(state$state, "running")
  expect_equal(state$external_state, "RUNNING")

  writeLines(c("#!/bin/sh", "echo RUNNING", "echo FAILED 1"), status)
  Sys.chmod(status, "0755")
  state <- dsHPC:::.backend_status_external("ext-123", home)
  expect_equal(state$external_state, "STATUS_UNKNOWN")
  expect_equal(state$reason, "external_status_invalid")

  marker_dir <- file.path(home, "artifacts", "job_marker", "step_001")
  dir.create(marker_dir, recursive = TRUE)
  dsHPC:::.backend_write_external_marker(marker_dir, "external", "bad;touch")
  expect_null(dsHPC:::.backend_read_external_marker(marker_dir))
  writeLines('{"backend":"external","external_id":["ext-1","ext-2"]}',
    file.path(marker_dir, "external_backend.json"))
  expect_null(dsHPC:::.backend_read_external_marker(marker_dir))

  writeLines(c(
    "#!/bin/sh",
    "touch \"$DSHPC_EXTERNAL_CONTRACT_CALLED\"",
    "echo RUNNING"
  ), status)
  Sys.chmod(status, "0755")
  unlink(called)
  invalid <- dsHPC:::.backend_status_external("bad;touch", home)
  expect_equal(invalid$external_state, "STATUS_UNKNOWN")
  expect_equal(invalid$reason, "invalid_external_id")
  expect_false(dsHPC:::.backend_cancel_step("external", "bad;touch"))
  expect_false(file.exists(called))
})

test_that("kubernetes backend submits job manifest and reaps completion", {
  home <- setup_test_home()
  backend_home <- file.path(tempdir(), paste0("dshpc_k8s_backend_view_",
    Sys.getpid()))
  if (!file.symlink(home, backend_home))
    skip("filesystem does not support symlinks for Kubernetes path mapping test")
  on.exit(unlink(backend_home, recursive = TRUE), add = TRUE)

  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  kubectl <- file.path(bin, "kubectl")
  args_file <- file.path(home, "kubectl_args.txt")
  manifest_file <- file.path(home, "kubernetes_manifest.json")
  status_file <- file.path(home, "kubernetes_status.json")
  delete_file <- file.path(home, "kubectl_delete_args.txt")
  withr::local_envvar(c(
    DSHPC_FAKE_KUBECTL_ARGS = args_file,
    DSHPC_FAKE_KUBECTL_MANIFEST = manifest_file,
    DSHPC_FAKE_KUBECTL_STATUS = status_file,
    DSHPC_FAKE_KUBECTL_DELETE = delete_file))
  writeLines(c(
    "#!/bin/sh",
    "printf '%s\\n' \"$@\" >> \"$DSHPC_FAKE_KUBECTL_ARGS\"",
    "action=''",
    "for arg in \"$@\"; do",
    "  case \"$arg\" in",
    "    apply|get|delete) action=\"$arg\"; break ;;",
    "  esac",
    "done",
    "case \"$action\" in",
    "  apply)",
    "    cat > \"$DSHPC_FAKE_KUBECTL_MANIFEST\"",
    "    name=$(grep -m 1 '\"name\"[[:space:]]*:' \"$DSHPC_FAKE_KUBECTL_MANIFEST\" | sed 's/.*\"name\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/')",
    "    [ -n \"$name\" ] || name=dshpc-fake",
    "    printf 'job.batch/%s created\\n' \"$name\"",
    "    ;;",
    "  get)",
    "    cat \"$DSHPC_FAKE_KUBECTL_STATUS\"",
    "    ;;",
    "  delete)",
    "    printf '%s\\n' \"$@\" > \"$DSHPC_FAKE_KUBECTL_DELETE\"",
    "    ;;",
    "  *)",
    "    echo unsupported fake kubectl action >&2",
    "    exit 2",
    "    ;;",
    "esac"
  ), kubectl)
  Sys.chmod(kubectl, "0755")
  writeLines('{"status":{"active":1}}', status_file)

  writeLines(c(
    "name: shell_k8s",
    "plane: artifact",
    "command: /bin/sh",
    "args_template: ['-c', 'false']",
    "container:",
    "  image: rocker/r-base:4.4",
    "  command: /bin/sh",
    "  args_template:",
    "    - -c",
    "    - mkdir -p {output_dir}; echo ok > {output_dir}/ok.txt",
    "resources:",
    "  memory_mb: 128",
    "  cpu_slots: 2"
  ), file.path(home, "runners", "shell_k8s.yml"))

  withr::local_options(list(
    dshpc.home = home,
    dshpc.executor_backend = "kubernetes",
    dshpc.kubernetes_kubectl = kubectl,
    dshpc.kubernetes_context = "kind-dshpc",
    dshpc.kubernetes_namespace = "dshpc",
    dshpc.kubernetes_pvc = "dshpc-pvc",
    dshpc.kubernetes_mount_path = backend_home,
    dshpc.backend_path_mappings = c(stats::setNames(backend_home, home)),
    dshpc.max_retries = 0
  ))
  on.exit(cleanup_test_home(home), add = TRUE)

  status <- dsHPC:::.executor_backend_status()
  expect_true(status$available)
  expect_equal(status$reason, "ok")
  expect_equal(status$kubernetes$pvc, "dshpc-pvc")

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "shell_k8s", config = list())))
  dsHPC:::.store_create_job(db, "job_k8s", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_k8s", state = "RUNNING", step_index = 1L)
  dsHPC:::.executor_run_step(db, "job_k8s", 1L, spec)

  step <- DBI::dbGetQuery(db,
    "SELECT external_backend, external_id FROM steps WHERE job_id = ?",
    params = list("job_k8s"))
  expect_equal(step$external_backend, "kubernetes")
  expect_match(step$external_id, "^dshpc-job-k8s-s001-")

  manifest <- jsonlite::fromJSON(readLines(manifest_file, warn = FALSE),
    simplifyVector = FALSE)
  expect_equal(manifest$kind, "Job")
  expect_equal(manifest$metadata$name, step$external_id)
  pod <- manifest$spec$template$spec
  container <- pod$containers[[1]]
  expect_equal(container$image, "rocker/r-base:4.4")
  expect_equal(container$workingDir, file.path(backend_home, "artifacts",
    "job_k8s", "step_001"))
  expect_equal(pod$volumes[[1]]$persistentVolumeClaim$claimName, "dshpc-pvc")
  expect_equal(container$volumeMounts[[1]]$mountPath, backend_home)
  expect_equal(container$resources$requests$cpu, "2")
  expect_equal(container$resources$requests$memory, "128Mi")
  expect_true(any(vapply(container$env, function(x) {
    identical(x$name, "DSHPC_JOB_ID") && identical(x$value, "job_k8s")
  }, logical(1))))
  kubectl_args <- readLines(args_file, warn = FALSE)
  expect_true("--context" %in% kubectl_args)
  expect_true("kind-dshpc" %in% kubectl_args)
  expect_true("--namespace" %in% kubectl_args)
  expect_true("dshpc" %in% kubectl_args)

  local_step <- file.path(home, "artifacts", "job_k8s", "step_001")
  dir.create(file.path(local_step, "output"), recursive = TRUE,
    showWarnings = FALSE)
  writeLines("ok", file.path(local_step, "output", "ok.txt"))
  writeLines("0", file.path(local_step, "exit_code"))
  writeLines('{"status":{"conditions":[{"type":"Complete","status":"True"}],"succeeded":1}}',
    status_file)

  dsHPC:::.worker_reap(db)
  job <- dsHPC:::.store_get_job(db, "job_k8s")
  expect_equal(job$state, "FINISHED")
  outputs <- DBI::dbGetQuery(db,
    "SELECT name FROM outputs WHERE job_id = ?",
    params = list("job_k8s"))
  expect_true("ok.txt" %in% outputs$name)

  dsHPC:::.backend_cancel_step("kubernetes", step$external_id)
  delete_args <- readLines(delete_file, warn = FALSE)
  expect_true("delete" %in% delete_args)
  expect_true("job" %in% delete_args)
  expect_true(step$external_id %in% delete_args)
})

test_that("external status command failures do not create duplicate retries", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  status <- file.path(home, "status")
  writeLines(c("#!/bin/sh", "echo temporary scheduler outage >&2", "exit 2"),
    status)
  Sys.chmod(status, "0755")

  withr::local_options(list(
    dshpc.executor_backend = "external",
    dshpc.external_status_cmd = status
  ))

  state <- dsHPC:::.backend_status_external("ext-unknown",
    file.path(home, "artifacts", "job_x", "step_001"))
  expect_equal(state$state, "running")
  expect_equal(state$external_state, "STATUS_UNKNOWN")
  expect_true(is.na(state$exit_code))

  withr::local_options(list(dshpc.external_status_cmd = ""))
  missing <- dsHPC:::.backend_status_external("ext-unknown",
    file.path(home, "artifacts", "job_x", "step_001"))
  expect_equal(missing$state, "running")
  expect_equal(missing$external_state, "STATUS_UNKNOWN")
})

test_that("backend step scripts write exit_code atomically", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  script <- file.path(home, "artifacts", "run_step.sh")
  prepared <- list(
    env_vars = c(DSHPC_OUTPUT_DIR = file.path(home, "out")),
    command = "/bin/sh",
    args = c("-c", "true"),
    step_dir = dirname(script),
    output_dir = file.path(home, "out"),
    runner_config = list())
  dsHPC:::.backend_write_step_script(script, prepared)
  lines <- readLines(script, warn = FALSE)
  expect_identical(lines[[1L]], "#!/bin/sh")
  expect_true(any(grepl("exit_code.tmp", lines, fixed = TRUE)))
  expect_true(any(grepl("mv exit_code.tmp exit_code", lines, fixed = TRUE)))
  expect_true(any(grepl("/usr/bin/env -i", lines, fixed = TRUE)))
  expect_true(any(grepl("/bin/sh \"$0\" \"$@\"", lines, fixed = TRUE)))
  expect_false(any(grepl("bash", lines, fixed = TRUE)))
})

test_that("backend runner scripts clear the submitting environment", {
  skip_on_os("windows")
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  env_name <- "DSHPC_BACKEND_PARENT_SECRET"
  old_env <- Sys.getenv(env_name, unset = NA_character_)
  on.exit({
    Sys.unsetenv(env_name)
    if (!is.na(old_env)) Sys.setenv(DSHPC_BACKEND_PARENT_SECRET = old_env)
  }, add = TRUE)
  Sys.setenv(DSHPC_BACKEND_PARENT_SECRET = "backend-secret-marker")
  on.exit(cleanup_test_home(home), add = TRUE)

  step_dir <- file.path(home, "artifacts", "job_backend", "step_001")
  dir.create(step_dir, recursive = TRUE)
  script <- file.path(step_dir, "run_step.sh")
  prepared <- list(
    env_vars = c(PATH = Sys.getenv("PATH"), HOME = file.path(step_dir, "home")),
    command = Sys.which("env"), args = character(0), step_dir = step_dir,
    output_dir = file.path(step_dir, "output"), runner_config = list())
  dsHPC:::.backend_write_step_script(script, prepared)
  expect_false(any(grepl("backend-secret-marker",
    readLines(script, warn = FALSE), fixed = TRUE)))
  output <- system2("/bin/sh", script, stdout = TRUE, stderr = TRUE)

  expect_false(any(grepl("backend-secret-marker", output, fixed = TRUE)))
  expect_false(any(grepl("DSHPC_BACKEND_PARENT_SECRET", output,
    fixed = TRUE)))
  expect_true(any(grepl(paste0("HOME=", prepared$env_vars[["HOME"]]), output,
    fixed = TRUE)))
})

test_that("backend path mappings support alternate host/container views", {
  withr::local_options(list(dshpc.backend_path_mappings = c(
    "/srv/dshpc" = "/host/dshpc",
    "/srv/dshpc/artifacts/special" = "/fast/artifacts/special"
  )))
  maps <- dsHPC:::.backend_path_mappings()
  expect_equal(maps$local[1], "/srv/dshpc/artifacts/special")
  expect_equal(dsHPC:::.backend_map_path(
    "/srv/dshpc/artifacts/job_x", "local_to_backend"), "/host/dshpc/artifacts/job_x")
  expect_equal(dsHPC:::.backend_map_path(
    "/host/dshpc/artifacts/job_x", "backend_to_local"), "/srv/dshpc/artifacts/job_x")
  expect_equal(dsHPC:::.backend_map_text(
    "write /srv/dshpc/artifacts/job_x/out", "local_to_backend"),
    "write /host/dshpc/artifacts/job_x/out")
})

test_that("external backend can execute through a mapped backend path", {
  home <- setup_test_home()
  backend_home <- file.path(tempdir(), paste0("dshpc_backend_view_", Sys.getpid()))
  if (!file.symlink(home, backend_home))
    skip("filesystem does not support symlinks for backend path mapping test")
  on.exit(unlink(backend_home, recursive = TRUE), add = TRUE)

  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  submit <- file.path(bin, "submit")
  status <- file.path(bin, "status")
  writeLines(c(
    "#!/bin/sh",
    "printf '%s\\n' \"$DSHPC_STEP_SCRIPT\" > \"$DSHPC_LOCAL_STEP_DIR/submit_script.txt\"",
    "printf '%s\\n' \"$DSHPC_LOCAL_STEP_SCRIPT\" > \"$DSHPC_LOCAL_STEP_DIR/local_script.txt\"",
    "sh \"$DSHPC_STEP_SCRIPT\" >/dev/null 2>&1",
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
    dshpc.home = home,
    dshpc.executor_backend = "external",
    dshpc.external_submit_cmd = submit,
    dshpc.external_status_cmd = status,
    dshpc.backend_path_mappings = c(stats::setNames(backend_home, home)),
    dshpc.max_retries = 0
  ))
  on.exit(cleanup_test_home(home), add = TRUE)

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "shell_map", config = list())))
  dsHPC:::.store_create_job(db, "job_mapped", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_mapped", state = "RUNNING", step_index = 1L)
  dsHPC:::.executor_run_step(db, "job_mapped", 1L, spec)

  local_step <- file.path(home, "artifacts", "job_mapped", "step_001")
  backend_step <- file.path(backend_home, "artifacts", "job_mapped", "step_001")
  expect_equal(readLines(file.path(local_step, "submit_script.txt"), warn = FALSE),
    file.path(backend_step, "run_step.sh"))
  expect_equal(readLines(file.path(local_step, "local_script.txt"), warn = FALSE),
    file.path(local_step, "run_step.sh"))

  dsHPC:::.worker_reap(db)
  job <- dsHPC:::.store_get_job(db, "job_mapped")
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
    dshpc.home = home,
    dshpc.executor_backend = "external",
    dshpc.container_runtime = "docker",
    dshpc.backend_path_mappings = c(stats::setNames("/hpc/dshpc", home))
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  step <- list(type = "run", plane = "artifact", runner = "containerized",
    config = list())
  local_step <- file.path(home, "artifacts", "job_container", "step_001")
  dir.create(file.path(local_step, "output"), recursive = TRUE)
  prepared <- dsHPC:::.prepare_artifact_command(db, "job_container", 1L,
    step, local_step, NULL)
  prepared$script_path <- file.path(local_step, "run_step.sh")
  prepared <- dsHPC:::.backend_map_prepared(prepared)
  dsHPC:::.backend_write_step_script(file.path(local_step, "run_step.sh"), prepared)
  script <- readLines(file.path(local_step, "run_step.sh"), warn = FALSE)

  expect_true(any(grepl("docker.*run", script)))
  expect_true(any(grepl("alpine:latest", script, fixed = TRUE)))
  expect_true(any(grepl("/bin/sh", script, fixed = TRUE)))
  expect_false(any(grepl("/missing/on/backend", script, fixed = TRUE)))
  expect_true(any(grepl("/hpc/dshpc", script, fixed = TRUE)))
})

test_that("external docker container runner can execute without backend deps", {
  if (!identical(Sys.getenv("DSHPC_RUN_DOCKER_TESTS", unset = ""), "1"))
    skip("set DSHPC_RUN_DOCKER_TESTS=1 to run Docker integration test")
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
    "sh \"$DSHPC_STEP_SCRIPT\" >/dev/null 2>&1 || exit $?",
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
    dshpc.home = home,
    dshpc.executor_backend = "external",
    dshpc.external_submit_cmd = submit,
    dshpc.external_status_cmd = status,
    dshpc.container_runtime = "docker",
    dshpc.container_pull = "never",
    dshpc.max_retries = 0
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "container_shell", config = list())))
  dsHPC:::.store_create_job(db, "job_container_external", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_container_external",
    state = "RUNNING", step_index = 1L)
  dsHPC:::.executor_run_step(db, "job_container_external", 1L, spec)
  dsHPC:::.worker_reap(db)

  job <- dsHPC:::.store_get_job(db, "job_container_external")
  expect_equal(job$state, "FINISHED")
  out <- file.path(home, "artifacts", "job_container_external",
    "step_001", "output", "container.txt")
  expect_equal(readLines(out, warn = FALSE), "container_ok")
})

test_that("slurm backend submits with runner resources and reaps completion", {
  home <- setup_test_home()
  backend_home <- file.path(tempdir(), paste0("dshpc_slurm_backend_view_", Sys.getpid()))
  if (!file.symlink(home, backend_home))
    skip("filesystem does not support symlinks for slurm path mapping test")
  on.exit(unlink(backend_home, recursive = TRUE), add = TRUE)
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  sbatch <- file.path(bin, "sbatch")
  squeue <- file.path(bin, "squeue")
  sacct <- file.path(bin, "sacct")
  args_file <- file.path(home, "sbatch_args.txt")
  withr::local_envvar(c(DSHPC_FAKE_SLURM_ARGS = args_file))
  writeLines(c(
    "#!/bin/sh",
    "printf '%s\\n' \"$@\" > \"$DSHPC_FAKE_SLURM_ARGS\"",
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
    dshpc.home = home,
    dshpc.executor_backend = "slurm",
    dshpc.slurm_sbatch = sbatch,
    dshpc.slurm_squeue = squeue,
    dshpc.slurm_sacct = sacct,
    dshpc.slurm_partition = "gpu_part",
    dshpc.slurm_account = "proj_acct",
    dshpc.slurm_qos = "fast_qos",
    dshpc.backend_path_mappings = c(stats::setNames(backend_home, home)),
    dshpc.max_retries = 0
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "shell_slurm", config = list())))
  dsHPC:::.store_create_job(db, "job_slurm", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_slurm", state = "RUNNING", step_index = 1L)
  dsHPC:::.executor_run_step(db, "job_slurm", 1L, spec)

  args <- readLines(args_file, warn = FALSE)
  expect_true("--mem=4096" %in% args)
  expect_true("--export=NONE" %in% args)
  expect_true("--cpus-per-task=2" %in% args)
  expect_true("--partition=gpu_part" %in% args)
  expect_true("--account=proj_acct" %in% args)
  expect_true("--qos=fast_qos" %in% args)
  expect_true(paste0("--chdir=", file.path(backend_home, "artifacts",
    "job_slurm", "step_001")) %in% args)
  expect_equal(args[length(args)], file.path(backend_home, "artifacts",
    "job_slurm", "step_001", "run_step.sh"))

  dsHPC:::.worker_reap(db)
  job <- dsHPC:::.store_get_job(db, "job_slurm")
  expect_equal(job$state, "FINISHED")
})

test_that("slurm status falls back to local exit_code file when sacct is missing", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  squeue_empty <- file.path(bin, "squeue_empty")
  squeue_running <- file.path(bin, "squeue_running")
  writeLines(c("#!/bin/sh", "exit 0"), squeue_empty)
  writeLines(c("#!/bin/sh", "echo RUNNING"), squeue_running)
  Sys.chmod(c(squeue_empty, squeue_running), "0755")

  step_dir <- file.path(home, "artifacts", "job_sacctless", "step_001")
  dir.create(step_dir, recursive = TRUE, showWarnings = FALSE)

  withr::local_options(list(
    dshpc.slurm_squeue = squeue_empty,
    dshpc.slurm_sacct = "/definitely/not/sacct"
  ))

  # Job left the queue, sacct unresolvable, exit_code file says success
  writeLines("0", file.path(step_dir, "exit_code"))
  st <- dsHPC:::.backend_status_slurm("99001", step_dir)
  expect_equal(st$state, "succeeded")
  expect_equal(st$external_state, "LOCAL_EXIT_FILE")

  # Same, but the step wrapper recorded a non-zero exit code
  writeLines("3", file.path(step_dir, "exit_code"))
  st <- dsHPC:::.backend_status_slurm("99001", step_dir)
  expect_equal(st$state, "failed")
  expect_equal(st$exit_code, 3L)

  # No exit_code file at all: status is unknown, keep polling
  unlink(file.path(step_dir, "exit_code"))
  st <- dsHPC:::.backend_status_slurm("99001", step_dir)
  expect_equal(st$state, "running")
  expect_equal(st$external_state, "UNKNOWN")

  # Active squeue state short-circuits before any fallback
  withr::local_options(list(dshpc.slurm_squeue = squeue_running))
  st <- dsHPC:::.backend_status_slurm("99001", step_dir)
  expect_equal(st$state, "running")

  cancelled <- dsHPC:::.backend_parse_slurm_sacct("CANCELLED|0:0")
  expect_equal(cancelled$state, "failed")
  expect_equal(cancelled$exit_code, 1L)
})

test_that("optional backend GPUs are requested independently of Rock GPUs", {
  home <- setup_test_home()
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  sbatch <- file.path(bin, "sbatch")
  args_file <- file.path(home, "sbatch_gpu_args.txt")
  withr::local_envvar(c(DSHPC_FAKE_SLURM_ARGS = args_file))
  writeLines(c(
    "#!/bin/sh",
    "printf '%s\\n' \"$@\" > \"$DSHPC_FAKE_SLURM_ARGS\"",
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
    dshpc.home = home,
    dshpc.executor_backend = "slurm",
    dshpc.slurm_sbatch = sbatch,
    dshpc.node_memory_mb = 1024,
    dshpc.memory_reserve_mb = 0,
    dshpc.cpu_slots = 1,
    dshpc.gpu_count = 0,
    dshpc.backend_gpu_count = 1,
    dshpc.backend_request_optional_gpus = "auto"
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "gpu_optional", config = list())))
  decision <- dsHPC:::.scheduler_can_start_job(db, "job_gpu_backend", spec)
  expect_true(decision$ok)
  dsHPC:::.store_create_job(db, "job_gpu_backend", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_gpu_backend", state = "RUNNING", step_index = 1L)
  dsHPC:::.executor_run_step(db, "job_gpu_backend", 1L, spec)
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
  withr::local_envvar(c(DSHPC_FAKE_SLURM_ARGS = args_file))
  writeLines(c(
    "#!/bin/sh",
    "printf '%s\\n' \"$@\" > \"$DSHPC_FAKE_SLURM_ARGS\"",
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
    dshpc.home = home,
    dshpc.executor_backend = "slurm",
    dshpc.slurm_sbatch = sbatch,
    dshpc.slurm_sinfo = sinfo,
    dshpc.node_memory_mb = 1024,
    dshpc.memory_reserve_mb = 0,
    dshpc.cpu_slots = 1,
    dshpc.gpu_count = 0,
    dshpc.backend_gpu_count = "auto",
    dshpc.backend_request_optional_gpus = "auto",
    dshpc.backend_capabilities_ttl_secs = 0
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "gpu_optional_auto", config = list())))
  decision <- dsHPC:::.scheduler_can_start_job(db, "job_gpu_auto", spec)
  expect_true(decision$ok)
  dsHPC:::.store_create_job(db, "job_gpu_auto", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_gpu_auto", state = "RUNNING", step_index = 1L)
  dsHPC:::.executor_run_step(db, "job_gpu_auto", 1L, spec)
  args <- readLines(args_file, warn = FALSE)
  expect_true("--gres=gpu:1" %in% args)
  expect_equal(dsHPC:::.executor_backend_status()$capabilities$gpus, 4L)
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
    "printf '%s\\n' \"$DSHPC_GPUS_REQUESTED\" > \"$DSHPC_LOCAL_STEP_DIR/gpus_requested.txt\"",
    "printf '%s\\n' \"$DSHPC_BACKEND_GPU_SOURCE\" > \"$DSHPC_LOCAL_STEP_DIR/gpu_source.txt\"",
    "sh \"$DSHPC_STEP_SCRIPT\" >/dev/null 2>&1",
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
    dshpc.home = home,
    dshpc.executor_backend = "external",
    dshpc.external_submit_cmd = submit,
    dshpc.external_status_cmd = status,
    dshpc.backend_capabilities_cmd = capabilities,
    dshpc.backend_capabilities_ttl_secs = 0,
    dshpc.gpu_count = 0,
    dshpc.backend_gpu_count = "auto",
    dshpc.backend_request_optional_gpus = "auto",
    dshpc.max_retries = 0
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "external_gpu_optional", config = list())))
  dsHPC:::.store_create_job(db, "job_external_gpu", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_external_gpu", state = "RUNNING", step_index = 1L)
  dsHPC:::.executor_run_step(db, "job_external_gpu", 1L, spec)

  step_dir <- file.path(home, "artifacts", "job_external_gpu", "step_001")
  expect_equal(readLines(file.path(step_dir, "gpus_requested.txt"), warn = FALSE), "1")
  expect_equal(readLines(file.path(step_dir, "gpu_source.txt"), warn = FALSE),
    "external_capabilities_cmd")
  expect_equal(dsHPC:::.executor_backend_status()$capabilities$gpus, 2L)
})

test_that("slurm parses only parsable stdout and derives a default time limit", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  sbatch <- file.path(bin, "sbatch")
  args_file <- file.path(home, "sbatch_args.txt")
  withr::local_envvar(c(DSHPC_TEST_SBATCH_ARGS = args_file))
  writeLines(c(
    "#!/bin/sh",
    "printf '%s\\n' \"$@\" > \"$DSHPC_TEST_SBATCH_ARGS\"",
    "echo 'sbatch: warning: fallback account' >&2",
    "echo '12345;cluster-a'"
  ), sbatch)
  Sys.chmod(sbatch, "0755")
  writeLines(c(
    "name: slurm_parse",
    "plane: artifact",
    "command: /bin/sh",
    "args_template: ['-c', 'true']",
    "resources:",
    "  memory_mb: 64",
    "  cpu_slots: 1"
  ), file.path(home, "runners", "slurm_parse.yml"))
  withr::local_options(list(
    dshpc.home = home,
    dshpc.executor_backend = "slurm",
    dshpc.slurm_sbatch = sbatch,
    dshpc.slurm_time = "",
    dshpc.default_timeout_secs = 90
  ))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "slurm_parse", config = list())))
  dsHPC:::.store_create_job(db, "job_slurm_parse", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_slurm_parse",
    state = "RUNNING", step_index = 1L)
  dsHPC:::.executor_run_step(db, "job_slurm_parse", 1L, spec)

  step <- DBI::dbGetQuery(db,
    "SELECT external_id FROM steps WHERE job_id = 'job_slurm_parse'")
  expect_equal(step$external_id, "12345")
  expect_true("--time=00:01:30" %in% readLines(args_file, warn = FALSE))

  writeLines(c("#!/bin/sh", "echo not-a-job-id"), sbatch)
  Sys.chmod(sbatch, "0755")
  dsHPC:::.store_create_job(db, "job_slurm_bad_id", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_slurm_bad_id",
    state = "RUNNING", step_index = 1L)
  expect_error(
    dsHPC:::.executor_run_step(db, "job_slurm_bad_id", 1L, spec),
    "valid parsable job id")
})

test_that("artifact retries discard stale backend state and output", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  sbatch <- file.path(bin, "sbatch")
  squeue <- file.path(bin, "squeue")
  writeLines(c("#!/bin/sh", "echo 22222"), sbatch)
  writeLines(c("#!/bin/sh", "echo RUNNING"), squeue)
  Sys.chmod(c(sbatch, squeue), "0755")
  writeLines(c(
    "name: retry_clean",
    "plane: artifact",
    "command: /bin/sh",
    "args_template: ['-c', 'true']",
    "resources:",
    "  memory_mb: 64",
    "  cpu_slots: 1"
  ), file.path(home, "runners", "retry_clean.yml"))
  withr::local_options(list(
    dshpc.home = home,
    dshpc.executor_backend = "slurm",
    dshpc.slurm_sbatch = sbatch,
    dshpc.slurm_squeue = squeue,
    dshpc.slurm_sacct = "/definitely/not/sacct",
    dshpc.default_timeout_secs = 3600
  ))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "retry_clean", config = list())))
  dsHPC:::.store_create_job(db, "job_retry_clean", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_retry_clean",
    state = "RUNNING", step_index = 1L, retry_count = 1L)
  step_dir <- dsHPC:::.ensure_step_dir("job_retry_clean", 1L)
  stale_output <- file.path(step_dir, "output", "stale.txt")
  writeLines("stale", stale_output)
  writeLines("9", file.path(step_dir, "exit_code"))
  dsHPC:::.backend_write_external_marker(step_dir, "slurm", "11111")
  dsHPC:::.db_register_output(db, "job_retry_clean", 1L, "stale.txt",
    "artifact_file", stale_output, safe_for_client = FALSE)
  dsHPC:::.store_update_step(db, "job_retry_clean", 1L,
    state = "failed", exit_code = 9L, output_ref = file.path("artifacts",
      "job_retry_clean", "step_001", "output"),
    external_backend = "slurm", external_id = "11111",
    external_status = "FAILED")

  dsHPC:::.executor_run_step(db, "job_retry_clean", 1L, spec)
  step <- DBI::dbGetQuery(db,
    "SELECT state, exit_code, external_id, external_status
     FROM steps WHERE job_id = 'job_retry_clean' AND step_index = 1")
  expect_equal(step$state, "running")
  expect_true(is.na(step$exit_code))
  expect_equal(step$external_id, "22222")
  expect_equal(step$external_status, "submitted")
  expect_false(file.exists(file.path(step_dir, "exit_code")))
  expect_false(file.exists(stale_output))
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM outputs WHERE job_id = 'job_retry_clean'")$n, 0L)

  dsHPC:::.worker_reap(db)
  expect_equal(dsHPC:::.store_get_job(db, "job_retry_clean")$state, "RUNNING")
})

test_that("retry cleanup failure leaves durable attempt state retryable", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  outside <- tempfile("dshpc_retry_outside_")
  dir.create(outside)
  on.exit(unlink(outside, recursive = TRUE), add = TRUE)
  withr::local_options(list(dshpc.home = home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "unused", config = list())))
  dsHPC:::.store_create_job(db, "job_retry_blocked", "user", spec, 1L)
  dsHPC:::.store_update_step(db, "job_retry_blocked", 1L,
    state = "failed", exit_code = 9L, external_backend = "slurm",
    external_id = "11111", external_status = "FAILED")
  job_dir <- file.path(home, "artifacts", "job_retry_blocked")
  dir.create(job_dir, recursive = TRUE)
  step_dir <- file.path(job_dir, "step_001")
  if (!file.symlink(outside, step_dir)) skip("filesystem does not support symlinks")

  expect_error(
    dsHPC:::.reset_failed_step_attempt(db, "job_retry_blocked", 1L),
    "storage is unavailable")
  step <- DBI::dbGetQuery(db,
    "SELECT state, exit_code, external_id, external_status
     FROM steps WHERE job_id = 'job_retry_blocked'")
  expect_equal(step$state, "failed")
  expect_equal(step$exit_code, 9L)
  expect_equal(step$external_id, "11111")
  expect_equal(step$external_status, "FAILED")
})

test_that("running timeout waits for accepted external cancellation", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  submit <- file.path(bin, "submit")
  status <- file.path(bin, "status")
  cancel <- file.path(bin, "cancel")
  cancel_mode <- file.path(home, "cancel_mode")
  cancel_called <- file.path(home, "cancel_called")
  withr::local_envvar(c(
    DSHPC_TEST_CANCEL_MODE = cancel_mode,
    DSHPC_TEST_CANCEL_CALLED = cancel_called
  ))
  writeLines(c("#!/bin/sh", "echo ext-timeout"), submit)
  writeLines(c("#!/bin/sh", "echo RUNNING"), status)
  writeLines(c(
    "#!/bin/sh",
    "if [ \"$(cat \"$DSHPC_TEST_CANCEL_MODE\")\" = fail ]; then exit 42; fi",
    "echo \"$DSHPC_EXTERNAL_ID\" >> \"$DSHPC_TEST_CANCEL_CALLED\"",
    "exit 0"
  ), cancel)
  Sys.chmod(c(submit, status, cancel), "0755")
  writeLines(c(
    "name: timeout_runner",
    "plane: artifact",
    "command: /bin/sh",
    "args_template: ['-c', 'true']",
    "resources:",
    "  memory_mb: 64",
    "  cpu_slots: 1"
  ), file.path(home, "runners", "timeout_runner.yml"))
  withr::local_options(list(
    dshpc.home = home,
    dshpc.executor_backend = "external",
    dshpc.external_submit_cmd = submit,
    dshpc.external_status_cmd = status,
    dshpc.external_cancel_cmd = cancel,
    dshpc.default_timeout_secs = 1,
    dshpc.max_retries = 0
  ))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "timeout_runner", config = list())))
  dsHPC:::.store_create_job(db, "job_timeout", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_timeout",
    state = "RUNNING", step_index = 1L)
  dsHPC:::.executor_run_step(db, "job_timeout", 1L, spec)
  dsHPC:::.store_update_step(db, "job_timeout", 1L,
    started_at = format(Sys.time() - 60, "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))

  writeLines("fail", cancel_mode)
  dsHPC:::.worker_reap(db)
  expect_equal(dsHPC:::.store_get_job(db, "job_timeout")$state, "RUNNING")
  step <- DBI::dbGetQuery(db,
    "SELECT external_status FROM steps WHERE job_id = 'job_timeout'")
  expect_equal(step$external_status, "TIMEOUT_CANCEL_FAILED")

  writeLines("succeed", cancel_mode)
  dsHPC:::.worker_reap(db)
  expect_equal(readLines(cancel_called, warn = FALSE), "ext-timeout")
  expect_equal(dsHPC:::.store_get_job(db, "job_timeout")$state, "RUNNING")
  step <- DBI::dbGetQuery(db,
    "SELECT exit_code, external_status FROM steps WHERE job_id = 'job_timeout'")
  expect_true(is.na(step$exit_code))
  expect_equal(step$external_status, "TIMEOUT_CANCEL_REQUESTED")
  expect_equal(dsHPC:::.store_get_job(db, "job_timeout")$retry_count, 0L)

  dsHPC:::.worker_reap(db)
  expect_equal(length(readLines(cancel_called, warn = FALSE)), 1L)
  step <- DBI::dbGetQuery(db,
    "SELECT external_status FROM steps WHERE job_id = 'job_timeout'")
  expect_equal(step$external_status, "TIMEOUT_CANCEL_REQUESTED")

  writeLines(c("#!/bin/sh", "echo CANCELLED 143"), status)
  Sys.chmod(status, "0755")
  dsHPC:::.worker_reap(db)
  expect_equal(dsHPC:::.store_get_job(db, "job_timeout")$state, "FAILED")
  step <- DBI::dbGetQuery(db,
    "SELECT exit_code, external_status FROM steps WHERE job_id = 'job_timeout'")
  expect_equal(step$exit_code, 124L)
  expect_equal(step$external_status, "TIMEOUT")
})

test_that("admin cancellation does not confirm a rejected backend request", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  bin <- file.path(home, "bin")
  dir.create(bin, showWarnings = FALSE)
  scancel <- file.path(bin, "scancel")
  squeue <- file.path(bin, "squeue")
  sacct <- file.path(bin, "sacct")
  writeLines(c("#!/bin/sh", "exit 42"), scancel)
  writeLines(c("#!/bin/sh", "exit 0"), squeue)
  writeLines(c("#!/bin/sh", "echo CANCELLED\\|0:0"), sacct)
  Sys.chmod(c(scancel, squeue, sacct), "0755")
  withr::local_options(list(
    dshpc.home = home,
    dshpc.admin_key = "secret",
    dshpc.slurm_scancel = scancel,
    dshpc.slurm_squeue = squeue,
    dshpc.slurm_sacct = sacct
  ))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "unused", config = list())))
  dsHPC:::.store_create_job(db, "job_cancel_rejected", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_cancel_rejected",
    state = "RUNNING", step_index = 1L)
  dsHPC:::.store_update_step(db, "job_cancel_rejected", 1L,
    state = "running", external_backend = "slurm", external_id = "33333")

  expect_error(hpcAdminCancelDS("job_cancel_rejected", "secret"),
    "not accepted")
  expect_equal(dsHPC:::.store_get_job(db, "job_cancel_rejected")$state,
    "RUNNING")

  writeLines(c("#!/bin/sh", "exit 0"), scancel)
  Sys.chmod(scancel, "0755")
  cancelled <- hpcAdminCancelDS("job_cancel_rejected", "secret")
  expect_equal(cancelled$state, "RUNNING")
  expect_equal(cancelled$cancellation, "REQUESTED")
  expect_equal(dsHPC:::.store_get_job(db, "job_cancel_rejected")$state,
    "RUNNING")
  step <- DBI::dbGetQuery(db,
    "SELECT external_status FROM steps WHERE job_id = 'job_cancel_rejected'")
  expect_equal(step$external_status, "CANCEL_REQUESTED")

  dsHPC:::.worker_reap(db)
  expect_equal(dsHPC:::.store_get_job(db, "job_cancel_rejected")$state,
    "CANCELLED")
})

options(
  dshpc.home = NULL,
  dshpc.executor_backend = NULL,
  dshpc.slurm_sbatch = NULL,
  dshpc.slurm_squeue = NULL,
  dshpc.slurm_sacct = NULL,
  dshpc.slurm_sinfo = NULL,
  dshpc.slurm_scancel = NULL,
  dshpc.external_submit_cmd = NULL,
  dshpc.external_status_cmd = NULL,
  dshpc.external_cancel_cmd = NULL,
  dshpc.kubernetes_kubectl = NULL,
  dshpc.kubernetes_context = NULL,
  dshpc.kubernetes_namespace = NULL,
  dshpc.kubernetes_service_account = NULL,
  dshpc.kubernetes_image = NULL,
  dshpc.kubernetes_image_pull_policy = NULL,
  dshpc.kubernetes_pvc = NULL,
  dshpc.kubernetes_mount_path = NULL,
  dshpc.kubernetes_backoff_limit = NULL,
  dshpc.kubernetes_ttl_seconds_after_finished = NULL,
  dshpc.backend_capabilities_cmd = NULL,
  dshpc.backend_capabilities_ttl_secs = NULL,
  dshpc.external_enforce_runner_concurrency = NULL)
options(dshpc.backend_path_mappings = NULL,
        dshpc.container_runtime = NULL,
        dshpc.container_pull = NULL,
        dshpc.backend_gpu_count = NULL,
        dshpc.backend_request_optional_gpus = NULL)
