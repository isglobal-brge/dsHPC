# Module: Artifact-Plane Runners
# Async artifact subprocesses. Worker reaps on next poll.

# Global registry of processx handles so the worker can use proc$get_exit_status()
# instead of PID-based checking (which fails on Rosetta/cross-arch emulation).
.proc_registry <- new.env(parent = emptyenv())

#' @keywords internal
.run_artifact_step <- function(db, job_id, step_index, step, step_dir, input_dir) {
  prepared <- .prepare_artifact_command(db, job_id, step_index, step, step_dir, input_dir)
  backend <- .executor_backend_name()
  if (!identical(backend, "embedded")) {
    .backend_submit_artifact_step(db, job_id, step_index, step, step_dir,
      input_dir, prepared = prepared)
    return(invisible(TRUE))
  }

  pid <- .launch_artifact_process(prepared, step_dir)
  .store_update_job(db, job_id, worker_pid = pid)
  .db_log_event(db, job_id, "artifact_started",
    list(step_index = step_index, runner = step$runner, pid = pid,
         backend = "embedded"))
}

#' Launch an embedded artifact process with explicit PID and exit-code files
#' @keywords internal
.launch_artifact_process <- function(prepared, step_dir) {
  if (!identical(.Platform$OS.type, "unix")) {
    proc <- processx::process$new(
      command = prepared$command, args = prepared$args,
      stdout = file.path(step_dir, "stdout.log"),
      stderr = file.path(step_dir, "stderr.log"),
      env = prepared$env_vars, cleanup = TRUE, cleanup_tree = TRUE)
    return(proc$get_pid())
  }

  script <- file.path(step_dir, "run.sh")
  command <- paste(c(shQuote(prepared$command),
                     shQuote(prepared$args)), collapse = " ")
  exit_file <- shQuote(file.path(step_dir, "exit_code"))
  exit_tmp <- shQuote(file.path(step_dir, "exit_code.tmp"))
  child_pid_file <- shQuote(file.path(step_dir, "child.pid"))
  stdout_file <- shQuote(file.path(step_dir, "stdout.log"))
  stderr_file <- shQuote(file.path(step_dir, "stderr.log"))
  env_vars <- prepared$env_vars
  env_lines <- character(0)
  if (length(env_vars) > 0) {
    env_lines <- vapply(names(env_vars), function(nm) {
      if (!nzchar(nm) || identical(env_vars[[nm]], "current")) return("")
      paste0("export ", nm, "=", shQuote(as.character(env_vars[[nm]])))
    }, character(1))
    env_lines <- env_lines[nzchar(env_lines)]
  }

  lines <- c(
    "#!/bin/sh",
    "umask 007",
    "set +e",
    env_lines,
    sprintf("cd %s || exit 1", shQuote(step_dir)),
    sprintf("mkdir -p %s", shQuote(prepared$output_dir)),
    "child=\"\"",
    "write_exit_code() {",
    paste0("  printf '%s\\n' \"$1\" > ", exit_tmp),
    paste0("  mv ", exit_tmp, " ", exit_file),
    "}",
    "term_child() {",
    "  if [ -n \"$child\" ] && kill -0 \"$child\" 2>/dev/null; then",
    "    kill -TERM \"$child\" 2>/dev/null || true",
    "    sleep 2",
    "    kill -KILL \"$child\" 2>/dev/null || true",
    "  fi",
    "  write_exit_code 143",
    "  exit 143",
    "}",
    "trap term_child TERM INT",
    paste0(command, " > ", stdout_file, " 2> ", stderr_file, " &"),
    "child=$!",
    paste0("printf '%s\\n' \"$child\" > ", child_pid_file),
    "wait \"$child\"",
    "code=$?",
    "write_exit_code \"$code\"",
    "exit \"$code\""
  )
  .dshpc_with_private_umask(writeLines(lines, script))
  Sys.chmod(script, "0770", use_umask = FALSE)

  env_command <- Sys.which("env")
  nohup_command <- Sys.which("nohup")
  if (!nzchar(env_command) || !nzchar(nohup_command)) {
    stop("A clean artifact runner environment is unavailable.", call. = FALSE)
  }
  clean_env <- paste(vapply(names(env_vars), function(nm) {
    shQuote(paste0(nm, "=", as.character(env_vars[[nm]])))
  }, character(1)), collapse = " ")
  launch <- sprintf(
    "cd %s && %s -i %s %s /bin/sh %s >/dev/null 2>&1 & echo $!",
    shQuote(step_dir), shQuote(env_command), clean_env,
    shQuote(nohup_command), shQuote(script))
  pid <- tryCatch(
    as.integer(system2("/bin/sh", c("-c", launch), stdout = TRUE,
                       stderr = FALSE)[1]),
    error = function(e) NA_integer_)
  if (is.na(pid)) stop("Failed to launch artifact runner.", call. = FALSE)
  pid
}

#' Build the minimal environment inherited by an artifact runner
#' @noRd
.artifact_runner_environment <- function(job_id, step_dir) {
  .dshpc_validate_job_artifact_path(step_dir, job_id, check_tree = TRUE)
  runtime_root <- file.path(step_dir, "runtime")
  runtime_dirs <- file.path(runtime_root, c("home", "tmp"))
  if (any(.dshpc_path_is_symlink(c(runtime_root, runtime_dirs)))) {
    stop("Artifact runner runtime storage is unavailable.", call. = FALSE)
  }
  for (path in c(runtime_root, runtime_dirs)) {
    .dshpc_with_private_umask(dir.create(path, recursive = TRUE,
      showWarnings = FALSE, mode = "0770"))
    if (!dir.exists(path) || .dshpc_path_is_symlink(path)) {
      stop("Artifact runner runtime storage is unavailable.", call. = FALSE)
    }
    .dshpc_validate_job_artifact_path(path, job_id, check_tree = TRUE)
    tryCatch(Sys.chmod(path, "0770", use_umask = FALSE),
      error = function(e) NULL)
  }

  path <- Sys.getenv("PATH", unset = "/usr/local/bin:/usr/bin:/bin")
  if (!nzchar(path)) path <- "/usr/local/bin:/usr/bin:/bin"
  env <- c(
    PATH = path,
    HOME = runtime_dirs[[1L]],
    TMPDIR = runtime_dirs[[2L]],
    TMP = runtime_dirs[[2L]],
    TEMP = runtime_dirs[[2L]])
  for (name in c("LANG", "LC_ALL", "LC_CTYPE", "TZ")) {
    value <- Sys.getenv(name, unset = "")
    if (nzchar(value)) env[[name]] <- value
  }
  env
}

#' Prepare an allowlisted artifact runner command for an executor backend.
#' @keywords internal
.prepare_artifact_command <- function(db, job_id, step_index, step, step_dir, input_dir) {
  runner_name <- step$runner
  runner_config <- .load_runner_config(runner_name)
  if (is.null(runner_config)) stop("Runner '", runner_name, "' not found.", call. = FALSE)
  .validate_runner_params(step, runner_config, step_index)
  registered_by <- runner_config$registered_by %||% NULL
  if (!is.null(registered_by) && !identical(registered_by, "dsHPC")) {
    job <- .store_get_job(db, job_id)
    if (is.null(job) ||
        !.dshpc_label_matches_package(job$label, registered_by)) {
      stop("Runner '", runner_name,
        "' is not registered for this job label.", call. = FALSE)
    }
  }

  raw_command <- runner_config$command %||% "python"
  command <- raw_command
  if (identical(command, "python")) {
    py <- .resolve_python_env(runner_config)
    command <- py$python
  }

  args <- .build_runner_args(runner_config, step, step_dir, input_dir)

  # Wrap with /usr/bin/env so runner-declared env vars in runner_config$env are
  # actually applied to the spawned process, regardless of how processx handles
  # the `env =` argument (some configurations silently drop appended entries).
  if (!is.null(runner_config$env) && is.list(runner_config$env) &&
      length(runner_config$env) > 0 && file.exists("/usr/bin/env")) {
    env_args <- c(unname(vapply(names(runner_config$env), function(k) {
      v <- as.character(runner_config$env[[k]])
      paste0(k, "=", v)
    }, character(1))), command)
    args <- c(env_args, args)
    command <- "/usr/bin/env"
  }
  output_dir <- file.path(step_dir, "output")

  # Runners start from a small explicit environment. HOME and temporary files
  # remain inside the job tree; arbitrary Rock/worker variables are not
  # inherited. R's LD_LIBRARY_PATH is cleared because it conflicts with some
  # Python native libraries (notably pyarrow's libarrow).
  env_vars <- c(
    .artifact_runner_environment(job_id, step_dir),
    LD_LIBRARY_PATH = "",
    DSHPC_STEP_DIR = step_dir,
    DSHPC_OUTPUT_DIR = output_dir,
    DSHPC_JOB_ID = job_id,
    DSHPC_STEP_INDEX = as.character(step_index),
    # MKL workaround for amd64-on-arm64 Rosetta emulation. Harmless on other
    # platforms. (Without these, Intel oneMKL refuses to load libtorch_cpu.so.)
    MKL_SERVICE_FORCE_INTEL = "0",
    MKL_THREADING_LAYER = "GNU")
  if (!is.null(input_dir))
    env_vars <- c(env_vars, DSHPC_INPUT_DIR = input_dir)
  inputs_json <- if (!is.null(input_dir)) file.path(input_dir, "inputs.json") else ""
  if (nzchar(inputs_json) && file.exists(inputs_json))
    env_vars <- c(env_vars, DSHPC_INPUTS_JSON = inputs_json)
  gpu_devices <- .scheduler_job_gpu_devices(db, job_id)
  if (length(gpu_devices) > 0) {
    gpu_csv <- paste(gpu_devices, collapse = ",")
    env_vars <- c(env_vars,
      CUDA_VISIBLE_DEVICES = gpu_csv,
      NVIDIA_VISIBLE_DEVICES = gpu_csv,
      DSHPC_GPU_DEVICES = gpu_csv)
  }
  if (!is.null(step$config)) {
    for (nm in names(step$config)) {
      val <- step$config[[nm]]
      if (is.null(val) || is.list(val)) next
      upper <- toupper(nm)
      if (upper %in% .BLOCKED_ENV_VARS)
        stop("Config key '", nm, "' is blocked for security.", call. = FALSE)
      val_str <- if (length(val) > 1) paste(val, collapse = ",")
                 else as.character(val)
      new_var <- val_str
      names(new_var) <- paste0("DSHPC_CFG_", upper)
      env_vars <- c(env_vars, new_var)
    }
  }

  # Runner-declared env vars from the YAML config (e.g. MKL workarounds for
  # torch under Rosetta emulation). Anything in runner_config$env is merged in.
  if (!is.null(runner_config$env) && is.list(runner_config$env)) {
    for (nm in names(runner_config$env)) {
      if (!nzchar(nm)) next
      if (toupper(nm) %in% .BLOCKED_ENV_VARS ||
          startsWith(toupper(nm), "DSHPC_")) {
        stop("Runner env variable '", nm, "' is reserved.", call. = FALSE)
      }
      v <- as.character(runner_config$env[[nm]])
      names(v) <- nm
      env_vars <- c(env_vars, v)
    }
  }

  # Make overrides deterministic and keep one value per variable. Reserved
  # runtime names cannot reach this point from runner configuration.
  named <- !is.na(names(env_vars)) & nzchar(names(env_vars))
  env_vars <- env_vars[named]
  env_vars <- env_vars[!duplicated(names(env_vars), fromLast = TRUE)]

  # Persist the resolved command/args/env next to the step output. Useful for
  # post-mortem debugging when a runner exits non-zero; harmless otherwise.
  tryCatch(.dshpc_with_private_umask(writeLines(
    c(paste0("# job=", job_id, " step=", step_index),
      paste0("# command=", command),
      paste0("# args=", paste(args, collapse = " ")),
      paste(names(env_vars), env_vars, sep = "=")),
    file.path(step_dir, "env.log"))), error = function(e) NULL)
  tryCatch(Sys.chmod(file.path(step_dir, "env.log"), "0660",
                     use_umask = FALSE), error = function(e) NULL)
  list(command = command, raw_command = raw_command, args = args,
       env_vars = env_vars, step = step, input_dir = input_dir,
       step_dir = step_dir,
       output_dir = output_dir, runner_config = runner_config)
}

#' Check if a job's artifact step is still running via processx handle
#' @keywords internal
.proc_is_alive <- function(job_id, step_index) {
  key <- paste0(job_id, "_", step_index)
  proc <- .proc_registry[[key]]
  if (is.null(proc)) return(FALSE)  # No handle = assume dead
  alive <- tryCatch(proc$is_alive(), error = function(e) FALSE)
  if (!isTRUE(alive)) return(FALSE)

  # Under cross-arch emulation (Rosetta/qemu), processx can occasionally keep a
  # stale live handle after the child has exited. The PID check is authoritative
  # in Linux containers and prevents jobs from staying RUNNING forever.
  pid <- tryCatch(proc$get_pid(), error = function(e) NA_integer_)
  if (!.pid_is_alive(pid)) return(FALSE)
  TRUE
}

#' Get exit status from processx handle, clean up registry
#' @keywords internal
.proc_get_exit <- function(job_id, step_index) {
  key <- paste0(job_id, "_", step_index)
  proc <- .proc_registry[[key]]
  if (is.null(proc)) return(NA_integer_)
  status <- proc$get_exit_status()
  # Clean up handle
  rm(list = key, envir = .proc_registry)
  status
}

#' @keywords internal
.resolve_python_env <- function(runner_config) {
  # If runner specifies an explicit python path, use it
  if (!is.null(runner_config$python)) {
    if (file.exists(runner_config$python))
      return(list(python = runner_config$python))
  }

  # System python fallback
  py <- Sys.which("python3")
  if (!nzchar(py)) py <- Sys.which("python")
  if (!nzchar(py)) py <- "python3"
  list(python = py)
}

#' @keywords internal
.build_runner_args <- function(runner_config, step, step_dir, input_dir) {
  tmpl <- runner_config$args_template
  if (is.null(tmpl)) return(character(0))
  in_dir <- input_dir %||% step_dir
  out_dir <- file.path(step_dir, "output")
  vapply(tmpl, function(a) {
    a <- gsub("{input_dir}", in_dir, a, fixed = TRUE)
    a <- gsub("{output_dir}", out_dir, a, fixed = TRUE)
    a <- gsub("{step_dir}", step_dir, a, fixed = TRUE)
    if (!is.null(step$config)) {
      for (nm in names(step$config)) {
        val <- step$config[[nm]]
        if (is.null(val) || is.list(val)) next
        val <- as.character(val)
        if (length(val) > 1) val <- paste(val, collapse = ",")
        a <- gsub(paste0("{", nm, "}"), val, a, fixed = TRUE)
      }
    }
    a
  }, character(1))
}
