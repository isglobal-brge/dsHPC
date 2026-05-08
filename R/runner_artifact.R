# Module: Artifact-Plane Runners
# Async processx subprocesses. Worker reaps on next poll.

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

  proc <- processx::process$new(
    command = prepared$command, args = prepared$args,
    stdout = file.path(step_dir, "stdout.log"),
    stderr = file.path(step_dir, "stderr.log"),
    env = prepared$env_vars, cleanup = TRUE, cleanup_tree = TRUE)

  # Store handle in registry for reliable exit status checking
  key <- paste0(job_id, "_", step_index)
  .proc_registry[[key]] <- proc

  .store_update_job(db, job_id, worker_pid = proc$get_pid())
  .db_log_event(db, job_id, "artifact_started",
    list(step_index = step_index, runner = step$runner, pid = proc$get_pid(),
         backend = "embedded"))
}

#' Prepare an allowlisted artifact runner command for an executor backend.
#' @keywords internal
.prepare_artifact_command <- function(db, job_id, step_index, step, step_dir, input_dir) {
  runner_name <- step$runner
  runner_config <- .load_runner_config(runner_name)
  if (is.null(runner_config)) stop("Runner '", runner_name, "' not found.", call. = FALSE)

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

  # processx expects named character vector: c(VAR = "value", ...)
  # "current" inherits the parent environment, but R's LD_LIBRARY_PATH
  # conflicts with Python native libs (pyarrow's libarrow). Clear it.
  env_vars <- c(
    "current",
    LD_LIBRARY_PATH = "",
    DSJOBS_STEP_DIR = step_dir,
    DSJOBS_OUTPUT_DIR = output_dir,
    DSJOBS_JOB_ID = job_id,
    DSJOBS_STEP_INDEX = as.character(step_index),
    # MKL workaround for amd64-on-arm64 Rosetta emulation. Harmless on other
    # platforms. (Without these, Intel oneMKL refuses to load libtorch_cpu.so.)
    MKL_SERVICE_FORCE_INTEL = "0",
    MKL_THREADING_LAYER = "GNU")
  if (!is.null(input_dir))
    env_vars <- c(env_vars, DSJOBS_INPUT_DIR = input_dir)
  gpu_devices <- .scheduler_job_gpu_devices(db, job_id)
  if (length(gpu_devices) > 0) {
    gpu_csv <- paste(gpu_devices, collapse = ",")
    env_vars <- c(env_vars,
      CUDA_VISIBLE_DEVICES = gpu_csv,
      NVIDIA_VISIBLE_DEVICES = gpu_csv,
      DSJOBS_GPU_DEVICES = gpu_csv)
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
      names(new_var) <- paste0("DSJOBS_CFG_", upper)
      env_vars <- c(env_vars, new_var)
    }
  }

  # Runner-declared env vars from the YAML config (e.g. MKL workarounds for
  # torch under Rosetta emulation). Anything in runner_config$env is merged in.
  if (!is.null(runner_config$env) && is.list(runner_config$env)) {
    for (nm in names(runner_config$env)) {
      if (!nzchar(nm)) next
      if (toupper(nm) %in% .BLOCKED_ENV_VARS) next
      v <- as.character(runner_config$env[[nm]])
      names(v) <- nm
      env_vars <- c(env_vars, v)
    }
  }

  # Persist the resolved command/args/env next to the step output. Useful for
  # post-mortem debugging when a runner exits non-zero; harmless otherwise.
  tryCatch(writeLines(
    c(paste0("# job=", job_id, " step=", step_index),
      paste0("# command=", command),
      paste0("# args=", paste(args, collapse = " ")),
      paste(names(env_vars), env_vars, sep = "=")),
    file.path(step_dir, "env.log")), error = function(e) NULL)
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
  proc$is_alive()
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
