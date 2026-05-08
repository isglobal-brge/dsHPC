# Module: Executor Backends
#
# The embedded backend runs allowlisted artifact runners as local processx
# children. External backends keep the same runner contracts but delegate
# resource enforcement to a site scheduler such as Slurm or a site-provided
# wrapper. User job specs never carry executable scheduler commands; these are
# admin options only.

#' @keywords internal
.executor_backend_name <- function(settings = .dsjobs_settings()) {
  value <- tolower(as.character(settings$executor_backend %||% "embedded")[1])
  if (!nzchar(value)) value <- "embedded"
  if (value %in% c("local", "processx")) return("embedded")
  if (value %in% c("slurm", "kubernetes", "external", "embedded")) return(value)
  value
}

#' @keywords internal
.executor_delegates_resources <- function(settings = .dsjobs_settings()) {
  !isTRUE(settings$external_enforce_local_resources) &&
    .executor_backend_name(settings) %in% c("slurm", "kubernetes", "external")
}

#' @keywords internal
.executor_enforces_runner_concurrency <- function(settings = .dsjobs_settings()) {
  if (identical(.executor_backend_name(settings), "embedded")) return(TRUE)
  isTRUE(settings$external_enforce_runner_concurrency)
}

#' @keywords internal
.executor_backend_status <- function(settings = .dsjobs_settings()) {
  backend <- .executor_backend_name(settings)
  out <- list(
    backend = backend,
    available = TRUE,
    delegates_resources = .executor_delegates_resources(settings),
    reason = "ok",
    commands = list())

  if (identical(backend, "embedded")) return(out)

  if (identical(backend, "slurm")) {
    cmds <- list(
      sbatch = .backend_resolve_cmd(settings$slurm_sbatch, "sbatch"),
      squeue = .backend_resolve_cmd(settings$slurm_squeue, "squeue"),
      sacct = .backend_resolve_cmd(settings$slurm_sacct, "sacct"),
      scancel = .backend_resolve_cmd(settings$slurm_scancel, "scancel"))
    out$commands <- cmds
    out$available <- nzchar(cmds$sbatch)
    if (!out$available) out$reason <- "sbatch_not_found"
    return(out)
  }

  if (identical(backend, "external")) {
    cmds <- list(
      submit = .backend_command_parts(settings$external_submit_cmd)$command,
      status = .backend_command_parts(settings$external_status_cmd)$command,
      cancel = .backend_command_parts(settings$external_cancel_cmd)$command)
    out$commands <- cmds
    out$available <- nzchar(cmds$submit) && nzchar(cmds$status)
    if (!out$available) out$reason <- "external_submit_or_status_not_configured"
    return(out)
  }

  if (identical(backend, "kubernetes")) {
    kubectl <- .backend_resolve_cmd(.dsj_option("kubernetes_kubectl",
      Sys.getenv("DSJOBS_KUBERNETES_KUBECTL", unset = "")), "kubectl")
    out$commands <- list(kubectl = kubectl)
    out$available <- nzchar(kubectl)
    out$reason <- if (out$available) "kubectl_available_contract_pending" else "kubectl_not_found"
    return(out)
  }

  out$available <- FALSE
  out$reason <- paste0("unsupported_executor_backend:", backend)
  out
}

#' @keywords internal
.backend_resolve_cmd <- function(configured, fallback) {
  configured <- as.character(configured %||% "")[1]
  if (nzchar(configured)) {
    if (grepl("[/\\\\]", configured)) {
      return(if (file.exists(configured)) configured else "")
    }
    found <- Sys.which(configured)
    return(if (length(found) == 1 && nzchar(found)) unname(found) else "")
  }
  found <- Sys.which(fallback)
  if (length(found) == 1 && nzchar(found)) unname(found) else ""
}

#' @keywords internal
.backend_command_parts <- function(value) {
  if (is.null(value) || length(value) == 0)
    return(list(command = "", args = character(0)))
  value <- as.character(value)
  value <- value[nzchar(value)]
  if (length(value) == 0) return(list(command = "", args = character(0)))
  list(command = value[1], args = value[-1])
}

#' @keywords internal
.backend_submit_artifact_step <- function(db, job_id, step_index, step,
                                          step_dir, input_dir,
                                          prepared = NULL) {
  settings <- .dsjobs_settings()
  backend <- .executor_backend_name(settings)
  if (identical(backend, "embedded"))
    stop("Internal error: embedded backend should use processx.", call. = FALSE)

  status <- .executor_backend_status(settings)
  if (!isTRUE(status$available))
    stop("Executor backend '", backend, "' is not available: ",
         status$reason, call. = FALSE)

  if (is.null(prepared))
    prepared <- .prepare_artifact_command(db, job_id, step_index, step, step_dir, input_dir)

  if (identical(backend, "slurm")) {
    external_id <- .backend_submit_slurm(job_id, step_index, step, step_dir,
      prepared, settings)
  } else if (identical(backend, "external")) {
    external_id <- .backend_submit_external(job_id, step_index, step, step_dir,
      prepared, settings)
  } else {
    stop("Executor backend '", backend, "' cannot submit artifact steps yet.",
         call. = FALSE)
  }

  .store_update_step(db, job_id, step_index,
    external_backend = backend,
    external_id = external_id,
    external_status = "submitted")
  .store_update_job(db, job_id, worker_pid = NA_integer_)
  .db_log_event(db, job_id, "artifact_submitted",
    list(step_index = step_index, runner = step$runner %||% NA_character_,
         backend = backend, external_id = external_id))
  external_id
}

#' @keywords internal
.backend_submit_slurm <- function(job_id, step_index, step, step_dir,
                                  prepared, settings = .dsjobs_settings()) {
  sbatch <- .backend_resolve_cmd(settings$slurm_sbatch, "sbatch")
  if (!nzchar(sbatch)) stop("sbatch not found.", call. = FALSE)

  script <- file.path(step_dir, "run_step.sh")
  .backend_write_step_script(script, prepared)

  profile <- .scheduler_runner_profile(step$runner, settings)
  job_name <- paste0("dsjobs_", substr(gsub("[^A-Za-z0-9]", "", job_id), 1, 16),
    "_", step_index)
  args <- c("--parsable",
    paste0("--job-name=", job_name),
    paste0("--output=", file.path(step_dir, "stdout.log")),
    paste0("--error=", file.path(step_dir, "stderr.log")),
    paste0("--chdir=", step_dir),
    paste0("--cpus-per-task=", max(1L, as.integer(profile$cpu_slots %||% 1L))),
    paste0("--mem=", max(1L, as.integer(profile$memory_mb %||% 1L))))

  if (nzchar(settings$slurm_partition %||% ""))
    args <- c(args, paste0("--partition=", settings$slurm_partition))
  if (nzchar(settings$slurm_account %||% ""))
    args <- c(args, paste0("--account=", settings$slurm_account))
  if (nzchar(settings$slurm_qos %||% ""))
    args <- c(args, paste0("--qos=", settings$slurm_qos))
  if (nzchar(settings$slurm_time %||% ""))
    args <- c(args, paste0("--time=", settings$slurm_time))

  gpu_n <- as.integer(profile$gpus %||% 0L)
  if (gpu_n <= 0L && isTRUE(settings$slurm_request_optional_gpus))
    gpu_n <- as.integer(profile$optional_gpus %||% 0L)
  if (gpu_n > 0L) args <- c(args, paste0("--gres=gpu:", gpu_n))

  extra <- as.character(settings$slurm_extra_args %||% character(0))
  if (length(extra) > 0) args <- c(args, extra[nzchar(extra)])
  args <- c(args, script)

  out <- tryCatch(system2(sbatch, args, stdout = TRUE, stderr = TRUE),
    error = function(e) stop("sbatch failed: ", conditionMessage(e), call. = FALSE))
  status <- attr(out, "status")
  if (!is.null(status) && !identical(as.integer(status), 0L))
    stop("sbatch failed: ", paste(out, collapse = "\n"), call. = FALSE)
  first <- trimws(out[1] %||% "")
  id <- strsplit(first, ";", fixed = TRUE)[[1]][1]
  if (!nzchar(id)) stop("sbatch did not return a job id.", call. = FALSE)
  id
}

#' @keywords internal
.backend_submit_external <- function(job_id, step_index, step, step_dir,
                                     prepared, settings = .dsjobs_settings()) {
  submit <- .backend_command_parts(settings$external_submit_cmd)
  if (!nzchar(submit$command))
    stop("External submit command is not configured.", call. = FALSE)

  script <- file.path(step_dir, "run_step.sh")
  .backend_write_step_script(script, prepared)
  profile <- .scheduler_runner_profile(step$runner, settings)
  env <- c(
    DSJOBS_JOB_ID = job_id,
    DSJOBS_STEP_INDEX = as.character(step_index),
    DSJOBS_RUNNER = step$runner %||% "",
    DSJOBS_STEP_DIR = step_dir,
    DSJOBS_STEP_SCRIPT = script,
    DSJOBS_MEMORY_MB = as.character(profile$memory_mb %||% 0L),
    DSJOBS_CPU_SLOTS = as.character(profile$cpu_slots %||% 0L),
    DSJOBS_GPUS = as.character(profile$gpus %||% 0L))
  out <- tryCatch(system2(submit$command, submit$args, stdout = TRUE,
    stderr = TRUE, env = .backend_env(env)), error = function(e)
      stop("External submit failed: ", conditionMessage(e), call. = FALSE))
  status <- attr(out, "status")
  if (!is.null(status) && !identical(as.integer(status), 0L))
    stop("External submit failed: ", paste(out, collapse = "\n"), call. = FALSE)
  id <- trimws(out[1] %||% "")
  if (!nzchar(id)) stop("External submit did not return a job id.", call. = FALSE)
  id
}

#' @keywords internal
.backend_step_status <- function(backend, external_id, step_dir,
                                 settings = .dsjobs_settings()) {
  backend <- tolower(as.character(backend %||% "")[1])
  if (identical(backend, "slurm"))
    return(.backend_status_slurm(external_id, step_dir, settings))
  if (identical(backend, "external"))
    return(.backend_status_external(external_id, step_dir, settings))
  list(state = "failed", exit_code = 1L,
       reason = paste0("unsupported_external_backend:", backend))
}

#' @keywords internal
.backend_status_slurm <- function(external_id, step_dir,
                                  settings = .dsjobs_settings()) {
  squeue <- .backend_resolve_cmd(settings$slurm_squeue, "squeue")
  if (nzchar(squeue)) {
    out <- tryCatch(system2(squeue, c("-h", "-j", external_id, "-o", "%T"),
      stdout = TRUE, stderr = FALSE), error = function(e) character(0))
    out <- trimws(out[nzchar(trimws(out))])
    if (length(out) > 0) {
      state <- toupper(out[1])
      if (state %in% c("PENDING", "RUNNING", "CONFIGURING", "COMPLETING"))
        return(list(state = "running", external_state = state, exit_code = NA_integer_))
    }
  }

  sacct <- .backend_resolve_cmd(settings$slurm_sacct, "sacct")
  if (nzchar(sacct)) {
    out <- tryCatch(system2(sacct,
      c("-n", "-P", "-j", external_id, "--format=State,ExitCode", "-X"),
      stdout = TRUE, stderr = FALSE), error = function(e) character(0))
    parsed <- .backend_parse_slurm_sacct(out)
    if (!is.null(parsed)) return(parsed)
  }

  exit_file <- file.path(step_dir, "exit_code")
  if (file.exists(exit_file)) {
    code <- tryCatch(as.integer(readLines(exit_file, n = 1, warn = FALSE)),
      error = function(e) NA_integer_)
    if (identical(as.integer(code), 0L))
      return(list(state = "succeeded", external_state = "LOCAL_EXIT_FILE",
                  exit_code = 0L))
    if (!is.na(code))
      return(list(state = "failed", external_state = "LOCAL_EXIT_FILE",
                  exit_code = as.integer(code)))
  }
  list(state = "running", external_state = "UNKNOWN", exit_code = NA_integer_)
}

#' @keywords internal
.backend_parse_slurm_sacct <- function(lines) {
  lines <- trimws(lines[nzchar(trimws(lines))])
  if (length(lines) == 0) return(NULL)
  fields <- strsplit(lines[1], "|", fixed = TRUE)[[1]]
  state <- toupper(fields[1] %||% "")
  exit_field <- fields[2] %||% "1:0"
  exit_code <- suppressWarnings(as.integer(strsplit(exit_field, ":", fixed = TRUE)[[1]][1]))
  if (is.na(exit_code)) exit_code <- if (identical(state, "COMPLETED")) 0L else 1L
  if (identical(state, "COMPLETED") && identical(exit_code, 0L))
    return(list(state = "succeeded", external_state = state, exit_code = 0L))
  if (state %in% c("PENDING", "RUNNING", "CONFIGURING", "COMPLETING"))
    return(list(state = "running", external_state = state, exit_code = NA_integer_))
  list(state = "failed", external_state = state, exit_code = exit_code)
}

#' @keywords internal
.backend_status_external <- function(external_id, step_dir,
                                     settings = .dsjobs_settings()) {
  status <- .backend_command_parts(settings$external_status_cmd)
  if (!nzchar(status$command))
    return(list(state = "failed", exit_code = 1L,
                reason = "external_status_not_configured"))
  env <- c(DSJOBS_EXTERNAL_ID = external_id, DSJOBS_STEP_DIR = step_dir)
  out <- tryCatch(system2(status$command, c(status$args, external_id),
    stdout = TRUE, stderr = TRUE, env = .backend_env(env)), error = function(e)
      return(paste("FAILED", conditionMessage(e))))
  first <- trimws(out[1] %||% "")
  .backend_parse_external_status(first)
}

#' @keywords internal
.backend_parse_external_status <- function(line) {
  parts <- strsplit(trimws(line), "[[:space:]]+")[[1]]
  state <- toupper(parts[1] %||% "")
  if (is.na(state) || !nzchar(state)) state <- "FAILED"
  exit_code <- suppressWarnings(as.integer(parts[2] %||% NA_character_))
  if (state %in% c("SUCCEEDED", "SUCCESS", "COMPLETED", "DONE"))
    return(list(state = "succeeded", external_state = state,
                exit_code = ifelse(is.na(exit_code), 0L, exit_code)))
  if (state %in% c("PENDING", "RUNNING", "QUEUED", "SUBMITTED"))
    return(list(state = "running", external_state = state, exit_code = NA_integer_))
  list(state = "failed", external_state = state %||% "FAILED",
       exit_code = ifelse(is.na(exit_code), 1L, exit_code))
}

#' @keywords internal
.backend_cancel_step <- function(backend, external_id, settings = .dsjobs_settings()) {
  backend <- tolower(as.character(backend %||% "")[1])
  if (identical(backend, "slurm")) {
    scancel <- .backend_resolve_cmd(settings$slurm_scancel, "scancel")
    if (nzchar(scancel)) {
      tryCatch(system2(scancel, external_id, stdout = FALSE, stderr = FALSE),
               error = function(e) NULL)
    }
    return(invisible(TRUE))
  }
  if (identical(backend, "external")) {
    cancel <- .backend_command_parts(settings$external_cancel_cmd)
    if (nzchar(cancel$command)) {
      tryCatch(system2(cancel$command, c(cancel$args, external_id),
        stdout = FALSE, stderr = FALSE,
        env = .backend_env(c(DSJOBS_EXTERNAL_ID = external_id))), error = function(e) NULL)
    }
  }
  invisible(TRUE)
}

#' @keywords internal
.backend_env <- function(x) {
  nms <- names(x)
  keep <- !is.na(nms) & nzchar(nms)
  paste0(nms[keep], "=", as.character(x[keep]))
}

#' @keywords internal
.backend_write_step_script <- function(path, prepared) {
  env <- prepared$env_vars
  env_names <- names(env)
  keep <- !is.na(env_names) & nzchar(env_names)
  env <- env[keep]
  exports <- if (length(env) > 0) {
    vapply(names(env), function(nm) {
      paste0("export ", nm, "=", shQuote(as.character(env[[nm]])))
    }, character(1))
  } else character(0)
  command <- paste(c(shQuote(prepared$command), shQuote(prepared$args)),
    collapse = " ")
  lines <- c(
    "#!/usr/bin/env bash",
    "set -u",
    paste0("cd ", shQuote(dirname(path))),
    paste0("mkdir -p ", shQuote(prepared$output_dir)),
    exports,
    command,
    "status=$?",
    "printf '%s\\n' \"$status\" > exit_code",
    "exit \"$status\"")
  writeLines(lines, path)
  tryCatch(Sys.chmod(path, "0755"), error = function(e) NULL)
  path
}
