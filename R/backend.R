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
    commands = list(),
    container = .backend_container_status(settings),
    path_mappings = .backend_path_mappings(settings))

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
.backend_container_status <- function(settings = .dsjobs_settings()) {
  runtime <- .backend_resolve_container_runtime(settings$container_runtime)
  list(runtime = runtime$name, command = runtime$command,
       available = nzchar(runtime$command),
       pull = as.character(settings$container_pull %||% "missing"),
       network = as.character(settings$container_network %||% "none"))
}

#' @keywords internal
.backend_resolve_container_runtime <- function(value = "auto") {
  value <- tolower(as.character(value %||% "auto")[1])
  if (!nzchar(value) || identical(value, "none") || identical(value, "false"))
    return(list(name = "none", command = ""))
  candidates <- if (identical(value, "auto")) c("docker", "podman", "apptainer", "singularity") else value
  for (candidate in candidates) {
    cmd <- .backend_resolve_cmd(candidate, candidate)
    if (nzchar(cmd)) return(list(name = candidate, command = cmd))
  }
  list(name = value, command = "")
}

#' @keywords internal
.backend_runner_container <- function(runner_config, settings = .dsjobs_settings()) {
  container <- runner_config$container %||% list()
  image <- container$image %||% runner_config$image %||%
    runner_config$container_image %||% ""
  image <- as.character(image %||% "")[1]
  if (!nzchar(image)) return(NULL)
  runtime_value <- container$runtime %||% settings$container_runtime
  runtime <- .backend_resolve_container_runtime(runtime_value)
  list(
    image = image,
    runtime = runtime,
    command = container$command %||% runner_config$container_command %||% NULL,
    args_template = container$args_template %||% runner_config$container_args_template %||% NULL,
    pull = container$pull %||% settings$container_pull,
    workdir = container$workdir %||% NULL,
    extra_args = c(as.character(settings$container_extra_args %||% character(0)),
                   as.character(container$extra_args %||% character(0))))
}

#' @keywords internal
.backend_gpu_request <- function(profile, settings = .dsjobs_settings()) {
  required <- as.integer(profile$gpus %||% 0L)
  optional <- as.integer(profile$optional_gpus %||% 0L)
  policy <- tolower(as.character(settings$backend_request_optional_gpus %||% "auto")[1])
  backend_count_raw <- settings$backend_gpu_count %||% "auto"
  backend_count <- suppressWarnings(as.integer(backend_count_raw))
  requested <- required
  if (requested <= 0L && optional > 0L) {
    if (policy %in% c("always", "true", "yes", "1")) {
      requested <- optional
    } else if (policy %in% c("never", "false", "no", "0")) {
      requested <- 0L
    } else if (is.finite(backend_count) && backend_count > 0L) {
      requested <- min(optional, backend_count)
    } else if (isTRUE(settings$slurm_request_optional_gpus)) {
      requested <- optional
    }
  }
  list(required = required, optional = optional, requested = requested,
       policy = policy, backend_gpu_count = backend_count_raw)
}

#' @keywords internal
.backend_path_mappings <- function(settings = .dsjobs_settings()) {
  value <- settings$backend_path_mappings %||% list()
  if (is.character(value) && length(value) > 0 && !is.null(names(value)) &&
      any(nzchar(names(value)))) {
    value <- lapply(names(value), function(local) {
      list(local = local, backend = unname(value[[local]]))
    })
  } else if (is.character(value) && length(value) == 1 && nzchar(value)) {
    if (startsWith(trimws(value), "[") || startsWith(trimws(value), "{")) {
      value <- tryCatch(jsonlite::fromJSON(value, simplifyVector = FALSE),
        error = function(e) value)
    } else {
      parts <- strsplit(value, ";", fixed = TRUE)[[1]]
      value <- lapply(parts[nzchar(parts)], function(x) {
        pair <- strsplit(x, "=", fixed = TRUE)[[1]]
        list(local = pair[1] %||% "", backend = pair[2] %||% "")
      })
    }
  }

  rows <- list()
  if (is.list(value) && length(value) > 0) {
    for (item in value) {
      if (is.null(item)) next
      if (is.character(item) && length(item) >= 2) {
        local <- item[[1]]
        backend <- item[[2]]
      } else {
        local <- item$local %||% item$from %||% item$rock %||% item$container
        backend <- item$backend %||% item$to %||% item$host %||% item$external
      }
      local <- .backend_trim_path(as.character(local %||% ""))
      backend <- .backend_trim_path(as.character(backend %||% ""))
      if (nzchar(local) && nzchar(backend)) {
        rows[[length(rows) + 1L]] <- data.frame(local = local, backend = backend,
          stringsAsFactors = FALSE)
      }
    }
  }
  if (length(rows) == 0)
    return(data.frame(local = character(0), backend = character(0),
      stringsAsFactors = FALSE))
  out <- do.call(rbind, rows)
  out[order(nchar(out$local), decreasing = TRUE), , drop = FALSE]
}

#' @keywords internal
.backend_trim_path <- function(path) {
  path <- as.character(path %||% "")[1]
  if (!nzchar(path)) return("")
  if (identical(path, "/")) return("/")
  sub("/+$", "", path)
}

#' @keywords internal
.backend_map_path <- function(path, direction = c("local_to_backend", "backend_to_local"),
                              settings = .dsjobs_settings()) {
  direction <- match.arg(direction)
  if (is.null(path) || length(path) == 0) return(path)
  maps <- .backend_path_mappings(settings)
  if (nrow(maps) == 0) return(path)
  vapply(as.character(path), function(one) {
    if (is.na(one) || !nzchar(one)) return(one)
    for (i in seq_len(nrow(maps))) {
      from <- if (identical(direction, "local_to_backend")) maps$local[i] else maps$backend[i]
      to <- if (identical(direction, "local_to_backend")) maps$backend[i] else maps$local[i]
      if (identical(one, from) || startsWith(one, paste0(from, "/"))) {
        return(paste0(to, substring(one, nchar(from) + 1L)))
      }
    }
    one
  }, character(1), USE.NAMES = FALSE)
}

#' @keywords internal
.backend_map_text <- function(x, direction = c("local_to_backend", "backend_to_local"),
                              settings = .dsjobs_settings()) {
  direction <- match.arg(direction)
  if (is.null(x) || length(x) == 0) return(x)
  maps <- .backend_path_mappings(settings)
  if (nrow(maps) == 0) return(x)
  out <- as.character(x)
  for (i in seq_len(nrow(maps))) {
    from <- if (identical(direction, "local_to_backend")) maps$local[i] else maps$backend[i]
    to <- if (identical(direction, "local_to_backend")) maps$backend[i] else maps$local[i]
    out <- gsub(from, to, out, fixed = TRUE)
  }
  names(out) <- names(x)
  out
}

#' @keywords internal
.backend_map_prepared <- function(prepared, settings = .dsjobs_settings()) {
  mapped <- prepared
  container <- .backend_runner_container(prepared$runner_config, settings)
  if (!is.null(container)) {
    command <- container$command %||% prepared$raw_command %||% prepared$command
    mapped$command <- as.character(command)[1]
    if (!is.null(container$args_template)) {
      cfg <- prepared$runner_config
      cfg$args_template <- container$args_template
      mapped$args <- .build_runner_args(cfg, prepared$step,
        prepared$step_dir, prepared$input_dir)
    }
  }
  mapped$command <- .backend_map_text(mapped$command, "local_to_backend", settings)
  mapped$args <- .backend_map_text(mapped$args, "local_to_backend", settings)
  mapped$env_vars <- .backend_map_text(prepared$env_vars, "local_to_backend", settings)
  mapped$output_dir <- .backend_map_path(prepared$output_dir, "local_to_backend", settings)
  mapped$step_dir <- .backend_map_path(prepared$step_dir, "local_to_backend", settings)
  mapped$script_path <- .backend_map_path(prepared$script_path, "local_to_backend", settings)
  mapped$local_step_dir <- prepared$step_dir
  mapped$local_output_dir <- prepared$output_dir
  mapped$local_script_path <- prepared$script_path
  profile <- .scheduler_runner_profile(prepared$step$runner, settings)
  gpu_request <- .backend_gpu_request(profile, settings)
  mapped$env_vars <- c(mapped$env_vars,
    DSJOBS_BACKEND_STEP_DIR = mapped$step_dir,
    DSJOBS_BACKEND_OUTPUT_DIR = mapped$output_dir,
    DSJOBS_BACKEND_STEP_SCRIPT = mapped$script_path,
    DSJOBS_LOCAL_STEP_DIR = prepared$step_dir,
    DSJOBS_LOCAL_OUTPUT_DIR = prepared$output_dir,
    DSJOBS_LOCAL_STEP_SCRIPT = prepared$script_path,
    DSJOBS_GPUS = as.character(gpu_request$requested %||% 0L),
    DSJOBS_GPUS_REQUIRED = as.character(gpu_request$required %||% 0L),
    DSJOBS_GPUS_OPTIONAL = as.character(gpu_request$optional %||% 0L),
    DSJOBS_GPUS_REQUESTED = as.character(gpu_request$requested %||% 0L),
    DSJOBS_GPU_POLICY = gpu_request$policy %||% "auto",
    DSJOBS_BACKEND_GPU_COUNT = as.character(gpu_request$backend_gpu_count %||% "auto"))
  mapped
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
  local_script <- file.path(step_dir, "run_step.sh")
  prepared$script_path <- local_script
  backend_prepared <- .backend_map_prepared(prepared, settings)

  if (identical(backend, "slurm")) {
    external_id <- .backend_submit_slurm(job_id, step_index, step, step_dir,
      backend_prepared, settings)
  } else if (identical(backend, "external")) {
    external_id <- .backend_submit_external(job_id, step_index, step, step_dir,
      backend_prepared, settings)
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
    paste0("--output=", file.path(prepared$step_dir, "stdout.log")),
    paste0("--error=", file.path(prepared$step_dir, "stderr.log")),
    paste0("--chdir=", prepared$step_dir),
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

  gpu_request <- .backend_gpu_request(profile, settings)
  gpu_n <- as.integer(gpu_request$requested %||% 0L)
  if (gpu_n > 0L) args <- c(args, paste0("--gres=gpu:", gpu_n))

  extra <- as.character(settings$slurm_extra_args %||% character(0))
  if (length(extra) > 0) args <- c(args, extra[nzchar(extra)])
  args <- c(args, prepared$script_path)

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
  gpu_request <- .backend_gpu_request(profile, settings)
  env <- c(
    DSJOBS_JOB_ID = job_id,
    DSJOBS_STEP_INDEX = as.character(step_index),
    DSJOBS_RUNNER = step$runner %||% "",
    DSJOBS_STEP_DIR = prepared$step_dir,
    DSJOBS_OUTPUT_DIR = prepared$output_dir,
    DSJOBS_STEP_SCRIPT = prepared$script_path,
    DSJOBS_BACKEND_STEP_DIR = prepared$step_dir,
    DSJOBS_BACKEND_OUTPUT_DIR = prepared$output_dir,
    DSJOBS_BACKEND_STEP_SCRIPT = prepared$script_path,
    DSJOBS_LOCAL_STEP_DIR = prepared$local_step_dir %||% step_dir,
    DSJOBS_LOCAL_OUTPUT_DIR = prepared$local_output_dir %||% file.path(step_dir, "output"),
    DSJOBS_LOCAL_STEP_SCRIPT = prepared$local_script_path %||% script,
    DSJOBS_MEMORY_MB = as.character(profile$memory_mb %||% 0L),
    DSJOBS_CPU_SLOTS = as.character(profile$cpu_slots %||% 0L),
    DSJOBS_GPUS = as.character(gpu_request$requested %||% 0L),
    DSJOBS_GPUS_REQUIRED = as.character(gpu_request$required %||% 0L),
    DSJOBS_GPUS_OPTIONAL = as.character(gpu_request$optional %||% 0L),
    DSJOBS_GPUS_REQUESTED = as.character(gpu_request$requested %||% 0L),
    DSJOBS_GPU_POLICY = gpu_request$policy %||% "auto",
    DSJOBS_BACKEND_GPU_COUNT = as.character(gpu_request$backend_gpu_count %||% "auto"))
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
  backend_step_dir <- .backend_map_path(step_dir, "local_to_backend", settings)
  env <- c(DSJOBS_EXTERNAL_ID = external_id,
    DSJOBS_STEP_DIR = backend_step_dir,
    DSJOBS_LOCAL_STEP_DIR = step_dir)
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
.backend_write_step_script <- function(path, prepared, settings = .dsjobs_settings()) {
  env <- prepared$env_vars
  env_names <- names(env)
  keep <- !is.na(env_names) & nzchar(env_names)
  env <- env[keep]
  exports <- if (length(env) > 0) {
    vapply(names(env), function(nm) {
      paste0("export ", nm, "=", shQuote(as.character(env[[nm]])))
    }, character(1))
  } else character(0)
  command <- .backend_step_command(prepared, settings)
  lines <- c(
    "#!/usr/bin/env bash",
    "set -u",
    paste0("cd ", shQuote(prepared$step_dir %||% dirname(path))),
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

#' @keywords internal
.backend_step_command <- function(prepared, settings = .dsjobs_settings()) {
  container <- .backend_runner_container(prepared$runner_config, settings)
  if (is.null(container))
    return(paste(c(shQuote(prepared$command), shQuote(prepared$args)),
      collapse = " "))

  runtime <- container$runtime
  if (!nzchar(runtime$command)) {
    return(paste0("echo ", shQuote(paste0("Container runtime '",
      runtime$name, "' is not available")), " >&2; exit 127"))
  }

  if (runtime$name %in% c("docker", "podman"))
    return(.backend_docker_like_command(runtime, container, prepared, settings))
  if (runtime$name %in% c("apptainer", "singularity"))
    return(.backend_apptainer_command(runtime, container, prepared, settings))

  paste0("echo ", shQuote(paste0("Unsupported container runtime: ",
    runtime$name)), " >&2; exit 127")
}

#' @keywords internal
.backend_container_mount <- function(settings = .dsjobs_settings()) {
  local_home <- .dsjobs_home(must_exist = FALSE)
  backend_home <- .backend_map_path(local_home, "local_to_backend", settings)
  list(source = backend_home, target = backend_home)
}

#' @keywords internal
.backend_container_env_args <- function(prepared, flag = "--env") {
  env <- prepared$env_vars
  nms <- names(env)
  keep <- !is.na(nms) & nzchar(nms)
  nms <- unique(nms[keep])
  if (length(nms) == 0) return(character(0))
  as.vector(rbind(flag, nms))
}

#' @keywords internal
.backend_container_gpu_lines <- function(runtime_name) {
  if (identical(runtime_name, "docker")) {
    return(c(
      "gpu_args=\"\"",
      "if [ \"${DSJOBS_GPUS_REQUIRED:-0}\" -gt 0 ]; then",
      "  gpu_args=\"--gpus ${DSJOBS_GPUS_REQUIRED}\"",
      "elif [ \"${DSJOBS_GPUS_REQUESTED:-0}\" -gt 0 ] && { command -v nvidia-smi >/dev/null 2>&1 || [ \"${DSJOBS_FORCE_CONTAINER_GPU:-0}\" = \"1\" ]; }; then",
      "  gpu_args=\"--gpus ${DSJOBS_GPUS_REQUESTED}\"",
      "fi"))
  }
  if (runtime_name %in% c("apptainer", "singularity")) {
    return(c(
      "gpu_args=\"\"",
      "if [ \"${DSJOBS_GPUS_REQUIRED:-0}\" -gt 0 ]; then",
      "  gpu_args=\"--nv\"",
      "elif [ \"${DSJOBS_GPUS_REQUESTED:-0}\" -gt 0 ] && { command -v nvidia-smi >/dev/null 2>&1 || [ \"${DSJOBS_FORCE_CONTAINER_GPU:-0}\" = \"1\" ]; }; then",
      "  gpu_args=\"--nv\"",
      "fi"))
  }
  "gpu_args=\"\""
}

#' @keywords internal
.backend_docker_like_command <- function(runtime, container, prepared,
                                         settings = .dsjobs_settings()) {
  mount <- .backend_container_mount(settings)
  workdir <- container$workdir %||% prepared$step_dir
  network <- as.character(settings$container_network %||% "none")[1]
  pull <- tolower(as.character(container$pull %||% "missing")[1])
  extra <- as.character(container$extra_args %||% character(0))
  extra <- extra[nzchar(extra)]
  env_args <- .backend_container_env_args(prepared, "--env")
  user_args <- character(0)
  if (isTRUE(settings$container_run_as_current_user)) {
    ids <- .backend_current_uid_gid()
    if (!is.null(ids)) user_args <- c("--user", paste(ids$uid, ids$gid, sep = ":"))
  }
  run_args <- c("run", "--rm", "$gpu_args")
  if (nzchar(network)) run_args <- c(run_args, "--network", network)
  run_args <- c(run_args, user_args, extra,
    "-v", paste0(mount$source, ":", mount$target),
    "-w", workdir,
    env_args,
    container$image,
    prepared$command,
    prepared$args)
  pull_lines <- character(0)
  if (identical(pull, "always")) {
    pull_lines <- paste(shQuote(runtime$command), "pull", shQuote(container$image))
  } else if (identical(pull, "missing")) {
    pull_lines <- paste0("if ! ", shQuote(runtime$command), " image inspect ",
      shQuote(container$image), " >/dev/null 2>&1; then ",
      shQuote(runtime$command), " pull ", shQuote(container$image), "; fi")
  }
  paste(c(pull_lines, .backend_container_gpu_lines(runtime$name),
    .backend_shell_join(c(runtime$command, run_args), raw = "$gpu_args")),
    collapse = "\n")
}

#' @keywords internal
.backend_current_uid_gid <- function() {
  id <- Sys.which("id")
  if (!nzchar(id)) return(NULL)
  uid <- suppressWarnings(as.integer(system2(id, "-u", stdout = TRUE, stderr = FALSE)[1]))
  gid <- suppressWarnings(as.integer(system2(id, "-g", stdout = TRUE, stderr = FALSE)[1]))
  if (!is.finite(uid) || !is.finite(gid)) return(NULL)
  list(uid = uid, gid = gid)
}

#' @keywords internal
.backend_apptainer_command <- function(runtime, container, prepared,
                                       settings = .dsjobs_settings()) {
  mount <- .backend_container_mount(settings)
  workdir <- container$workdir %||% prepared$step_dir
  extra <- as.character(container$extra_args %||% character(0))
  extra <- extra[nzchar(extra)]
  exec_args <- c("exec", "$gpu_args", extra,
    "--bind", paste0(mount$source, ":", mount$target),
    "--pwd", workdir,
    container$image,
    prepared$command,
    prepared$args)
  paste(c(.backend_container_gpu_lines(runtime$name),
    .backend_shell_join(c(runtime$command, exec_args), raw = "$gpu_args")),
    collapse = "\n")
}

#' @keywords internal
.backend_shell_join <- function(args, raw = character(0)) {
  paste(vapply(as.character(args), function(x) {
    if (x %in% raw) x else shQuote(x)
  }, character(1)), collapse = " ")
}
