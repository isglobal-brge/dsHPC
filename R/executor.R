# Module: Step Execution
# Called by worker daemon or inline for session-only jobs.

.BLOCKED_ENV_VARS <- c("PATH", "HOME", "TMPDIR", "TMP", "TEMP", "USER", "SHELL",
  "LD_PRELOAD", "LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH",
  "DYLD_INSERT_LIBRARIES", "PYTHONPATH", "PYTHONSTARTUP",
  "BASH_ENV", "ENV", "CDPATH", "IFS")

# Output kinds that are safe to return to the client via hpcResultDS.
# Everything else stays server-side (loadable via hpcLoadOutputInternal).
.CLIENT_SAFE_KINDS <- c("summary", "aggregate_result", "job_metadata")

#' Execute the current step of a job
#' @keywords internal
.executor_run_step <- function(db, job_id, step_index, spec) {
  step <- spec$steps[[step_index]]
  step_dir <- .ensure_step_dir(job_id, step_index)
  input_dir <- .resolve_step_input(db, job_id, step_index, step, step_dir)

  step_hash <- NA_character_
  if (.step_cache_enabled_for_job(db, job_id, step)) {
    step_hash <- .step_cache_hash(step, input_dir)
    cached <- .step_cache_find(db, step_hash, current_job_id = job_id)
    if (!is.null(cached) &&
        .step_cache_apply(db, job_id, step_index, step_hash, cached, step_dir)) {
      .executor_advance(db, job_id)
      return(invisible(TRUE))
    }
    inflight <- .step_cache_inflight_find(db, step_hash,
      current_job_id = job_id)
    if (!is.null(inflight)) {
      waiting <- .step_cache_wait_for_inflight(db, job_id, step_index,
        step_hash, inflight)
      if (isTRUE(waiting)) return(invisible(TRUE))
    }
  }

  .store_update_step(db, job_id, step_index, state = "running",
    step_hash = step_hash,
    cache_hit = 0L,
    cache_source_job_id = NA_character_,
    cache_source_step_index = NA_integer_,
    error_message = NA_character_,
    started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))

  if (identical(step$plane, "session")) {
    .run_session_step(db, job_id, step_index, step, step_dir, input_dir)
  } else {
    .run_artifact_step(db, job_id, step_index, step, step_dir, input_dir)
  }
}

#' Advance after step success
#' @keywords internal
.executor_advance <- function(db, job_id) {
  job <- .store_get_job(db, job_id)
  if (is.null(job)) return()
  current <- as.integer(job$step_index)
  total <- as.integer(job$total_steps)

  if (current >= total) {
    .build_job_result(db, job_id)
    .store_update_job(db, job_id, state = "FINISHED", worker_pid = NA_integer_,
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .scheduler_release_leases(db, job_id)
    .db_log_event(db, job_id, "finished")
    return()
  }

  next_idx <- current + 1L
  .store_update_job(db, job_id, step_index = next_idx)
  spec <- .store_get_spec(db, job_id)
  .executor_run_step(db, job_id, next_idx, spec)
}

#' Kill a worker process
#' @keywords internal
.executor_kill <- function(db, job_id) {
  job <- .store_get_job(db, job_id)
  if (!is.null(job)) {
    step <- DBI::dbGetQuery(db,
      "SELECT external_backend, external_id FROM steps
       WHERE job_id = ? AND step_index = ?",
      params = list(job_id, as.integer(job$step_index %||% 0L)))
    if (nrow(step) > 0 && !is.na(step$external_id[1]) &&
        nzchar(step$external_id[1])) {
      .backend_cancel_step(step$external_backend[1], step$external_id[1])
      .scheduler_release_leases(db, job_id)
      .store_update_job(db, job_id, worker_pid = NA_integer_)
      return(invisible(TRUE))
    }
  }
  if (!is.null(job) && !is.na(job$worker_pid)) {
    pid <- as.integer(job$worker_pid)
    .terminate_pid(pid)
    step_dir <- file.path(.dshpc_home(), "artifacts", job_id,
                          sprintf("step_%03d",
                                  as.integer(job$step_index %||% 0L)))
    child_pid <- file.path(step_dir, "child.pid")
    if (file.exists(child_pid)) {
      child <- tryCatch(as.integer(readLines(child_pid, n = 1, warn = FALSE)),
                        error = function(e) NA_integer_)
      .terminate_pid(child)
    }
    .scheduler_release_leases(db, job_id)
    .store_update_job(db, job_id, worker_pid = NA_integer_)
  }
}

#' Build the final result object for a completed job
#'
#' DISCLOSURE RULE: Only outputs of kind "summary", "aggregate_result",
#' or "job_metadata" can have their values returned to the client via
#' hpcResultDS(). All other outputs (emit_value, artifact_file, etc.)
#' are listed by name/kind only -- their values stay server-side and
#' must be loaded via hpcLoadOutputInternal() or published as assets.
#'
#' @keywords internal
.build_job_result <- function(db, job_id) {
  home <- .dshpc_home()
  result_dir <- file.path(home, "artifacts", job_id, "result")
  if (.dshpc_path_is_symlink(result_dir)) {
    stop("Job result storage is unavailable.", call. = FALSE)
  }
  .dshpc_with_private_umask(dir.create(
    result_dir, recursive = TRUE, showWarnings = FALSE, mode = "0770"))
  if (!dir.exists(result_dir)) {
    stop("Job result storage is unavailable.", call. = FALSE)
  }
  .dshpc_validate_job_artifact_path(result_dir, job_id, check_tree = TRUE)
  tryCatch(Sys.chmod(result_dir, "0770", use_umask = FALSE),
           error = function(e) NULL)

  safe_result <- list(ready = TRUE)

  # Only summary/aggregate_result outputs cross the wire with values
  safe_outputs <- DBI::dbGetQuery(db,
    "SELECT name, kind, path_or_ref FROM outputs
     WHERE job_id = ?
       AND safe_for_client = 1
       AND kind IN ('summary', 'aggregate_result', 'job_metadata')",
    params = list(job_id))

  if (nrow(safe_outputs) > 0) {
    safe_result$summaries <- lapply(seq_len(nrow(safe_outputs)), function(i) {
      row <- safe_outputs[i, ]
      out <- list(name = row$name, kind = row$kind)
      path <- .dshpc_validate_job_artifact_path(row$path_or_ref, job_id,
        check_tree = TRUE)
      if (grepl("\\.rds$", path)) {
        value <- tryCatch(
          suppressWarnings(readRDS(path)),
          error = function(e) stop(
            "A client-safe result could not be read.", call. = FALSE))
        if (!.dshpc_client_safe_value(value, row$kind)) {
          stop("A client-safe result failed disclosure validation.",
            call. = FALSE)
        }
        out$value <- value
      }
      out
    })
  }

  # Unsafe output names can themselves contain record identifiers. Only
  # explicitly client-safe outputs are discoverable through the result API.
  safe_result$available_outputs <- if (nrow(safe_outputs) > 0) {
    lapply(seq_len(nrow(safe_outputs)), function(i) {
      list(name = safe_outputs$name[i], kind = safe_outputs$kind[i])
    })
  } else list()

  result_path <- file.path(result_dir, "result.rds")
  tmp <- tempfile(pattern = ".result-", tmpdir = result_dir,
                  fileext = ".rds")
  on.exit(unlink(tmp, force = TRUE), add = TRUE)
  .dshpc_with_private_umask(saveRDS(safe_result, tmp))
  tryCatch(Sys.chmod(tmp, "0660", use_umask = FALSE),
           error = function(e) NULL)
  if (!file.rename(tmp, result_path)) {
    stop("Job result storage is unavailable.", call. = FALSE)
  }
  safe_result
}

# --- Helpers ---

#' @keywords internal
.ensure_step_dir <- function(job_id, step_index) {
  home <- .dshpc_home()
  job_dir <- file.path(home, "artifacts", job_id)
  step_dir <- file.path(job_dir, sprintf("step_%03d", step_index))
  out_dir <- file.path(step_dir, "output")
  if (any(.dshpc_path_is_symlink(c(job_dir, step_dir, out_dir)))) {
    stop("Job artifact storage is unavailable.", call. = FALSE)
  }
  # Create with permissive mode so both worker and DS sessions can access
  .dshpc_with_private_umask(dir.create(
    job_dir, recursive = TRUE, showWarnings = FALSE, mode = "0770"))
  .dshpc_with_private_umask(dir.create(
    out_dir, recursive = TRUE, showWarnings = FALSE, mode = "0770"))
  .dshpc_validate_job_artifact_path(step_dir, job_id, check_tree = TRUE)
  # Ensure the dirs are writable even if already existed with restrictive perms
  tryCatch(Sys.chmod(job_dir, "0770", use_umask = FALSE),
           error = function(e) NULL)
  tryCatch(Sys.chmod(step_dir, "0770", use_umask = FALSE),
           error = function(e) NULL)
  tryCatch(Sys.chmod(out_dir, "0770", use_umask = FALSE),
           error = function(e) NULL)
  step_dir
}

#' @keywords internal
.resolve_step_input <- function(db, job_id, step_index, step_spec, step_dir = NULL) {
  if (!is.null(step_spec$inputs)) {
    refs <- .normalize_step_input_refs(step_spec$inputs)
    refs <- refs[!is.na(vapply(refs, `[[`, integer(1), "step"))]
    if (length(refs) == 1L) {
      path <- .step_output_path(db, job_id, refs[[1]]$step)
      if (!is.null(path)) return(path)
    }
    if (length(refs) > 1L && !is.null(step_dir)) {
      return(.stage_step_inputs(db, job_id, refs, step_dir))
    }
  }
  if (step_index > 1L) {
    path <- .step_output_path(db, job_id, step_index - 1L)
    if (!is.null(path)) return(path)
  }
  NULL
}

#' @keywords internal
.normalize_step_input_refs <- function(inputs) {
  if (is.null(inputs)) return(list())
  if (is.numeric(inputs)) {
    return(lapply(as.integer(inputs), function(i) list(step = i, name = paste0("step_", i))))
  }
  if (!is.list(inputs)) return(list())
  nm <- names(inputs) %||% rep("", length(inputs))
  out <- list()
  for (i in seq_along(inputs)) {
    item <- inputs[[i]]
    name <- nm[[i]]
    if (is.numeric(item) && length(item) == 1) {
      step <- as.integer(item)
      out[[length(out) + 1L]] <- list(
        step = step,
        name = if (nzchar(name)) name else paste0("step_", step))
    } else if (is.list(item)) {
      step <- item$step %||% item$step_index
      if (!is.null(step)) {
        out[[length(out) + 1L]] <- list(
          step = as.integer(step),
          name = item$name %||% if (nzchar(name)) name else paste0("step_", step),
          output = item$output %||% item$output_name %||% NULL,
          ref = item$ref %||% item$node %||% item$id %||% NULL)
      }
    }
  }
  out
}

#' @keywords internal
.step_output_path <- function(db, job_id, step_index) {
  row <- DBI::dbGetQuery(db,
    "SELECT output_ref FROM steps WHERE job_id = ? AND step_index = ?",
    params = list(job_id, as.integer(step_index)))
  if (nrow(row) == 0 || is.na(row$output_ref[1])) return(NULL)
  .dshpc_resolve_job_artifact_ref(row$output_ref[1], job_id,
    check_tree = TRUE)
}

#' @keywords internal
.stage_step_inputs <- function(db, job_id, refs, step_dir) {
  step_dir <- .dshpc_validate_job_artifact_path(step_dir, job_id,
    check_tree = TRUE)
  input_root <- file.path(step_dir, "input")
  if (dir.exists(input_root)) unlink(input_root, recursive = TRUE, force = TRUE)
  .dshpc_with_private_umask(dir.create(
    input_root, recursive = TRUE, showWarnings = FALSE, mode = "0770"))

  manifest <- list()
  used_names <- character(0)
  for (i in seq_along(refs)) {
    ref <- refs[[i]]
    source <- .step_output_path(db, job_id, ref$step)
    if (is.null(source)) next
    name <- .sanitize_input_name(ref$name %||% paste0("input_", i))
    if (name %in% used_names) name <- paste0(name, "_", i)
    used_names <- c(used_names, name)
    target <- file.path(input_root, name)
    # Copies keep the staged input self-contained. Symlinks could be swapped
    # after validation or point a later runner outside this job's artifacts.
    .copy_input_tree(source, target,
      target_root = file.path(.dshpc_home(), "artifacts"))
    manifest[[name]] <- list(
      step = as.integer(ref$step),
      ref = ref$ref %||% NA_character_,
      source = source,
      path = target
    )
  }
  manifest_path <- file.path(input_root, "inputs.json")
  .dshpc_with_private_umask(jsonlite::write_json(manifest, manifest_path,
    auto_unbox = TRUE, pretty = TRUE))
  tryCatch(Sys.chmod(manifest_path, "0660", use_umask = FALSE),
           error = function(e) NULL)
  input_root
}

#' @keywords internal
.copy_input_tree <- function(source, target, target_root = NULL) {
  .dshpc_assert_symlink_free_tree(source)
  if (!is.null(target_root)) {
    .dshpc_validate_storage_target(target, target_root)
  }
  if (.dshpc_path_is_symlink(target)) {
    stop("Artifact copy target failed validation.", call. = FALSE)
  }
  if (dir.exists(source)) {
    .dshpc_with_private_umask(dir.create(
      target, recursive = TRUE, showWarnings = FALSE, mode = "0770"))
    files <- list.files(source, all.files = TRUE, no.. = TRUE, full.names = TRUE)
    for (f in files) {
      copied <- .dshpc_with_private_umask(file.copy(f, target,
        recursive = TRUE, copy.date = TRUE, copy.mode = FALSE,
        overwrite = TRUE))
      if (!all(copied)) {
        stop("Artifact copy failed.", call. = FALSE)
      }
    }
  } else if (file.exists(source)) {
    .dshpc_with_private_umask(dir.create(dirname(target), recursive = TRUE,
      showWarnings = FALSE, mode = "0770"))
    copied <- .dshpc_with_private_umask(file.copy(source, target,
      overwrite = TRUE, copy.date = TRUE, copy.mode = FALSE))
    if (!isTRUE(copied)) stop("Artifact copy failed.", call. = FALSE)
  }
  .dshpc_assert_symlink_free_tree(target)
  invisible(target)
}

#' @keywords internal
.sanitize_input_name <- function(x) {
  x <- as.character(x %||% "input")[1]
  x <- gsub("[^A-Za-z0-9_.-]+", "_", x)
  x <- sub("^[._-]+", "", x)
  if (!nzchar(x)) "input" else x
}
