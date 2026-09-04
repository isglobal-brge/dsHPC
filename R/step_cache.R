# Module: Step-Level Content Cache
#
# Complete job specifications are deduplicated at submit time. This module adds
# a finer cache for deterministic compute steps: resolved input content + the
# executable step definition address a completed step output from a previous job.

#' @keywords internal
.step_cache_enabled <- function(step) {
  enabled <- .dshpc_option("step_cache", TRUE)
  if (identical(enabled, FALSE) || identical(enabled, "false")) return(FALSE)
  if (identical(step$cache, FALSE) || identical(step$cacheable, FALSE)) return(FALSE)

  # Publishing and assign/aggregate operations may have session side effects.
  # Keep the generic cache on artifact-plane compute steps.
  identical(step$plane %||% .infer_step_plane(step$type), "artifact")
}

#' @keywords internal
.step_cache_enabled_for_job <- function(db, job_id, step) {
  if (!.step_cache_enabled(step)) return(FALSE)
  job <- .store_get_job(db, job_id)
  !is.null(job) && identical(job$visibility, "global")
}

#' @keywords internal
.step_cache_hash <- function(step, input_dir = NULL, execution_unit = NULL) {
  step_for_hash <- step[setdiff(names(step),
    c("inputs", "node_id", "cache", "cacheable"))]
  step_for_hash <- .canonicalise_spec(step_for_hash)
  runner_config_hash <- NULL
  if (!is.null(step$runner) && nzchar(as.character(step$runner)[1])) {
    runner_config <- tryCatch(.load_runner_config(step$runner),
      error = function(e) NULL)
    if (!is.null(runner_config)) {
      runner_config_hash <- digest::digest(jsonlite::toJSON(
        .canonicalise_spec(runner_config), auto_unbox = TRUE, null = "null"),
        algo = "sha256", serialize = FALSE)
    }
  }
  payload <- list(
    version = 2L,
    input_hash = .step_cache_hash_path(input_dir),
    step = step_for_hash,
    runner_config_hash = runner_config_hash,
    execution_unit = execution_unit
  )
  digest::digest(jsonlite::toJSON(payload, auto_unbox = TRUE, null = "null"),
    algo = "sha256", serialize = FALSE)
}

#' @keywords internal
.step_cache_hash_path <- function(path) {
  if (is.null(path) || length(path) == 0L || is.na(path) || !nzchar(path)) {
    return(digest::digest("dshpc:no-input", algo = "sha256", serialize = FALSE))
  }
  path <- path.expand(path)
  if (!file.exists(path)) {
    return(digest::digest(paste("dshpc:missing-input", path),
      algo = "sha256", serialize = FALSE))
  }
  .dshpc_assert_symlink_free_tree(path)
  if (!dir.exists(path)) {
    return(digest::digest(path, algo = "sha256", file = TRUE))
  }

  entries <- list.files(path, all.files = TRUE, no.. = TRUE, recursive = TRUE,
    full.names = TRUE, include.dirs = TRUE)
  entries <- entries[file.exists(entries)]
  if (length(entries) == 0L) {
    return(digest::digest("dshpc:empty-dir", algo = "sha256",
      serialize = FALSE))
  }
  root <- sub("/+$", "", gsub("\\\\", "/", path))
  entries_display <- gsub("\\\\", "/", entries)
  prefix <- paste0(root, "/")
  rel <- ifelse(startsWith(entries_display, prefix),
    substring(entries_display, nchar(prefix) + 1L),
    basename(entries_display))
  ord <- order(rel)
  rel <- rel[ord]
  entries <- entries[ord]
  parts <- vapply(seq_along(entries), function(i) {
    if (dir.exists(entries[[i]])) {
      return(paste("D", rel[[i]], sep = "\t"))
    }
    if (identical(rel[[i]], "inputs.json")) {
      size <- "canonical"
      file_hash <- .step_cache_hash_inputs_manifest(entries[[i]])
    } else {
      size <- file.info(entries[[i]])$size
      file_hash <- digest::digest(entries[[i]], algo = "sha256", file = TRUE)
    }
    paste("F", rel[[i]], size %||% NA_integer_, file_hash, sep = "\t")
  }, character(1))
  digest::digest(paste(parts, collapse = "\n"), algo = "sha256",
    serialize = FALSE)
}

#' @keywords internal
.step_cache_hash_inputs_manifest <- function(path) {
  manifest <- tryCatch(jsonlite::read_json(path, simplifyVector = FALSE),
    error = function(e) NULL)
  if (is.null(manifest) || !is.list(manifest)) {
    return(digest::digest(path, algo = "sha256", file = TRUE))
  }

  stable <- lapply(manifest, function(item) {
    if (!is.list(item)) return(item)
    item[intersect(c("step", "ref", "output"), names(item))]
  })
  stable <- .canonicalise_spec(stable)
  digest::digest(jsonlite::toJSON(stable, auto_unbox = TRUE, null = "null"),
    algo = "sha256", serialize = FALSE)
}

#' @keywords internal
.step_cache_find <- function(db, step_hash, current_job_id = NULL) {
  if (is.null(step_hash) || is.na(step_hash) || !nzchar(step_hash) ||
      is.null(current_job_id) || is.na(current_job_id) ||
      !nzchar(current_job_id)) return(NULL)
  current <- DBI::dbGetQuery(db,
    "SELECT label, visibility FROM jobs WHERE job_id = ?",
    params = list(current_job_id))
  if (nrow(current) != 1L || is.na(current$label[1]) ||
      !nzchar(current$label[1]) ||
      !identical(current$visibility[1], "global")) return(NULL)
  rows <- DBI::dbGetQuery(db,
    "SELECT s.job_id, s.step_index, s.output_ref, j.finished_at
     FROM steps s
     JOIN jobs j ON j.job_id = s.job_id
     WHERE s.step_hash = ?
       AND s.state = 'done'
       AND s.output_ref IS NOT NULL
       AND j.visibility = 'global'
       AND j.label = ?
       AND j.state IN ('RUNNING', 'FINISHED', 'PUBLISHED')
     ORDER BY COALESCE(s.finished_at, j.finished_at) DESC, s.job_id DESC
     LIMIT 20",
    params = list(step_hash, current$label[1]))
  if (nrow(rows) == 0L) return(NULL)

  for (i in seq_len(nrow(rows))) {
    if (!is.null(current_job_id) && identical(rows$job_id[i], current_job_id)) {
      next
    }
    ref <- rows$output_ref[i]
    if (is.na(ref) || !nzchar(ref)) next
    path <- tryCatch(.dshpc_resolve_job_artifact_ref(ref, rows$job_id[i],
      check_tree = TRUE), error = function(e) NULL)
    if (!is.null(path)) return(as.list(rows[i, ]))
  }
  NULL
}

#' @keywords internal
.step_cache_inflight_find <- function(db, step_hash, current_job_id = NULL) {
  if (is.null(step_hash) || is.na(step_hash) || !nzchar(step_hash) ||
      is.null(current_job_id) || is.na(current_job_id) ||
      !nzchar(current_job_id)) return(NULL)
  current <- DBI::dbGetQuery(db,
    "SELECT label, visibility FROM jobs WHERE job_id = ?",
    params = list(current_job_id))
  if (nrow(current) != 1L || is.na(current$label[1]) ||
      !nzchar(current$label[1]) ||
      !identical(current$visibility[1], "global")) return(NULL)
  rows <- DBI::dbGetQuery(db,
    "SELECT s.job_id, s.step_index, s.started_at
     FROM steps s
     JOIN jobs j ON j.job_id = s.job_id
     WHERE s.step_hash = ?
       AND s.state = 'running'
       AND j.visibility = 'global'
       AND j.label = ?
       AND j.state = 'RUNNING'
     ORDER BY s.started_at, s.job_id
     LIMIT 20",
    params = list(step_hash, current$label[1]))
  if (nrow(rows) == 0L) return(NULL)
  for (i in seq_len(nrow(rows))) {
    if (!is.null(current_job_id) && identical(rows$job_id[i], current_job_id)) {
      next
    }
    return(as.list(rows[i, ]))
  }
  NULL
}

#' @keywords internal
.step_cache_wait_for_inflight <- function(db, job_id, step_index, step_hash,
                                          inflight_step) {
  boundaries <- DBI::dbGetQuery(db,
    "SELECT job_id, visibility, label FROM jobs WHERE job_id IN (?, ?)",
    params = list(job_id, inflight_step$job_id))
  if (nrow(boundaries) != 2L ||
      !all(boundaries$visibility %in% "global") ||
      anyNA(boundaries$label) ||
      length(unique(boundaries$label)) != 1L) return(FALSE)
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  .store_update_step(db, job_id, step_index,
    state = "waiting_cache",
    step_hash = step_hash,
    cache_source_job_id = inflight_step$job_id,
    cache_source_step_index = as.integer(inflight_step$step_index),
    started_at = now,
    error_message = NA_character_)
  .scheduler_release_leases(db, job_id)
  .store_update_job(db, job_id, state = "PENDING",
    worker_pid = NA_integer_,
    error_message = paste("Waiting for equivalent step",
      inflight_step$job_id, inflight_step$step_index))
  .db_log_event(db, job_id, "step_cache_wait",
    list(step_index = as.integer(step_index),
         source_job_id = inflight_step$job_id,
         source_step_index = as.integer(inflight_step$step_index)))
  TRUE
}

#' @keywords internal
.step_cache_waiting_active <- function(db, job_id, step_index) {
  row <- DBI::dbGetQuery(db,
    "SELECT state, step_hash, cache_source_job_id, cache_source_step_index
     FROM steps WHERE job_id = ? AND step_index = ?",
    params = list(job_id, as.integer(step_index)))
  if (nrow(row) == 0L || !identical(row$state[1], "waiting_cache")) {
    return(FALSE)
  }
  source_job <- row$cache_source_job_id[1]
  source_step <- as.integer(row$cache_source_step_index[1])
  if (is.na(source_job) || !nzchar(source_job) ||
      is.na(source_step) || source_step < 1L) {
    return(FALSE)
  }
  source <- DBI::dbGetQuery(db,
    "SELECT s.state AS step_state, j.state AS job_state
     FROM steps s
     JOIN jobs j ON j.job_id = s.job_id
     WHERE s.job_id = ? AND s.step_index = ?",
    params = list(source_job, source_step))
  boundaries <- DBI::dbGetQuery(db,
    "SELECT job_id, visibility, label FROM jobs WHERE job_id IN (?, ?)",
    params = list(job_id, source_job))
  nrow(boundaries) == 2L &&
    all(boundaries$visibility %in% "global") &&
    !anyNA(boundaries$label) &&
    length(unique(boundaries$label)) == 1L &&
    nrow(source) > 0L &&
    identical(source$step_state[1], "running") &&
    identical(source$job_state[1], "RUNNING")
}

#' @keywords internal
.step_cache_apply <- function(db, job_id, step_index, step_hash, cached_step,
                              step_dir) {
  visibility <- DBI::dbGetQuery(db,
    "SELECT job_id, visibility, label FROM jobs WHERE job_id IN (?, ?)",
    params = list(job_id, cached_step$job_id))
  if (nrow(visibility) != 2L ||
      !all(visibility$visibility %in% "global") ||
      anyNA(visibility$label) ||
      length(unique(visibility$label)) != 1L) return(FALSE)

  source_ref <- cached_step$output_ref
  source_path <- tryCatch(.dshpc_resolve_job_artifact_ref(
    source_ref, cached_step$job_id, check_tree = TRUE),
    error = function(e) NULL)
  if (is.null(source_path)) return(FALSE)
  target_ref <- file.path("artifacts", job_id,
    sprintf("step_%03d", as.integer(step_index)), "output")
  target_path <- file.path(.dshpc_home(), target_ref)
  target_parent <- tryCatch(.dshpc_validate_job_artifact_path(
    dirname(target_path), job_id, check_tree = TRUE), error = function(e) NULL)
  if (is.null(target_parent)) return(FALSE)

  source_outputs <- DBI::dbGetQuery(db,
    "SELECT name, kind, path_or_ref, size_bytes, safe_for_client
     FROM outputs WHERE job_id = ? AND step_index = ?",
    params = list(cached_step$job_id, as.integer(cached_step$step_index)))
  mapped_outputs <- character(nrow(source_outputs))
  if (nrow(source_outputs) > 0L) {
    prefix <- paste0(source_path, "/")
    for (i in seq_len(nrow(source_outputs))) {
      src <- tryCatch(.dshpc_validate_job_artifact_path(
        source_outputs$path_or_ref[i], cached_step$job_id,
        check_tree = TRUE), error = function(e) NULL)
      if (is.null(src) ||
          (!identical(src, source_path) && !startsWith(src, prefix))) {
        return(FALSE)
      }
      mapped_outputs[i] <- .step_cache_rewrite_output_path(
        src, source_path, target_path)
      if (is.na(mapped_outputs[i])) return(FALSE)
    }
  }

  if (dir.exists(target_path) || file.exists(target_path)) {
    if (.dshpc_path_is_symlink(target_path)) return(FALSE)
    unlink(target_path, recursive = TRUE, force = TRUE)
  }
  .copy_input_tree(source_path, target_path,
    target_root = file.path(.dshpc_home(), "artifacts"))
  target_path <- tryCatch(.dshpc_validate_job_artifact_path(
    target_path, job_id, check_tree = TRUE), error = function(e) NULL)
  if (is.null(target_path)) return(FALSE)

  if (nrow(source_outputs) > 0L) {
    for (i in seq_len(nrow(source_outputs))) {
      dst <- mapped_outputs[i]
      size <- if (!is.na(dst) && file.exists(dst)) file.info(dst)$size
              else source_outputs$size_bytes[i]
      .db_register_output(db, job_id, step_index, source_outputs$name[i],
        source_outputs$kind[i], dst, size_bytes = size,
        safe_for_client = as.logical(source_outputs$safe_for_client[i]))
    }
  }

  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  .store_update_step(db, job_id, step_index, state = "done",
    output_ref = target_ref,
    step_hash = step_hash,
    cache_hit = 1L,
    cache_source_job_id = cached_step$job_id,
    cache_source_step_index = as.integer(cached_step$step_index),
    started_at = now,
    finished_at = now)
  .db_log_event(db, job_id, "step_cached",
    list(step_index = as.integer(step_index),
         source_job_id = cached_step$job_id,
         source_step_index = as.integer(cached_step$step_index)))
  TRUE
}

#' @keywords internal
.step_cache_rewrite_output_path <- function(path, source_root, target_root) {
  if (is.na(path) || !nzchar(path)) return(path)
  src_norm <- normalizePath(source_root, winslash = "/", mustWork = FALSE)
  path_norm <- normalizePath(path, winslash = "/", mustWork = FALSE)
  if (identical(path_norm, src_norm)) return(target_root)
  prefix <- paste0(src_norm, "/")
  if (startsWith(path_norm, prefix)) {
    rel <- substring(path_norm, nchar(prefix) + 1L)
    return(file.path(target_root, rel))
  }
  NA_character_
}
