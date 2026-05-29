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
.step_cache_hash <- function(step, input_dir = NULL) {
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
    version = 1L,
    input_hash = .step_cache_hash_path(input_dir),
    step = step_for_hash,
    runner_config_hash = runner_config_hash
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
    size <- file.info(entries[[i]])$size
    file_hash <- digest::digest(entries[[i]], algo = "sha256", file = TRUE)
    paste("F", rel[[i]], size %||% NA_integer_, file_hash, sep = "\t")
  }, character(1))
  digest::digest(paste(parts, collapse = "\n"), algo = "sha256",
    serialize = FALSE)
}

#' @keywords internal
.step_cache_find <- function(db, step_hash, current_job_id = NULL) {
  if (is.null(step_hash) || is.na(step_hash) || !nzchar(step_hash)) return(NULL)
  rows <- DBI::dbGetQuery(db,
    "SELECT s.job_id, s.step_index, s.output_ref, j.finished_at
     FROM steps s
     JOIN jobs j ON j.job_id = s.job_id
     WHERE s.step_hash = ?
       AND s.state = 'done'
       AND s.output_ref IS NOT NULL
       AND j.state IN ('FINISHED', 'PUBLISHED')
     ORDER BY j.finished_at DESC, s.job_id DESC
     LIMIT 20",
    params = list(step_hash))
  if (nrow(rows) == 0L) return(NULL)

  home <- .dshpc_home()
  for (i in seq_len(nrow(rows))) {
    if (!is.null(current_job_id) && identical(rows$job_id[i], current_job_id)) {
      next
    }
    ref <- rows$output_ref[i]
    if (is.na(ref) || !nzchar(ref)) next
    path <- file.path(home, ref)
    if (file.exists(path)) return(as.list(rows[i, ]))
  }
  NULL
}

#' @keywords internal
.step_cache_apply <- function(db, job_id, step_index, step_hash, cached_step,
                              step_dir) {
  source_ref <- cached_step$output_ref
  source_path <- file.path(.dshpc_home(), source_ref)
  target_ref <- file.path("artifacts", job_id,
    sprintf("step_%03d", as.integer(step_index)), "output")
  target_path <- file.path(.dshpc_home(), target_ref)
  if (!file.exists(source_path)) return(FALSE)

  if (dir.exists(target_path) || file.exists(target_path)) {
    unlink(target_path, recursive = TRUE, force = TRUE)
  }
  .copy_input_tree(source_path, target_path)
  if (!file.exists(target_path)) return(FALSE)

  source_outputs <- DBI::dbGetQuery(db,
    "SELECT name, kind, path_or_ref, size_bytes, safe_for_client
     FROM outputs WHERE job_id = ? AND step_index = ?",
    params = list(cached_step$job_id, as.integer(cached_step$step_index)))
  if (nrow(source_outputs) > 0L) {
    for (i in seq_len(nrow(source_outputs))) {
      src <- source_outputs$path_or_ref[i]
      dst <- .step_cache_rewrite_output_path(src, source_path, target_path)
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
  path
}
