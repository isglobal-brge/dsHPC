# Module: Public API for Plugin Packages
#
# Functions that domain packages need to interact
# with dsHPC without accessing internal functions via :::.

#' Query jobs by tag
#'
#' Returns jobs matching a tag pattern. This is server-side API for domain
#' packages that need to reconcile their own state with dsHPC state; it is
#' intentionally not registered as a DataSHIELD method.
#'
#' @param tag_pattern Character; pattern to match against job tags (SQL LIKE).
#' @param states Character vector or NULL; optional job states to include.
#' @return data.frame with safe operational columns.
#' @export
query_jobs_by_tag <- function(tag_pattern, states = NULL) {
  .dshpc_require_trusted_server_caller()
  db <- .db_connect()
  on.exit(.db_close(db))

  where <- "tags LIKE ?"
  params <- list(tag_pattern)
  if (!is.null(states) && length(states) > 0) {
    states <- as.character(states)
    ph <- paste(rep("?", length(states)), collapse = ", ")
    where <- paste0(where, " AND state IN (", ph, ")")
    params <- c(params, as.list(states))
  }

  DBI::dbGetQuery(db, paste(
    "SELECT job_id, state, label, tags, error_message, spec_hash,",
    "submitted_at, started_at, finished_at, step_index, total_steps,",
    "retry_count",
    "FROM jobs WHERE", where,
    "ORDER BY submitted_at DESC"),
    params = params)
}

#' Query failed jobs by tag
#'
#' Returns jobs in FAILED state that match a tag pattern.
#' Used by domain packages to sync failure states back to their
#' own tracking systems (e.g. asset_items in dsImaging).
#'
#' @param tag_pattern Character; pattern to match against job tags (SQL LIKE).
#' @return data.frame with columns: job_id, tags, error_message.
#' @export
query_failed_jobs <- function(tag_pattern) {
  .dshpc_require_trusted_server_caller()
  rows <- query_jobs_by_tag(tag_pattern, states = "FAILED")
  return(rows[, c("job_id", "tags", "error_message"), drop = FALSE])
}

#' Cancel jobs by tag
#'
#' Cancels PENDING/RUNNING/CLONING jobs matching a tag pattern. This is
#' server-side API
#' for domain packages that own a higher-level workflow, such as a dsImaging
#' generation. It is intentionally not registered as a DataSHIELD method.
#'
#' @param tag_pattern Character; pattern to match against job tags (SQL LIKE).
#' @param admin_key Character/list/B64 payload accepted by `hpcAdminCancelDS`.
#' @param reason Character; cancellation reason stored on each job.
#' @param states Character vector; job states eligible for cancellation.
#' @return data.frame with `job_id`, previous `state`, and new `state`.
#' @export
cancel_jobs_by_tag <- function(tag_pattern, admin_key,
                               reason = "Cancelled by admin",
                               states = c("PENDING", "RUNNING", "CLONING")) {
  .dshpc_require_trusted_server_caller()
  .verify_admin_key(admin_key)

  rows <- query_jobs_by_tag(tag_pattern, states = states)
  if (nrow(rows) == 0) {
    return(data.frame(job_id = character(0), previous_state = character(0),
      state = character(0), stringsAsFactors = FALSE))
  }

  db <- .db_connect()
  on.exit(.db_close(db))
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  out <- vector("list", nrow(rows))

  for (i in seq_len(nrow(rows))) {
    job_id <- rows$job_id[i]
    job <- .store_get_job(db, job_id)
    if (is.null(job) || job$state %in% c("FINISHED", "PUBLISHED",
                                         "FAILED", "CANCELLED")) {
      out[[i]] <- data.frame(job_id = job_id,
        previous_state = if (is.null(job)) NA_character_ else job$state,
        state = if (is.null(job)) NA_character_ else job$state,
        stringsAsFactors = FALSE)
      next
    }

    .executor_kill(db, job_id)
    .scheduler_release_leases(db, job_id)
    .store_update_job(db, job_id, state = "CANCELLED",
      worker_pid = NA_integer_, error_message = reason %||% "Cancelled",
      finished_at = now)
    .db_log_event(db, job_id, "cancelled_by_tag",
      list(reason = reason %||% "Cancelled", tag_pattern = tag_pattern))
    out[[i]] <- data.frame(job_id = job_id, previous_state = job$state,
      state = "CANCELLED", stringsAsFactors = FALSE)
  }

  do.call(rbind, out)
}

#' Return a server-side output reference for a job
#'
#' This helper returns the output path without loading the data. It is for
#' trusted server-side domain packages, not for client disclosure.
#'
#' @param job_id_or_symbol Job ID or symbol.
#' @param output_name Output name.
#' @param required_label Required exact package label used as a secondary
#'   domain-boundary check.
#' @return Named list with job_id, name, kind, path, exists, and size_bytes.
#' @export
get_job_output_ref <- function(job_id_or_symbol, output_name,
                               required_label = NULL) {
  .dshpc_require_trusted_server_caller()
  required_label <- .dshpc_require_label_value(required_label,
    "dsHPC load operation requires a domain label (required_label). The caller must identify the domain it belongs to (typically the calling server-side package's name). This is a hard requirement; there is no opt-out.")
  .dshpc_require_trusted_server_caller(required_label)
  output_name <- .validate_identifier(output_name, "output_name")
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  if (!job$state %in% c("FINISHED", "PUBLISHED"))
    stop("Job not finished (state: ", job$state, ").", call. = FALSE)
  job_label <- job$label %||% ""
  if (!identical(job_label, required_label))
    stop("Job '", job_id, "' does not belong to '", required_label,
         "'. Access denied.", call. = FALSE)

  out <- DBI::dbGetQuery(db,
    "SELECT name, kind, path_or_ref, size_bytes FROM outputs
     WHERE job_id = ? AND name = ? ORDER BY id DESC LIMIT 1",
    params = list(job_id, output_name))
  if (nrow(out) == 0)
    stop("Output '", output_name, "' not found for job ", job_id, ".",
         call. = FALSE)
  path <- out$path_or_ref[1]
  path <- .dshpc_validate_job_artifact_path(path, job_id,
    check_tree = TRUE)
  list(job_id = job_id, name = out$name[1], kind = out$kind[1],
       path = path, exists = !is.na(path) && file.exists(path),
       size_bytes = .normalize_output_size_bytes(out$size_bytes[1]))
}

#' Count active jobs by tag
#'
#' Returns the number of PENDING, RUNNING, or CLONING jobs matching a tag
#' pattern.
#' Used by domain packages for backpressure / drip feed decisions.
#'
#' @param tag_pattern Character; pattern to match (SQL LIKE).
#' @return Integer; count of active jobs.
#' @export
count_active_jobs <- function(tag_pattern) {
  .dshpc_require_trusted_server_caller()
  db <- .db_connect()
  on.exit(.db_close(db))
  DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM jobs
     WHERE state IN ('PENDING','RUNNING','CLONING') AND tags LIKE ?",
    params = list(tag_pattern))$n
}

#' Get the current owner ID
#'
#' Resolves the node/session fallback owner. This is not an independently
#' authenticated analyst identity. Domain packages submitting on behalf of
#' users must authorize and inject their own stable owner value.
#'
#' @return Character; the owner identifier.
#' @export
get_owner_id <- function() {
  .dshpc_require_trusted_server_caller()
  .get_owner_id()
}
