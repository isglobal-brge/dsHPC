# Module: Public API for Plugin Packages
#
# Functions that domain packages (dsRadiomics, etc.) need to interact
# with dsJobs without accessing internal functions via :::.

#' Query jobs by tag
#'
#' Returns jobs matching a tag pattern. This is server-side API for domain
#' packages that need to reconcile their own state with dsJobs state; it is
#' intentionally not registered as a DataSHIELD method.
#'
#' @param tag_pattern Character; pattern to match against job tags (SQL LIKE).
#' @param states Character vector or NULL; optional job states to include.
#' @return data.frame with safe operational columns.
#' @export
query_jobs_by_tag <- function(tag_pattern, states = NULL) {
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
    "submitted_at, started_at, finished_at",
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
  rows <- query_jobs_by_tag(tag_pattern, states = "FAILED")
  return(rows[, c("job_id", "tags", "error_message"), drop = FALSE])
}

#' Return a server-side output reference for a job
#'
#' This helper returns the output path without loading the data. It is for
#' trusted server-side domain packages, not for client disclosure.
#'
#' @param job_id_or_symbol Job ID or symbol.
#' @param output_name Output name.
#' @param required_label Optional label substring used as package ownership check.
#' @return Named list with job_id, name, kind, path, exists, and size_bytes.
#' @export
get_job_output_ref <- function(job_id_or_symbol, output_name,
                               required_label = NULL) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  if (!job$state %in% c("FINISHED", "PUBLISHED"))
    stop("Job not finished (state: ", job$state, ").", call. = FALSE)
  if (!is.null(required_label)) {
    job_label <- job$label %||% ""
    if (!grepl(required_label, job_label, fixed = TRUE))
      stop("Job '", job_id, "' does not belong to '", required_label,
           "'. Access denied.", call. = FALSE)
  }

  out <- DBI::dbGetQuery(db,
    "SELECT name, kind, path_or_ref, size_bytes FROM outputs
     WHERE job_id = ? AND name = ? ORDER BY id DESC LIMIT 1",
    params = list(job_id, output_name))
  if (nrow(out) == 0)
    stop("Output '", output_name, "' not found for job ", job_id, ".",
         call. = FALSE)
  path <- out$path_or_ref[1]
  list(job_id = job_id, name = out$name[1], kind = out$kind[1],
       path = path, exists = !is.na(path) && file.exists(path),
       size_bytes = as.integer(out$size_bytes[1]))
}

#' Count active jobs by tag
#'
#' Returns the number of PENDING or RUNNING jobs matching a tag pattern.
#' Used by domain packages for backpressure / drip feed decisions.
#'
#' @param tag_pattern Character; pattern to match (SQL LIKE).
#' @return Integer; count of active jobs.
#' @export
count_active_jobs <- function(tag_pattern) {
  db <- .db_connect()
  on.exit(.db_close(db))
  DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM jobs
     WHERE state IN ('PENDING','RUNNING') AND tags LIKE ?",
    params = list(tag_pattern))$n
}

#' Get the current owner ID
#'
#' Resolves the owner identity from the session context.
#' Used by domain packages when submitting jobs on behalf of users.
#'
#' @return Character; the owner identifier.
#' @export
get_owner_id <- function() {
  .get_owner_id()
}
