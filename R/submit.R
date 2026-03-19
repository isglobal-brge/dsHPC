# Module: Job Submission

#' Submit a job to all nodes
#'
#' Returns a \code{dsjobs_submission} with per-node job IDs.
#' The job_ids are the key for all subsequent operations
#' (status, result, load_output, cancel).
#'
#' @param conns DSI connections object.
#' @param job A dsjobs_job object.
#' @param symbol Character; symbol name for the job handle on server.
#' @return A \code{dsjobs_submission} with job_ids, symbol, and initial status.
#' @export
ds.jobs.submit <- function(conns, job, symbol = NULL) {
  if (!inherits(job, "dsjobs_job"))
    stop("'job' must be a dsjobs_job object.", call. = FALSE)
  if (is.null(symbol)) symbol <- .generate_symbol("dsJ")

  spec_encoded <- .ds_encode(as.list(job))
  DSI::datashield.assign.expr(conns, symbol = symbol,
    expr = call("jobSubmitDS", spec_encoded))

  # Get immediate status to capture job_ids
 results <- .ds_safe_aggregate(conns, expr = call("jobStatusDS", symbol))

  job_ids <- list()
  for (srv in names(results))
    if (!is.null(results[[srv]]$job_id))
      job_ids[[srv]] <- results[[srv]]$job_id

  submission <- list(
    symbol = symbol,
    job_ids = job_ids,
    label = job$label,
    servers = names(conns),
    submitted_at = Sys.time(),
    status = results
  )
  class(submission) <- c("dsjobs_submission", "list")
  submission
}

#' @export
print.dsjobs_submission <- function(x, ...) {
  cat("dsjobs_submission\n")
  cat("  Symbol:", x$symbol, "\n")
  if (!is.null(x$label)) cat("  Label:", x$label, "\n")
  cat("  Submitted:", format(x$submitted_at, "%Y-%m-%d %H:%M:%S"), "\n")
  for (srv in names(x$job_ids)) {
    st <- x$status[[srv]]$state %||% "?"
    cat("  ", srv, ":", x$job_ids[[srv]], "(", st, ")\n")
  }
  invisible(x)
}
