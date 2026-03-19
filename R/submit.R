# Module: Job Submission

#' Submit a job to all nodes
#'
#' Returns a \code{dsjobs_submission} object containing the per-node
#' job IDs and server symbol. This object can be saved to disk and
#' used to recover the job in a future R session.
#'
#' @param conns DSI connections object.
#' @param job A dsjobs_job object.
#' @param symbol Character; symbol name for the job handle on server.
#' @param save Logical; if TRUE, persists the submission locally for
#'   recovery across sessions (default TRUE).
#' @return A \code{dsjobs_submission} object with job IDs and symbol.
#' @export
ds.jobs.submit <- function(conns, job, symbol = NULL, save = TRUE) {
  if (!inherits(job, "dsjobs_job"))
    stop("'job' must be a dsjobs_job object.", call. = FALSE)
  if (is.null(symbol)) symbol <- .generate_symbol("dsJ")

  spec_encoded <- .ds_encode(as.list(job))

  DSI::datashield.assign.expr(
    conns, symbol = symbol,
    expr = call("jobSubmitDS", spec_encoded))

  .dsjobs_client_env$.last_symbol <- symbol
  .dsjobs_client_env$.conns <- conns

  # Get immediate status to capture job_ids
  results <- .ds_safe_aggregate(conns, expr = call("jobStatusDS", symbol))

  # Extract per-node job IDs
  job_ids <- list()
  for (srv in names(results)) {
    if (!is.null(results[[srv]]$job_id))
      job_ids[[srv]] <- results[[srv]]$job_id
  }

  # Build submission receipt
  submission <- list(
    symbol = symbol,
    job_ids = job_ids,
    servers = names(conns),
    submitted_at = Sys.time(),
    status = results
  )
  class(submission) <- c("dsjobs_submission", "list")

  # Persist locally for cross-session recovery
  if (isTRUE(save)) .save_submission(submission)

  submission
}

#' Print a dsjobs_submission
#' @export
print.dsjobs_submission <- function(x, ...) {
  cat("dsjobs_submission\n")
  cat("  Symbol:", x$symbol, "\n")
  cat("  Submitted:", format(x$submitted_at, "%Y-%m-%d %H:%M:%S"), "\n")
  for (srv in names(x$job_ids)) {
    st <- x$status[[srv]]$state %||% "?"
    cat("  ", srv, ":", x$job_ids[[srv]], "(", st, ")\n")
  }
  invisible(x)
}
