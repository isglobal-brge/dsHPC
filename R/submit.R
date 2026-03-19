# Module: Job Submission

#' Submit a job to all nodes
#'
#' @param conns DSI connections object.
#' @param job A dsjobs_job object.
#' @param symbol Character; symbol name for the job handle.
#' @return A dsjobs_result with per-site submission results.
#' @export
ds.jobs.submit <- function(conns, job, symbol = NULL) {
  if (!inherits(job, "dsjobs_job")) {
    stop("'job' must be a dsjobs_job object.", call. = FALSE)
  }
  if (is.null(symbol)) symbol <- .generate_symbol("dsJ")

  spec_encoded <- .ds_encode(as.list(job))

  DSI::datashield.assign.expr(
    conns, symbol = symbol,
    expr = call("jobSubmitDS", spec_encoded)
  )

  .dsjobs_client_env$.last_symbol <- symbol
  .dsjobs_client_env$.conns <- conns

  # Get immediate status using the symbol (handle lookup)
  results <- .ds_safe_aggregate(conns, expr = call("jobStatusDS", symbol))

  dsjobs_result(per_site = results, meta = list(scope = "per_site"))
}
