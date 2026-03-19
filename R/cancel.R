# Module: Job Cancellation
# Cancel running or pending jobs.

#' Cancel a job on all nodes
#'
#' @param conns DSI connections object.
#' @param symbol Character; job handle symbol.
#' @return A \code{dsjobs_result} with per-site cancellation status.
#' @export
ds.jobs.cancel <- function(conns, symbol) {
  DSI::datashield.assign.expr(
    conns,
    symbol = symbol,
    expr = call("jobCancelDS", symbol)
  )

  results <- .ds_safe_aggregate(conns, expr = call("jobStatusDS", symbol))

  dsjobs_result(
    per_site = results,
    meta = list(scope = "per_site")
  )
}
