# Module: Job Status
# Query job status across all nodes.

#' Get job status from all nodes
#'
#' @param conns DSI connections object.
#' @param symbol Character; job handle symbol.
#' @return A \code{dsjobs_result} with per-site status.
#' @export
ds.jobs.status <- function(conns, symbol) {
  results <- .ds_safe_aggregate(conns, expr = call("jobStatusDS", symbol))

  dsjobs_result(
    per_site = results,
    meta = list(scope = "per_site")
  )
}
