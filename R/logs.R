# Module: Job Logs
# Retrieve sanitized logs from job execution.

#' Get job logs from all nodes
#'
#' @param conns DSI connections object.
#' @param symbol Character; job handle symbol.
#' @param last_n Integer; number of log lines to return (default 50).
#' @return A \code{dsjobs_result} with per-site sanitized logs.
#' @export
ds.jobs.logs <- function(conns, symbol, last_n = 50L) {
  results <- .ds_safe_aggregate(
    conns,
    expr = call("jobLogsDS", symbol, as.integer(last_n))
  )

  dsjobs_result(
    per_site = results,
    meta = list(scope = "per_site")
  )
}
