# Module: Job Logs

#' @export
ds.jobs.logs <- function(conns, symbol, last_n = 50L) {
  results <- .ds_safe_aggregate(conns,
    expr = call("jobLogsDS", symbol, as.integer(last_n)))
  dsjobs_result(per_site = results, meta = list(scope = "per_site"))
}
