# Module: Job Cancellation

#' @export
ds.jobs.cancel <- function(conns, symbol) {
  DSI::datashield.assign.expr(conns, symbol = symbol,
    expr = call("jobCancelDS", symbol))
  results <- .ds_safe_aggregate(conns, expr = call("jobStatusDS", symbol))
  dsjobs_result(per_site = results, meta = list(scope = "per_site"))
}
