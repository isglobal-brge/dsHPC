# Module: Job Status

#' @export
ds.jobs.status <- function(conns, symbol) {
  results <- .ds_safe_aggregate(conns, expr = call("jobStatusDS", symbol))
  dsjobs_result(per_site = results, meta = list(scope = "per_site"))
}
