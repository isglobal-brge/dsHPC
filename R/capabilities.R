# Module: Job Capabilities

#' @export
ds.jobs.capabilities <- function(conns) {
  results <- .ds_safe_aggregate(conns, expr = call("jobCapabilitiesDS"))
  dsjobs_result(per_site = results, meta = list(scope = "per_site"))
}
