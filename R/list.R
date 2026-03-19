# Module: Job Listing

#' @export
ds.jobs.list <- function(conns) {
  results <- .ds_safe_aggregate(conns, expr = call("jobListDS"))
  dsjobs_result(per_site = results, meta = list(scope = "per_site"))
}
