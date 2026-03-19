# Module: Job Listing

#' List all jobs on all nodes
#'
#' @param conns DSI connections object.
#' @param label Character or NULL; filter by label (e.g. "dsRadiomics").
#'   Domain packages use this to show only their own jobs.
#' @return A dsjobs_result with per-site job listings.
#' @export
ds.jobs.list <- function(conns, label = NULL) {
  if (is.null(label)) {
    results <- .ds_safe_aggregate(conns, expr = call("jobListDS"))
  } else {
    results <- .ds_safe_aggregate(conns, expr = call("jobListDS", label))
  }
  dsjobs_result(per_site = results, meta = list(scope = "per_site"))
}
