# Module: Job Listing
# List all jobs across nodes.

#' List all jobs on all nodes
#'
#' @param conns DSI connections object.
#' @return A \code{dsjobs_result} with per-site job lists.
#' @export
ds.jobs.list <- function(conns) {
  results <- .ds_safe_aggregate(conns, expr = call("jobListDS"))

  dsjobs_result(
    per_site = results,
    meta = list(scope = "per_site")
  )
}
