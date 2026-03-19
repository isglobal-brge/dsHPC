# Module: Job Capabilities
# Query runners, quotas, and limits from nodes.

#' Get job capabilities from all nodes
#'
#' @param conns DSI connections object.
#' @return A \code{dsjobs_result} with per-site capabilities.
#' @export
ds.jobs.capabilities <- function(conns) {
  results <- .ds_safe_aggregate(conns, expr = call("jobCapabilitiesDS"))

  dsjobs_result(
    per_site = results,
    meta = list(scope = "per_site")
  )
}
