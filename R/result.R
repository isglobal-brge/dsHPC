# Module: Job Results
# DS method is primary.

#' @export
ds.jobs.result <- function(conns, job_id, access_token = NULL) {
  results <- list()
  for (srv in names(conns)) {
    r <- DSI::datashield.aggregate(conns[srv],
      expr = call("jobResultDS", job_id, access_token))
    results[[srv]] <- r[[srv]]
  }
  dsjobs_result(per_site = results)
}
