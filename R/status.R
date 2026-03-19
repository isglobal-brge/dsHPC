# Module: Job Status
# DS method is primary. Backend mirror as supplement when available.

#' @export
ds.jobs.status <- function(conns, job_id, access_token = NULL) {
  results <- list()
  for (srv in names(conns)) {
    # DS method (works everywhere, enforces token)
    r <- DSI::datashield.aggregate(conns[srv],
      expr = call("jobStatusDS", job_id, access_token))
    results[[srv]] <- r[[srv]]
  }
  dsjobs_result(per_site = results)
}
