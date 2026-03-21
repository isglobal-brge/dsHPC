# Module: Job Status

#' @export
ds.jobs.status <- function(conns, job_id) {
  results <- list()
  for (srv in names(conns)) {
    r <- DSI::datashield.aggregate(conns[srv],
      expr = call("jobStatusDS", job_id))
    results[[srv]] <- r[[srv]]
  }
  dsjobs_result(per_site = results)
}
