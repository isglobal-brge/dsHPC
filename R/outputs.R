# Module: Job Outputs (DS aggregate/assign helpers -- backend-agnostic)

#' @export
ds.jobs.outputs <- function(conns, job_id) {
  results <- .ds_safe_aggregate(conns, expr = call("jobOutputsDS", job_id))
  dsjobs_result(per_site = results)
}

#' @export
ds.jobs.load_output <- function(conns, job_id, output_name, symbol = output_name) {
  DSI::datashield.assign.expr(conns, symbol = symbol,
    expr = call("jobLoadOutputDS", job_id, output_name))
  invisible(NULL)
}

#' @export
ds.jobs.capabilities <- function(conns) {
  results <- .ds_safe_aggregate(conns, expr = call("jobCapabilitiesDS"))
  dsjobs_result(per_site = results)
}

#' @export
ds.jobs.logs <- function(conns, job_id, last_n = 50L) {
  results <- .ds_safe_aggregate(conns, expr = call("jobLogsDS", job_id, as.integer(last_n)))
  dsjobs_result(per_site = results)
}
