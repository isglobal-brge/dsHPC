# Module: Job Outputs

#' @export
ds.jobs.outputs <- function(conns, job_id, access_token = NULL) {
  results <- .ds_safe_aggregate(conns,
    expr = call("jobOutputsDS", job_id, access_token))
  dsjobs_result(per_site = results)
}

#' @export
ds.jobs.load_output <- function(conns, job_id, output_name,
                                 symbol = output_name, access_token = NULL) {
  DSI::datashield.assign.expr(conns, symbol = symbol,
    expr = call("jobLoadOutputDS", job_id, output_name, access_token))
  invisible(NULL)
}

#' @export
ds.jobs.capabilities <- function(conns) {
  results <- .ds_safe_aggregate(conns, expr = call("jobCapabilitiesDS"))
  dsjobs_result(per_site = results)
}

#' @export
ds.jobs.logs <- function(conns, job_id, last_n = 50L, access_token = NULL) {
  results <- .ds_safe_aggregate(conns,
    expr = call("jobLogsDS", job_id, as.integer(last_n), access_token))
  dsjobs_result(per_site = results)
}
