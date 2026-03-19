# Module: Job Cancellation

#' @export
ds.jobs.cancel <- function(conns, job_id, access_token = NULL) {
  for (srv in names(conns)) {
    tryCatch(
      DSI::datashield.assign.expr(conns[srv], symbol = job_id,
        expr = call("jobCancelDS", job_id, access_token)),
      error = function(e) NULL)
  }
  invisible(NULL)
}
