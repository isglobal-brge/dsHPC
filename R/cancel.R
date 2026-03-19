# Module: Job Cancellation

#' @export
ds.jobs.cancel <- function(conns, job_id) {
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])
    if (identical(backend$type, "dslite")) {
      tryCatch(DSI::datashield.assign.expr(conns[srv], symbol = job_id,
        expr = call("jobCancelDS", job_id)), error = function(e) NULL)
    } else {
      backend$cp_cancel(job_id)
    }
  }
  invisible(NULL)
}
