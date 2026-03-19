# Module: Job Status

#' @export
ds.jobs.status <- function(conns, job_id) {
  results <- list()
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])
    if (identical(backend$type, "dslite")) {
      results[[srv]] <- tryCatch({
        r <- DSI::datashield.aggregate(conns[srv], expr = call("jobStatusDS", job_id))
        r[[srv]]
      }, error = function(e) list(state = "ERROR", error = e$message))
    } else {
      st <- backend$cp_read_status(job_id)
      results[[srv]] <- if (!is.null(st)) st
        else list(job_id = job_id, state = "PENDING", note = "Not yet processed by worker")
    }
  }
  dsjobs_result(per_site = results)
}
