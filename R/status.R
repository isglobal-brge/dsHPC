# Module: Job Status

#' @param access_token Character or NULL; the access token from submission.
#' @export
ds.jobs.status <- function(conns, job_id, access_token = NULL) {
  results <- list()
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])
    st <- NULL
    if (!identical(backend$type, "dslite"))
      st <- tryCatch(backend$cp_read_status(job_id), error = function(e) NULL)
    if (is.null(st) || identical(st$state, "PENDING")) {
      # DS fallback -- let access denied errors propagate
      r <- DSI::datashield.aggregate(conns[srv],
        expr = call("jobStatusDS", job_id, access_token))
      st <- r[[srv]]
    }
    results[[srv]] <- st %||% list(job_id = job_id, state = "UNKNOWN")
  }
  dsjobs_result(per_site = results)
}
