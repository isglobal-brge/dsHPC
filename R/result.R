# Module: Job Results

#' @export
ds.jobs.result <- function(conns, job_id, access_token = NULL) {
  results <- list()
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])
    res <- NULL
    if (!identical(backend$type, "dslite"))
      res <- tryCatch(backend$cp_read_result(job_id), error = function(e) NULL)
    if (!is.null(res)) { res$ready <- TRUE; results[[srv]] <- res; next }
    # DS fallback -- let access denied errors propagate
    r <- DSI::datashield.aggregate(conns[srv],
      expr = call("jobResultDS", job_id, access_token))
    results[[srv]] <- r[[srv]] %||% list(job_id = job_id, ready = FALSE, state = "UNKNOWN")
  }
  dsjobs_result(per_site = results)
}
