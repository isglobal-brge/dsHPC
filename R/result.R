# Module: Job Results

#' @export
ds.jobs.result <- function(conns, job_id) {
  results <- list()
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])
    if (identical(backend$type, "dslite")) {
      results[[srv]] <- tryCatch({
        r <- DSI::datashield.aggregate(conns[srv], expr = call("jobResultDS", job_id))
        r[[srv]]
      }, error = function(e) list(ready = FALSE, error = e$message))
    } else {
      res <- backend$cp_read_result(job_id)
      if (!is.null(res)) { res$ready <- TRUE; results[[srv]] <- res }
      else {
        st <- backend$cp_read_status(job_id)
        results[[srv]] <- list(job_id = job_id, ready = FALSE,
                                state = st$state %||% "PENDING")
      }
    }
  }
  dsjobs_result(per_site = results)
}
