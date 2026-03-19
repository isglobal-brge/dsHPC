# Module: Job Results
# Try filesystem mirror first, fallback to DS aggregate.

#' @export
ds.jobs.result <- function(conns, job_id) {
  results <- list()
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])

    # Try filesystem mirror
    res <- NULL
    if (!identical(backend$type, "dslite"))
      res <- tryCatch(backend$cp_read_result(job_id), error = function(e) NULL)

    if (!is.null(res)) {
      res$ready <- TRUE
      results[[srv]] <- res
      next
    }

    # Fallback to DS aggregate
    res_ds <- tryCatch({
      r <- DSI::datashield.aggregate(conns[srv], expr = call("jobResultDS", job_id))
      r[[srv]]
    }, error = function(e) NULL)

    results[[srv]] <- res_ds %||% list(job_id = job_id, ready = FALSE, state = "UNKNOWN")
  }
  dsjobs_result(per_site = results)
}
