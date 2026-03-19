# Module: Job Status
# Try filesystem mirror first, fallback to DS aggregate.

#' @export
ds.jobs.status <- function(conns, job_id) {
  results <- list()
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])

    # Try filesystem mirror first (if not DSLite)
    st <- NULL
    if (!identical(backend$type, "dslite"))
      st <- tryCatch(backend$cp_read_status(job_id), error = function(e) NULL)

    # Fallback to DS aggregate
    if (is.null(st) || identical(st$state, "PENDING")) {
      st_ds <- tryCatch({
        r <- DSI::datashield.aggregate(conns[srv], expr = call("jobStatusDS", job_id))
        r[[srv]]
      }, error = function(e) NULL)
      if (!is.null(st_ds)) st <- st_ds
    }

    results[[srv]] <- st %||% list(job_id = job_id, state = "UNKNOWN")
  }
  dsjobs_result(per_site = results)
}
