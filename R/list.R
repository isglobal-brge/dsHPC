# Module: Job Listing
# DS method with caller_id for visibility filtering.
# Identity verified by filesystem inbox (submit path).

#' @param mode Character; "mine", "mine+global", or "global".
#' @export
ds.jobs.list <- function(conns, label = NULL, mode = "mine+global") {
  results <- list()
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])
    caller_id <- backend$username
    r <- tryCatch({
      if (is.null(label))
        DSI::datashield.aggregate(conns[srv],
          expr = call("jobListDS", NULL, caller_id))
      else
        DSI::datashield.aggregate(conns[srv],
          expr = call("jobListDS", label, caller_id))
    }, error = function(e) list())
    results[[srv]] <- r[[srv]] %||% .empty_job_list()
  }
  dsjobs_result(per_site = results)
}
