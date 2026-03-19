# Module: Job Listing

#' @export
ds.jobs.list <- function(conns, label = NULL) {
  results <- list()
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])
    if (identical(backend$type, "dslite")) {
      results[[srv]] <- tryCatch({
        if (is.null(label)) r <- DSI::datashield.aggregate(conns[srv], expr = call("jobListDS"))
        else r <- DSI::datashield.aggregate(conns[srv], expr = call("jobListDS", label))
        r[[srv]]
      }, error = function(e) .empty_job_list())
    } else {
      results[[srv]] <- backend$cp_list_jobs(label)
    }
  }
  dsjobs_result(per_site = results)
}
