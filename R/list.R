# Module: Job Listing

#' @param mode Character; "mine", "mine+global", or "global".
#' @export
ds.jobs.list <- function(conns, label = NULL, mode = "mine+global") {
  results <- list()
  for (srv in names(conns)) {
    r <- tryCatch({
      if (is.null(label))
        DSI::datashield.aggregate(conns[srv],
          expr = call("jobListDS"))
      else
        DSI::datashield.aggregate(conns[srv],
          expr = call("jobListDS", label))
    }, error = function(e) list())
    results[[srv]] <- r[[srv]] %||% .empty_job_list()
  }
  dsjobs_result(per_site = results)
}
