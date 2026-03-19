# Module: Job Listing
# Passes authenticated username as caller_id so the server can filter
# private jobs to only show the caller's own + global jobs.

#' @export
ds.jobs.list <- function(conns, label = NULL) {
  results <- list()
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])

    # Try filesystem mirror
    fs_list <- NULL
    if (!identical(backend$type, "dslite"))
      fs_list <- tryCatch(backend$cp_list_jobs(label), error = function(e) NULL)

    # DS aggregate with caller_id for visibility filtering
    ds_list <- tryCatch({
      if (is.null(label))
        r <- DSI::datashield.aggregate(conns[srv],
          expr = call("jobListDS", NULL, backend$username))
      else
        r <- DSI::datashield.aggregate(conns[srv],
          expr = call("jobListDS", label, backend$username))
      r[[srv]]
    }, error = function(e) NULL)

    # Prefer DS list (has execution data), supplement with FS list
    if (!is.null(ds_list) && is.data.frame(ds_list) && nrow(ds_list) > 0)
      results[[srv]] <- ds_list
    else if (!is.null(fs_list) && is.data.frame(fs_list) && nrow(fs_list) > 0)
      results[[srv]] <- fs_list
    else
      results[[srv]] <- .empty_job_list()
  }
  dsjobs_result(per_site = results)
}
