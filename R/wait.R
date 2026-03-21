# Module: Blocking Wait

#' @export
ds.jobs.wait <- function(conns, job_id, timeout = 3600, poll_interval = 5) {
  deadline <- Sys.time() + timeout
  srv_names <- names(conns)
  done <- stats::setNames(rep(FALSE, length(srv_names)), srv_names)
  terminal <- c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED")
  last <- list()

  message("Waiting for job '", job_id, "' ...")
  while (Sys.time() < deadline) {
    for (srv in srv_names[!done]) {
      st <- tryCatch({
        r <- DSI::datashield.aggregate(conns[srv],
          expr = call("jobStatusDS", job_id))
        r[[srv]]
      }, error = function(e) NULL)

      if (is.null(st)) next
      key <- paste0(st$step_index %||% 0, "/", st$total_steps %||% 0, ":", st$state)
      if (!identical(last[[srv]], key)) {
        message("  ", srv, ": ", st$state, " [",
                st$step_index %||% 0, "/", st$total_steps %||% 0, "]")
        last[[srv]] <- key
      }
      if (st$state %in% terminal) done[[srv]] <- TRUE
    }
    if (all(done)) break
    Sys.sleep(poll_interval)
  }
  if (!all(done)) warning("Timeout on: ", paste(srv_names[!done], collapse=", "), call.=FALSE)
  ds.jobs.status(conns, job_id)
}
