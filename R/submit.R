# Module: Job Submission
# Routes through the detected backend (Opal/Armadillo/DSLite).

#' Submit a job to all nodes
#'
#' Detects the backend for each connection and submits via the
#' appropriate control plane (Opal filesystem, Armadillo project, or
#' DSLite DS method fallback).
#'
#' @param conns DSI connections object.
#' @param job A dsjobs_job object.
#' @return A dsjobs_submission with job_id and per-server details.
#' @export
ds.jobs.submit <- function(conns, job) {
  if (!inherits(job, "dsjobs_job"))
    stop("'job' must be a dsjobs_job object.", call. = FALSE)

  job_id <- .generate_job_id()
  spec <- as.list(job)
  spec$job_id <- job_id

  submissions <- list()
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])

    if (identical(backend$type, "dslite")) {
      # DSLite fallback: use DS method directly
      spec_enc <- .ds_encode(spec)
      DSI::datashield.assign.expr(conns[srv], symbol = job_id,
        expr = call("jobSubmitDS", spec_enc))
      submissions[[srv]] <- list(method = "ds_method", username = backend$username)
    } else {
      # Opal or Armadillo: submit via control plane backend
      backend$cp_submit(job_id, spec)
      submissions[[srv]] <- list(method = backend$type, username = backend$username)
    }
  }

  result <- list(job_id = job_id, label = job$label, visibility = job$visibility,
    servers = names(conns), submissions = submissions, submitted_at = Sys.time())
  class(result) <- c("dsjobs_submission", "list")
  result
}

#' @export
print.dsjobs_submission <- function(x, ...) {
  cat("dsjobs_submission\n")
  cat("  Job ID:", x$job_id, "\n")
  if (!is.null(x$label)) cat("  Label:", x$label, "\n")
  cat("  Submitted:", format(x$submitted_at, "%Y-%m-%d %H:%M:%S"), "\n")
  for (srv in names(x$submissions)) {
    s <- x$submissions[[srv]]
    cat("  ", srv, ": ", s$method, " (", s$username, ")\n", sep = "")
  }
  invisible(x)
}
