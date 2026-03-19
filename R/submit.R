# Module: Job Submission
# Dual-path: filesystem for identity + DS method for execution.

#' Submit a job to all nodes
#'
#' For Opal/Armadillo: writes spec to user's inbox (identity) AND
#' calls jobSubmitDS (execution). For DSLite: DS method only.
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

    # 1. Write to filesystem for identity/audit trail
    if (!identical(backend$type, "dslite")) {
      tryCatch(backend$cp_submit(job_id, spec), error = function(e) NULL)
    }

    # 2. Also submit via DS method for immediate execution
    #    Inject the verified username from the backend
    spec$.owner <- backend$username
    spec_enc <- .ds_encode(spec)
    DSI::datashield.assign.expr(conns[srv], symbol = job_id,
      expr = call("jobSubmitDS", spec_enc))

    submissions[[srv]] <- list(
      method = backend$type,
      username = backend$username
    )
  }

  # Get initial status
  results <- .ds_safe_aggregate(conns, expr = call("jobStatusDS", job_id))

  result <- list(job_id = job_id, label = job$label, visibility = job$visibility,
    servers = names(conns), submissions = submissions,
    submitted_at = Sys.time(), status = results)
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
  if (!is.null(x$status)) {
    for (srv in names(x$status)) {
      st <- x$status[[srv]]
      if (!is.null(st$state))
        cat("  ", srv, ": ", st$state, " [", st$step_index %||% 0,
            "/", st$total_steps %||% 0, "]\n", sep = "")
    }
  }
  invisible(x)
}
