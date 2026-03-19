# Module: Job Submission
# Inbox is the canonical submit path. DS method used ONLY for
# inline execution of session-only jobs (convenience, not identity).

#' Submit a job to all nodes
#'
#' @param conns DSI connections object.
#' @param job A dsjobs_job object.
#' @return A dsjobs_submission with job_id, access_token, per-server info.
#' @export
ds.jobs.submit <- function(conns, job) {
  if (!inherits(job, "dsjobs_job"))
    stop("'job' must be a dsjobs_job object.", call. = FALSE)

  job_id <- .generate_job_id()
  access_token <- .generate_access_token()

  spec <- as.list(job)
  spec$job_id <- job_id
  spec$.access_token_hash <- digest::digest(access_token, algo = "sha256",
                                             serialize = FALSE)

  submissions <- list()
  for (srv in names(conns)) {
    backend <- .detect_backend(conns[[srv]])

    # 1. Write to control plane backend (canonical submit)
    if (!identical(backend$type, "dslite")) {
      spec$.owner <- backend$username
      tryCatch(backend$cp_submit(job_id, spec), error = function(e)
        warning("Inbox write failed on ", srv, ": ", e$message, call. = FALSE))
    }

    # 2. DS method for execution (worker reads from SQLite, not inbox)
    spec_enc <- .ds_encode(spec)
    tryCatch(
      DSI::datashield.assign.expr(conns[srv], symbol = job_id,
        expr = call("jobSubmitDS", spec_enc)),
      error = function(e)
        warning("DS submit failed on ", srv, ": ", e$message, call. = FALSE))

    submissions[[srv]] <- list(
      method = backend$type, username = backend$username)
  }

  result <- list(job_id = job_id, access_token = access_token,
    label = job$label, visibility = job$visibility,
    servers = names(conns), submissions = submissions,
    submitted_at = Sys.time())
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
