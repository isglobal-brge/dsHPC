# Module: Job Submission
# Fan-out job submission to all nodes via DSI.

#' Submit a job to all nodes
#'
#' Encodes the job spec and calls \code{jobSubmitDS} on each server.
#'
#' @param conns DSI connections object.
#' @param job A \code{dsjobs_job} object.
#' @param symbol Character; symbol name for the job handle (default
#'   auto-generated).
#' @return A \code{dsjobs_result} with per-site submission results.
#' @export
ds.jobs.submit <- function(conns, job, symbol = NULL) {
  if (!inherits(job, "dsjobs_job")) {
    stop("'job' must be a dsjobs_job object. Use ds_job() to create one.",
         call. = FALSE)
  }

  if (is.null(symbol)) {
    symbol <- .generate_symbol("dsJ")
  }

  # Encode job spec for transport
  spec_encoded <- .ds_encode(as.list(job))

  # Fan-out: submit to all servers
  DSI::datashield.assign.expr(
    conns,
    symbol = symbol,
    expr = call("jobSubmitDS", spec_encoded)
  )

  # Store for later reference
  .dsjobs_client_env$.last_symbol <- symbol
  .dsjobs_client_env$.conns <- conns

  # Get immediate status
  results <- .ds_safe_aggregate(conns, expr = call("jobStatusDS", symbol))

  # Extract job_ids for convenience
  job_ids <- list()
  for (srv in names(results)) {
    if (!is.null(results[[srv]]$job_id)) {
      job_ids[[srv]] <- results[[srv]]$job_id
    }
  }
  .dsjobs_client_env$.job_ids <- job_ids

  dsjobs_result(
    per_site = results,
    meta = list(scope = "per_site")
  )
}
