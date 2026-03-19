# Module: Job Outputs
# List outputs (aggregate) and load outputs into server session (assign).

#' List available outputs from a completed job
#'
#' AGGREGATE -- returns info to client. Shows what's available to load.
#'
#' @param conns DSI connections object.
#' @param symbol Character; job handle symbol.
#' @return A dsjobs_result with per-site output listings.
#' @export
ds.jobs.outputs <- function(conns, symbol) {
  results <- .ds_safe_aggregate(conns, expr = call("jobOutputsDS", symbol))
  dsjobs_result(per_site = results, meta = list(scope = "per_site"))
}

#' Load a job output into the server R session
#'
#' ASSIGN -- the output object gets assigned to a symbol on the server.
#' The researcher never sees the data directly, but can use it as input
#' for dsFlower, dsImaging, another job, etc.
#'
#' Example:
#'   ds.jobs.load_output(conns, "myjob", "radiomics_table", symbol = "radio_features")
#'   # Now "radio_features" exists on each server as an R object
#'   ds.flower.nodes.init(conns, data = "radio_features")
#'
#' @param conns DSI connections object.
#' @param job_symbol Character; job handle symbol (or raw job_id).
#' @param output_name Character; name of the output to load.
#' @param symbol Character; symbol to assign the output to on the server.
#' @return Invisible NULL.
#' @export
ds.jobs.load_output <- function(conns, job_symbol, output_name,
                                 symbol = output_name) {
  DSI::datashield.assign.expr(
    conns, symbol = symbol,
    expr = call("jobLoadOutputDS", job_symbol, output_name)
  )
  invisible(NULL)
}
