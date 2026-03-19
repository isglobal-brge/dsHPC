# Module: Job Results
# Retrieve disclosure-safe results from completed jobs.

#' Get job results from all nodes
#'
#' @param conns DSI connections object.
#' @param symbol Character; job handle symbol.
#' @return A \code{dsjobs_result} with per-site results.
#' @export
ds.jobs.result <- function(conns, symbol) {
  results <- .ds_safe_aggregate(conns, expr = call("jobResultDS", symbol))

  # Check readiness
  all_ready <- all(vapply(results, function(r) {
    isTRUE(r$ready)
  }, logical(1)))

  warnings <- character(0)
  if (!all_ready) {
    not_ready <- names(Filter(function(r) !isTRUE(r$ready), results))
    warnings <- paste("Results not ready on:", paste(not_ready, collapse = ", "))
  }

  dsjobs_result(
    per_site = results,
    meta = list(scope = "per_site", warnings = warnings)
  )
}
