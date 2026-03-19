# Module: Job Results

#' @export
ds.jobs.result <- function(conns, symbol) {
  results <- .ds_safe_aggregate(conns, expr = call("jobResultDS", symbol))
  all_ready <- all(vapply(results, function(r) isTRUE(r$ready), logical(1)))
  warnings <- if (!all_ready) {
    not_ready <- names(Filter(function(r) !isTRUE(r$ready), results))
    paste("Results not ready on:", paste(not_ready, collapse = ", "))
  } else character(0)
  dsjobs_result(per_site = results, meta = list(scope = "per_site", warnings = warnings))
}
