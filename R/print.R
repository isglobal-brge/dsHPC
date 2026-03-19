# Module: Result Objects
# dsjobs_result construction and printing.

#' Create a dsjobs_result object
#'
#' @param per_site Named list mapping server names to their results.
#' @param pooled NULL or a single aggregated result.
#' @param meta Named list of metadata.
#' @return A \code{dsjobs_result} object.
#' @keywords internal
dsjobs_result <- function(per_site, pooled = NULL, meta = list()) {
  obj <- list(
    per_site = per_site,
    pooled   = pooled,
    meta     = list(
      call_code      = meta$call_code %||% "",
      timestamp      = Sys.time(),
      servers        = names(per_site),
      scope          = meta$scope %||% "per_site",
      warnings       = meta$warnings %||% character(0)
    )
  )
  class(obj) <- c("dsjobs_result", "list")
  obj
}

#' Print a dsjobs_result
#' @param x A dsjobs_result object.
#' @param ... Additional arguments (ignored).
#' @export
print.dsjobs_result <- function(x, ...) {
  cat("dsjobs_result\n")
  cat("  Servers:", paste(x$meta$servers, collapse = ", ") %||% "(none)", "\n")
  cat("  Scope:  ", x$meta$scope, "\n")

  # Show per-site status if available
  for (srv in x$meta$servers) {
    site <- x$per_site[[srv]]
    if (is.list(site) && !is.null(site$state)) {
      progress <- ""
      if (!is.null(site$step_index) && !is.null(site$total_steps)) {
        progress <- paste0(" [", site$step_index, "/", site$total_steps, "]")
      }
      cat("  ", srv, ": ", site$state, progress, "\n", sep = "")
    }
  }

  if (length(x$meta$warnings) > 0) {
    cat("  Warnings:", length(x$meta$warnings), "\n")
  }
  invisible(x)
}

#' Access dsjobs_result elements
#' @param x A dsjobs_result object.
#' @param name Character; the element name.
#' @export
`$.dsjobs_result` <- function(x, name) {
  if (name %in% c("per_site", "pooled", "meta")) return(.subset2(x, name))
  ps <- .subset2(x, "per_site")
  if (name %in% names(ps)) return(ps[[name]])
  .subset2(x, name)
}

#' Convert dsjobs_result to data.frame
#' @param x A dsjobs_result object.
#' @param ... Additional arguments (ignored).
#' @export
as.data.frame.dsjobs_result <- function(x, ...) {
  if (!is.null(x$pooled) && is.data.frame(x$pooled)) {
    return(x$pooled)
  }
  ps <- x$per_site
  if (length(ps) > 0) {
    first <- ps[[1]]
    if (is.data.frame(first)) return(first)
  }
  data.frame()
}
