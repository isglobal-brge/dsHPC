# Module: Result Objects

#' @keywords internal
dsjobs_result <- function(per_site, pooled = NULL, meta = list()) {
  obj <- list(per_site = per_site, pooled = pooled,
    meta = list(timestamp = Sys.time(), servers = names(per_site),
                scope = meta$scope %||% "per_site"))
  class(obj) <- c("dsjobs_result", "list")
  obj
}
#' @export
print.dsjobs_result <- function(x, ...) {
  cat("dsjobs_result\n  Servers:", paste(x$meta$servers, collapse=", "), "\n")
  for (srv in x$meta$servers) {
    site <- x$per_site[[srv]]
    if (is.list(site) && !is.null(site$state))
      cat("  ", srv, ": ", site$state, if (!is.null(site$step_index))
        paste0(" [", site$step_index, "/", site$total_steps %||% "?", "]") else "", "\n", sep="")
  }
  invisible(x)
}
#' @export
`$.dsjobs_result` <- function(x, name) {
  if (name %in% c("per_site","pooled","meta")) return(.subset2(x, name))
  ps <- .subset2(x, "per_site")
  if (name %in% names(ps)) return(ps[[name]])
  .subset2(x, name)
}
#' @export
as.data.frame.dsjobs_result <- function(x, ...) {
  if (!is.null(x$pooled) && is.data.frame(x$pooled)) return(x$pooled)
  ps <- x$per_site
  if (length(ps) > 0 && is.data.frame(ps[[1]])) return(ps[[1]])
  data.frame()
}
