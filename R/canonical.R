# Module: Canonical Spec Helpers
#
# The job spec hash must be independent of named-list construction order while
# preserving positional lists such as steps, args, and dependency arrays.

#' Canonicalise a job spec object before JSON hashing
#' @keywords internal
.canonicalise_spec <- function(x) {
  if (!is.list(x) || is.data.frame(x)) return(x)

  nms <- names(x)
  if (is.null(nms) || !any(nzchar(nms))) {
    return(lapply(x, .canonicalise_spec))
  }

  named <- nzchar(nms)
  named_idx <- which(named)
  unnamed_idx <- which(!named)
  ordered_named_idx <- named_idx[order(nms[named_idx])]
  out <- x[c(ordered_named_idx, unnamed_idx)]
  lapply(out, .canonicalise_spec)
}
