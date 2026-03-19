# Module: Job Constructor

#' Create a job specification
#'
#' @param steps List of dsjobs_step objects.
#' @param label Character or NULL; package signature for filtering
#'   (e.g. "dsRadiomics", "dsImaging"). Upstream packages set this
#'   so their jobs are distinguishable from others.
#' @param tags Character vector or NULL; freeform tags for categorization.
#' @param visibility Character; "private" (default, only owner sees it) or
#'   "global" (visible to all users on the node -- for shared resources
#'   like radiomics features that benefit everyone).
#' @param publish Named list with publish config, or NULL.
#' @param resource_class Character; resource class hint.
#' @param ... Additional metadata.
#' @return A dsjobs_job object.
#' @export
ds_job <- function(steps, label = NULL, tags = NULL, visibility = "private",
                   publish = NULL, resource_class = NULL, ...) {
  if (!is.list(steps) || length(steps) == 0)
    stop("A job must contain at least one step.", call. = FALSE)
  for (i in seq_along(steps))
    if (!inherits(steps[[i]], "dsjobs_step"))
      stop("Step ", i, " is not a dsjobs_step object.", call. = FALSE)

  steps_plain <- lapply(steps, function(s) {
    s_list <- as.list(s)
    class(s_list) <- NULL
    s_list
  })

  job <- list(steps = steps_plain, label = label, tags = tags,
              visibility = visibility, publish = publish,
              resource_class = resource_class %||% "default", ...)
  class(job) <- c("dsjobs_job", "list")
  job
}

#' @export
print.dsjobs_job <- function(x, ...) {
  cat("dsjobs_job\n")
  if (!is.null(x$label)) cat("  Label:", x$label, "\n")
  if (!is.null(x$tags)) cat("  Tags:", paste(x$tags, collapse = ", "), "\n")
  if (!identical(x$visibility, "private")) cat("  Visibility:", x$visibility, "\n")
  cat("  Steps:", length(x$steps), "\n")
  cat("  Resource class:", x$resource_class, "\n")
  if (!is.null(x$publish))
    cat("  Publish:", x$publish$asset_name %||% "(configured)", "\n")
  for (i in seq_along(x$steps)) {
    s <- x$steps[[i]]
    cat("  [", i, "] ", s$type, " (", s$plane, ")",
        if (!is.null(s$runner)) paste0(" runner=", s$runner) else "",
        "\n", sep = "")
  }
  invisible(x)
}
