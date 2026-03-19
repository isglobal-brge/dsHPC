# Module: Job Constructor

#' @export
ds_job <- function(steps, label = NULL, tags = NULL, visibility = "private",
                   publish = NULL, resource_class = NULL, ...) {
  if (!is.list(steps) || length(steps) == 0)
    stop("A job must contain at least one step.", call. = FALSE)
  for (i in seq_along(steps))
    if (!inherits(steps[[i]], "dsjobs_step"))
      stop("Step ", i, " is not a dsjobs_step object.", call. = FALSE)
  steps_plain <- lapply(steps, function(s) { l <- as.list(s); class(l) <- NULL; l })
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
  for (i in seq_along(x$steps)) {
    s <- x$steps[[i]]
    cat("  [", i, "] ", s$type, " (", s$plane, ")",
        if (!is.null(s$runner)) paste0(" runner=", s$runner) else "", "\n", sep = "")
  }
  invisible(x)
}
