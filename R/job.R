# Module: Job Constructor

#' Create a dsHPC job specification
#'
#' Internal builder for server-side domain packages that prefer a typed helper
#' over raw lists.
#'
#' @param steps List of `dshpc_step` objects created with `ds_step_*()`.
#' @param pipeline Optional DAG pipeline created with `ds_pipeline()`.
#' @param dag Optional raw DAG list for advanced callers.
#' @param name Optional human-readable job name for displays. If it contains the
#'   dsHPC number placeholder, dsHPC resolves that placeholder on submit using
#'   the next visible job number on each server.
#' @param label Required domain label identifying the server-side package that
#'   submitted the job.
#' @param tags Optional character vector of operational tags.
#' @param visibility Character visibility marker, usually `"private"` or
#'   `"global"`.
#' @param publish Optional publish metadata consumed by server packages.
#' @param resource_class Optional coarse resource class.
#' @param ... Additional server-side job specification fields.
#' @return A `dshpc_job` object.
#' @keywords internal
ds_job <- function(steps = NULL, name = NULL, label = NULL, tags = NULL,
                   visibility = "private", publish = NULL,
                   resource_class = NULL, pipeline = NULL, dag = NULL, ...) {
  if (!is.character(label) || length(label) != 1L || is.na(label) ||
      !nzchar(label)) {
    stop("'label' must be a non-empty domain label.", call. = FALSE)
  }
  if (!is.null(pipeline) && !is.null(dag))
    stop("Use either 'pipeline' or 'dag', not both.", call. = FALSE)
  if (!is.null(pipeline)) dag <- pipeline
  if (!is.null(dag)) {
    dag <- as.list(dag)
    class(dag) <- NULL
    job <- list(dag = dag, name = name, label = label, tags = tags,
                visibility = visibility, publish = publish,
                resource_class = resource_class %||% "default", ...)
    class(job) <- c("dshpc_job", "list")
    return(job)
  }

  if (!is.list(steps) || length(steps) == 0L)
    stop("A job must contain at least one step.", call. = FALSE)
  for (i in seq_along(steps)) {
    if (!inherits(steps[[i]], "dshpc_step"))
      stop("Step ", i, " is not a dshpc_step object.", call. = FALSE)
  }
  steps_plain <- lapply(steps, function(s) {
    l <- as.list(s)
    class(l) <- NULL
    l
  })
  job <- list(steps = steps_plain, name = name, label = label, tags = tags,
              visibility = visibility, publish = publish,
              resource_class = resource_class %||% "default", ...)
  class(job) <- c("dshpc_job", "list")
  job
}

#' @export
print.dshpc_job <- function(x, ...) {
  cat("dshpc_job\n")
  if (!is.null(x$name)) cat("  Name:", x$name, "\n")
  if (!is.null(x$label)) cat("  Label:", x$label, "\n")
  if (!is.null(x$tags)) cat("  Tags:", paste(x$tags, collapse = ", "), "\n")
  if (!identical(x$visibility, "private")) cat("  Visibility:", x$visibility, "\n")
  if (!is.null(x$dag)) {
    nodes <- x$dag$nodes %||% x$dag
    node_names <- names(nodes) %||% rep("", length(nodes))
    cat("  DAG nodes:", length(nodes), "\n")
    for (i in seq_along(nodes)) {
      node <- nodes[[i]]
      s <- node$step %||% node
      cat("  [", i, "] ", node$id %||% node_names[[i]] %||% "<unnamed>",
          ": ", s$type %||% "<unknown>",
          if (!is.null(s$runner)) paste0(" runner=", s$runner) else "",
          "\n", sep = "")
    }
  } else {
    cat("  Steps:", length(x$steps), "\n")
    for (i in seq_along(x$steps)) {
      s <- x$steps[[i]]
      cat("  [", i, "] ", s$type, " (", s$plane, ")",
          if (!is.null(s$runner)) paste0(" runner=", s$runner) else "",
          "\n", sep = "")
    }
  }
  invisible(x)
}
