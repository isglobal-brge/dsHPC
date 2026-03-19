# Module: Package Hooks
# Session-level state for dsJobsClient.

# Null-coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

#' Internal environment for storing dsJobsClient session state
#' @keywords internal
.dsjobs_client_env <- new.env(parent = emptyenv())
