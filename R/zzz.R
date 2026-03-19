# Module: Package Hooks

`%||%` <- function(x, y) if (is.null(x)) y else x

.dsjobs_client_env <- new.env(parent = emptyenv())
