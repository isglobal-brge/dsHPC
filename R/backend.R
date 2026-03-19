# Module: Control Plane Backend Interface
# Abstract interface for job submission, status, results, cancellation.
# Concrete implementations in backend_opal.R and backend_armadillo.R.
# DSLite fallback in backend_dslite.R.

#' Detect and create the appropriate backend for a DSI connection
#'
#' Inspects the connection type and returns the correct backend.
#' Falls back to DSLite backend if no Opal/Armadillo detected.
#'
#' @param conn A single DSI connection (one server).
#' @return A backend object (list with cp_* functions).
#' @keywords internal
.detect_backend <- function(conn) {
  # DSOpal connection
  if (inherits(conn, "OpalConnection")) {
    if (!requireNamespace("opalr", quietly = TRUE))
      stop("opalr package required for Opal backend.", call. = FALSE)
    return(.backend_opal(conn))
  }

  # DSMolgenisArmadillo connection
  if (inherits(conn, "ArmadilloConnection")) {
    if (!requireNamespace("MolgenisArmadillo", quietly = TRUE))
      stop("MolgenisArmadillo package required for Armadillo backend.", call. = FALSE)
    return(.backend_armadillo(conn))
  }

  # DSLite / local fallback
  .backend_dslite(conn)
}

#' Get backends for all connections
#' @keywords internal
.get_backends <- function(conns) {
  backends <- list()
  for (srv in names(conns)) {
    backends[[srv]] <- .detect_backend(conns[[srv]])
  }
  backends
}
