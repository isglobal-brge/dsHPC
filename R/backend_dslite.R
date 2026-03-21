# Module: DSLite Backend (fallback)
# Returns minimal backend for local testing with DSLite.

#' @keywords internal
.backend_dslite <- function(conn) {
  list(
    type = "dslite",
    username = Sys.getenv("USER", "anonymous")
  )
}
