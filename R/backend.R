# Module: Backend Detection
# Returns a minimal backend with type and username for owner injection.

#' Detect backend type and extract username from a DSI connection
#'
#' @param conn A single DSI connection (one server).
#' @return A list with `type` and `username` fields.
#' @keywords internal
.detect_backend <- function(conn) {
  if (inherits(conn, "OpalConnection")) {
    username <- tryCatch(conn@opal$username, error = function(e) "anonymous")
    return(list(type = "opal", username = username %||% "anonymous"))
  }

  if (inherits(conn, "ArmadilloConnection")) {
    username <- tryCatch(conn@user, error = function(e) "anonymous")
    return(list(type = "armadillo", username = username))
  }

  # DSLite / local fallback
  .backend_dslite(conn)
}
