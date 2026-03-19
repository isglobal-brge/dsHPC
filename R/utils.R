# Module: Client Utilities
# Transport encoding, symbol generation, and resilient aggregation.

#' Generate a unique temporary symbol name
#'
#' @param prefix Character; prefix for the generated symbol.
#' @return Character; a unique symbol string.
#' @keywords internal
.generate_symbol <- function(prefix = "dsJ") {
  paste0(prefix, ".",
         paste(sample(c(letters, LETTERS, 0:9), 6,
                       replace = TRUE),
               collapse = ""))
}

#' Encode a complex R object for DataSHIELD transport
#'
#' @param x An R object to encode.
#' @return A B64-encoded string if x is complex, or x unchanged if scalar.
#' @keywords internal
.ds_encode <- function(x) {
  if (is.list(x) || (is.vector(x) && length(x) > 1)) {
    json <- as.character(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"))
    b64 <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(json)))
    b64 <- gsub("\\+", "-", b64)
    b64 <- gsub("/", "_", b64)
    b64 <- gsub("=+$", "", b64)
    paste0("B64:", b64)
  } else {
    x
  }
}

#' Resilient datashield.aggregate that tolerates per-server failures
#'
#' @param conns DSI connections object.
#' @param expr The call expression to evaluate.
#' @return Named list of results (only successful servers).
#' @keywords internal
.ds_safe_aggregate <- function(conns, expr) {
  server_names <- names(conns)
  results <- list()
  errors <- list()
  for (srv in server_names) {
    tryCatch({
      res <- DSI::datashield.aggregate(conns[srv], expr = expr)
      results[[srv]] <- res[[srv]]
    }, error = function(e) {
      errors[[srv]] <<- e$message
    })
  }
  if (length(errors) > 0) {
    attr(results, "ds_errors") <- errors
  }
  results
}
