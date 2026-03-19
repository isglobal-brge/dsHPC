# Module: Client Utilities
# Transport encoding, symbol generation, resilient aggregation.

#' @keywords internal
.generate_symbol <- function(prefix = "dsJ") {
  paste0(prefix, ".",
         paste(sample(c(letters, LETTERS, 0:9), 6, replace = TRUE), collapse = ""))
}

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
  if (length(errors) > 0) attr(results, "ds_errors") <- errors
  results
}
