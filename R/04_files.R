#' Upload content to the HPC resource
#'
#' @param config API configuration created by create_api_config
#' @param content Content to upload (raw vector, character, or other object)
#' @param filename Name to use for the uploaded content
#'
#' @return TRUE if the content was successfully uploaded or already exists
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' content <- "Hello, World!"
#' upload_file(config, content, "hello.txt")
#' }
upload_file <- function(config, content, filename) {
  if (!requireNamespace("base64enc", quietly = TRUE)) {
    stop("Package 'base64enc' is required. Please install it.")
  }
  
  # Convert content to raw bytes if it's not already
  if (!is.raw(content)) {
    if (is.character(content)) {
      content <- charToRaw(paste(content, collapse = "\n"))
    } else {
      content <- serialize(content, NULL)
    }
  }
  
  # Compute content hash using the dedicated function
  file_hash <- hash_content(content)
  
  # Check if content already exists
  if (hash_exists(config, file_hash)) {
    message("Content already exists in the database.")
    return(TRUE)
  }
  
  # Check size of content
  content_size <- length(content)
  size_mb <- content_size / (1024 * 1024)
  
  # Use optimized upload for large files (>100MB)
  if (size_mb > 100) {
    message(sprintf("Large file detected (%.1f MB). Using optimized upload...", size_mb))
    return(upload_file_optimized(config, content, filename, file_hash))
  }
  
  # For smaller files, use direct encoding
  content_base64 <- base64enc::base64encode(content)
  
  # Create request body
  body <- list(
    file_hash = file_hash,
    content = content_base64,
    filename = filename,
    content_type = "application/octet-stream"
  )
  
  # Set very long timeout to avoid issues
  original_timeout <- config$timeout
  config$timeout <- 31536000  # 1 year in seconds
  
  # Upload the content
  tryCatch({
    response <- api_post(config, "/files/upload", body = body)
    message("Content uploaded successfully.")
    return(TRUE)
  }, error = function(e) {
    stop(paste0("Error uploading content: ", e$message))
  }, finally = {
    config$timeout <- original_timeout
  })
}

#' Upload large file with optimized memory usage
#' 
#' @param config API configuration
#' @param content Raw content to upload
#' @param filename Name for the file
#' @param file_hash Pre-computed hash of the content
#' @return TRUE if successful
#' @keywords internal
upload_file_optimized <- function(config, content, filename, file_hash) {
  # Write to a temp file for more efficient processing
  temp_file <- tempfile()
  on.exit(unlink(temp_file), add = TRUE)
  
  # Write raw content to temp file
  writeBin(content, temp_file)
  
  # Use R base64enc for reliable encoding
  # base64enc::base64encode can accept a filename as input (memory efficient)
  message("  Using R base64enc for reliable encoding...")
  content_base64 <- base64enc::base64encode(temp_file)
  
  # Create request body
  body <- list(
    file_hash = file_hash,
    content = content_base64,
    filename = filename,
    content_type = "application/octet-stream"
  )
  
  # Set a very long timeout (use .Machine$integer.max for max safe integer)
  original_timeout <- config$timeout
  config$timeout <- .Machine$integer.max
  
  size_mb <- length(content) / (1024 * 1024)
  message(sprintf("  Uploading %.1f MB file...", size_mb))
  
  # Upload the content
  tryCatch({
    response <- api_post(config, "/files/upload", body = body)
    message("Large file uploaded successfully.")
    return(TRUE)
  }, error = function(e) {
    stop(paste0("Error uploading large file: ", e$message))
  }, finally = {
    # Restore original timeout
    config$timeout <- original_timeout
  })
} 