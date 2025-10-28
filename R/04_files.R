#' Upload content to the HPC resource
#'
#' @param config API configuration created by create_api_config
#' @param content Content to upload (raw vector, character, file path, or other object)
#' @param filename Name to use for the uploaded content
#' @param use_chunked_threshold_mb Threshold in MB for using chunked upload (default: 100)
#'
#' @return TRUE if the content was successfully uploaded or already exists
#' @export
#'
#' @details
#' This function automatically detects if content is a file path and uses chunked
#' upload for files larger than the threshold. For in-memory objects, it uses the
#' standard upload method. Chunked upload is more memory-efficient for large files.
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' 
#' # Upload in-memory content
#' content <- "Hello, World!"
#' upload_file(config, content, "hello.txt")
#' 
#' # Upload file (automatically uses chunked upload if > 100 MB)
#' upload_file(config, "large_dataset.rds", "dataset.rds")
#' 
#' # Customize chunked threshold
#' upload_file(config, "medium_file.csv", "data.csv", use_chunked_threshold_mb = 50)
#' }
upload_file <- function(config, content, filename, use_chunked_threshold_mb = 100) {
  if (!requireNamespace("base64enc", quietly = TRUE)) {
    stop("Package 'base64enc' is required. Please install it.")
  }
  
  # Check if content is a file path (auto-detect)
  if (is.character(content) && length(content) == 1 && file.exists(content)) {
    # Content is a file path
    file_path <- content
    file_size_mb <- file.info(file_path)$size / (1024^2)
    
    # Use chunked upload for large files
    if (file_size_mb > use_chunked_threshold_mb) {
      message(sprintf("File size (%.2f MB) exceeds threshold (%.2f MB). Using chunked upload...",
                     file_size_mb, use_chunked_threshold_mb))
      return(upload_file_chunked(config, file_path, filename))
    }
    
    # For smaller files, read into memory and use regular upload
    message(sprintf("Reading file into memory (%.2f MB)...", file_size_mb))
    content <- readBin(file_path, "raw", file.info(file_path)$size)
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

#' Calculate hash of a file efficiently
#'
#' @param file_path Path to the file to hash
#'
#' @return SHA-256 hash of the file as a character string
#'
#' @details
#' This function calculates the SHA-256 hash of a file without loading the
#' entire file into memory. It uses digest::digest with file=TRUE for efficient
#' reading of large files.
#'
#' @examples
#' \dontrun{
#' file_path <- "large_dataset.rds"
#' file_hash <- hash_file(file_path)
#' print(file_hash)
#' }
hash_file <- function(file_path) {
  if (!requireNamespace("digest", quietly = TRUE)) {
    stop("Package 'digest' is required. Please install it.")
  }
  
  # Validate file exists
  if (!file.exists(file_path)) {
    stop(paste0("File not found: ", file_path))
  }
  
  # Calculate hash efficiently without loading file into memory
  file_hash <- digest::digest(file_path, file = TRUE, algo = "sha256")
  
  return(file_hash)
}

#' Upload a file using chunked upload
#'
#' @param config API configuration created by create_api_config
#' @param file_path Path to the file to upload
#' @param filename Name to use for the uploaded file (default: basename of file_path)
#' @param chunk_size_mb Size of each chunk in megabytes (default: 10)
#' @param content_type MIME type of the content (default: "application/octet-stream")
#'
#' @return TRUE if the upload was successful
#'
#' @details
#' This function uploads large files in chunks without loading the entire file
#' into memory. It:
#' 1. Calculates the file hash efficiently
#' 2. Initializes a chunked upload session
#' 3. Reads and uploads the file in chunks
#' 4. Finalizes the upload and verifies the hash
#'
#' The function includes progress reporting and automatic retry for failed chunks.
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' upload_file_chunked(config, "large_dataset.rds", chunk_size_mb = 10)
#' }
upload_file_chunked <- function(config, file_path, filename = NULL, 
                               chunk_size_mb = 10,
                               content_type = "application/octet-stream") {
  if (!requireNamespace("base64enc", quietly = TRUE)) {
    stop("Package 'base64enc' is required. Please install it.")
  }
  
  # Validate file exists
  if (!file.exists(file_path)) {
    stop(paste0("File not found: ", file_path))
  }
  
  # Use basename if filename not provided
  if (is.null(filename)) {
    filename <- basename(file_path)
  }
  
  # Get file info
  file_info <- file.info(file_path)
  file_size <- file_info$size
  
  message(sprintf("Preparing to upload file: %s (%.2f MB)", 
                  filename, file_size / (1024^2)))
  
  # Calculate file hash efficiently
  message("Calculating file hash...")
  file_hash <- hash_file(file_path)
  message(sprintf("File hash: %s", file_hash))
  
  # Check if file already exists
  if (hash_exists(config, file_hash)) {
    message("File already exists in the database (based on hash).")
    return(TRUE)
  }
  
  # Calculate chunk size in bytes
  chunk_size <- chunk_size_mb * 1024 * 1024
  
  # Initialize chunked upload session
  message("Initializing chunked upload session...")
  init_body <- list(
    file_hash = file_hash,
    filename = filename,
    content_type = content_type,
    total_size = file_size,
    chunk_size = chunk_size
  )
  
  init_response <- tryCatch({
    api_post(config, "/files/upload-chunked/init", body = init_body)
  }, error = function(e) {
    stop(paste0("Error initializing chunked upload: ", e$message))
  })
  
  session_id <- init_response$session_id
  message(sprintf("Session initialized: %s", session_id))
  
  # Open file connection for reading
  con <- file(file_path, "rb")
  on.exit(close(con), add = TRUE)
  
  # Calculate total number of chunks
  total_chunks <- ceiling(file_size / chunk_size)
  message(sprintf("Uploading file in %d chunks...", total_chunks))
  
  # Upload chunks
  chunk_num <- 0
  bytes_uploaded <- 0
  
  tryCatch({
    repeat {
      # Read chunk
      chunk_data <- readBin(con, "raw", chunk_size)
      if (length(chunk_data) == 0) break
      
      # Encode chunk to base64
      chunk_base64 <- base64enc::base64encode(chunk_data)
      
      # Upload chunk with retry logic
      max_retries <- 3
      retry_count <- 0
      upload_success <- FALSE
      
      while (retry_count < max_retries && !upload_success) {
        chunk_response <- tryCatch({
          chunk_body <- list(
            chunk_number = chunk_num,
            chunk_data = chunk_base64
          )
          
          api_post(config, paste0("/files/upload-chunked/", session_id, "/chunk"), 
                  body = chunk_body)
          
          upload_success <- TRUE
        }, error = function(e) {
          retry_count <<- retry_count + 1
          if (retry_count < max_retries) {
            message(sprintf("  Chunk %d upload failed, retrying (%d/%d)...", 
                           chunk_num, retry_count, max_retries))
            Sys.sleep(1)  # Wait before retry
          }
          NULL
        })
      }
      
      if (!upload_success) {
        # Cancel the upload session
        tryCatch({
          api_request(config, "DELETE", 
                     paste0("/files/upload-chunked/", session_id))
        }, error = function(e) {
          # Ignore errors during cancellation
        })
        stop(sprintf("Failed to upload chunk %d after %d retries", 
                    chunk_num, max_retries))
      }
      
      bytes_uploaded <- bytes_uploaded + length(chunk_data)
      chunk_num <- chunk_num + 1
      
      # Progress report
      progress_pct <- (bytes_uploaded / file_size) * 100
      message(sprintf("  Chunk %d/%d uploaded (%.1f%%)", 
                     chunk_num, total_chunks, progress_pct))
    }
    
    # Finalize upload
    message("Finalizing upload...")
    finalize_body <- list(total_chunks = chunk_num)
    
    finalize_response <- api_post(
      config,
      paste0("/files/upload-chunked/", session_id, "/finalize"),
      body = finalize_body
    )
    
    message("File uploaded successfully!")
    return(TRUE)
    
  }, error = function(e) {
    # Try to cancel the session on error
    tryCatch({
      api_request(config, "DELETE", 
                 paste0("/files/upload-chunked/", session_id))
      message("Upload session cancelled due to error")
    }, error = function(e2) {
      # Ignore errors during cancellation
    })
    
    stop(paste0("Error during chunked upload: ", e$message))
  })
}

#' Cancel a chunked upload session
#'
#' @param config API configuration created by create_api_config
#' @param session_id Session identifier to cancel
#'
#' @return TRUE if the session was successfully cancelled
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' cancel_chunked_upload(config, "session-id-here")
#' }
cancel_chunked_upload <- function(config, session_id) {
  tryCatch({
    api_request(config, "DELETE", 
               paste0("/files/upload-chunked/", session_id))
    message(sprintf("Session %s cancelled successfully", session_id))
    return(TRUE)
  }, error = function(e) {
    stop(paste0("Error cancelling upload: ", e$message))
  })
} 