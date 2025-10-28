#' Upload content to the HPC resource
#'
#' @param config API configuration created by create_api_config
#' @param content Content to upload (file path, raw vector, character, or other R object)
#' @param filename Name to use for the uploaded content
#' @param chunk_size_mb Size of each chunk in megabytes (default: 10)
#' @param show_progress Show progress bar (default: TRUE)
#'
#' @return TRUE if the content was successfully uploaded or already exists
#' @export
#'
#' @details
#' This function uses chunked upload for optimal memory efficiency.
#' 
#' - For file paths: Uses hash_file() for incremental hashing, then chunked upload
#' - For R objects: Serializes to raw, then uses chunked upload without temp files
#' - Progress bar is shown by default (requires 'progress' package)
#' 
#' The function automatically detects the content type and handles it appropriately.
#' All uploads go through the chunked system which is memory-efficient regardless
#' of file size.
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 8001, "key",
#'                             auth_header = "X-API-Key", auth_prefix = "")
#' 
#' # Upload file from disk (chunked upload, no memory issues)
#' upload_file(config, "large_dataset.rds", "dataset.rds")
#' 
#' # Upload R object (serialized and chunked)
#' my_data <- data.frame(x = 1:1000000, y = rnorm(1000000))
#' upload_file(config, my_data, "data.rds")
#' 
#' # Upload with custom chunk size
#' upload_file(config, "file.csv", "data.csv", chunk_size_mb = 5)
#' }
upload_file <- function(config, content, filename, chunk_size_mb = 10, show_progress = TRUE) {
  # Check if content is a file path
  if (is.character(content) && length(content) == 1 && file.exists(content)) {
    # Content is a file path - use upload_file_chunked
    # Return the hash of the complete file
    file_hash <- hash_file(content)
    success <- upload_file_chunked(config, content, filename, chunk_size_mb, 
                                   content_type = "application/octet-stream")
    if (!success) {
      stop("Failed to upload file")
    }
    return(file_hash)
  }
  
  # Content is an in-memory object - use upload_object
  # This returns the hash directly
  return(upload_object(config, content, filename, chunk_size_mb, show_progress))
}

#' Upload an R object using chunked upload
#'
#' @param config API configuration created by create_api_config
#' @param obj R object to upload (will be serialized)
#' @param filename Name to use for the uploaded content
#' @param chunk_size_mb Size of each chunk in megabytes (default: 10)
#' @param show_progress Show progress bar (default: TRUE)
#'
#' @return TRUE if the upload was successful
#' @export
#'
#' @details
#' This function uploads R objects using chunked upload without creating temporary files.
#' The object is serialized to raw bytes in memory, then uploaded in chunks.
#' Memory is freed progressively as chunks are uploaded.
#' 
#' Process:
#' 1. Serialize object to raw vector
#' 2. Calculate hash of serialized bytes
#' 3. Check if already exists (deduplication)
#' 4. Upload in chunks, freeing memory after each chunk
#' 5. Show progress bar
#'
#' Peak memory usage: object size + ~1.5x chunk_size
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 8001, "key",
#'                             auth_header = "X-API-Key", auth_prefix = "")
#' 
#' # Upload large data frame
#' big_df <- data.frame(x = 1:1000000, y = rnorm(1000000))
#' upload_object(config, big_df, "big_data.rds")
#' 
#' # Upload list
#' my_list <- list(a = 1:100, b = letters, c = iris)
#' upload_object(config, my_list, "my_list.rds", chunk_size_mb = 5)
#' }
upload_object <- function(config, obj, filename, chunk_size_mb = 10, show_progress = TRUE) {
  if (!requireNamespace("base64enc", quietly = TRUE)) {
    stop("Package 'base64enc' is required. Please install it.")
  }
  
  # IMPORTANT: For R objects, we hash the ORIGINAL object (if it's raw bytes)
  # If obj is already raw bytes (e.g., from readBin), hash those directly
  # If obj is an R object, we'll serialize it but the hash should be of the original data
  
  # Determine if this is raw bytes or needs serialization
  if (is.raw(obj)) {
    # This is already raw bytes (e.g., file loaded with readBin)
    # Calculate hash BEFORE any processing
    message("Calculating hash of original file...")
    file_hash <- hash_content(obj)
    message(sprintf("Hash: %s", file_hash))
    
    # Use the raw bytes directly (no serialization needed)
    serialized <- obj
    total_size <- length(serialized)
    message(sprintf("File size: %.2f MB", total_size / (1024^2)))
  } else {
    # This is an R object that needs serialization
    # For objects, we serialize first, then hash the serialized form
    # (because the "file" IS the serialization)
    message("Serializing R object...")
    serialized <- serialize(obj, NULL)
    total_size <- length(serialized)
    
    message(sprintf("Object serialized: %.2f MB", total_size / (1024^2)))
    
    # Calculate hash of serialized form (this IS the file content)
    file_hash <- hash_content(serialized)
    message(sprintf("Hash: %s", file_hash))
  }
  
  # Check if already exists
  if (hash_exists(config, file_hash)) {
    message("Object already exists in the database (based on hash).")
    rm(serialized)
    gc(verbose = FALSE)
    return(file_hash)  # Return hash instead of TRUE
  }
  
  # Create chunk provider for serialized bytes
  chunk_size <- chunk_size_mb * 1024 * 1024
  
  chunk_provider <- function(chunk_num, chunk_size) {
    start <- chunk_num * chunk_size + 1
    end <- min(start + chunk_size - 1, total_size)
    
    if (start > total_size) return(NULL)
    
    # Extract chunk (indexing, no copy until necessary)
    chunk <- serialized[start:end]
    return(chunk)
  }
  
  # Use upload_with_provider
  result <- upload_with_provider(
    config = config,
    filename = filename,
    total_size = total_size,
    file_hash = file_hash,
    chunk_provider = chunk_provider,
    chunk_size_mb = chunk_size_mb,
    content_type = "application/octet-stream",
    show_progress = show_progress
  )
  
  # Free serialized bytes
  rm(serialized)
  gc(verbose = FALSE)
  
  if (!result) {
    stop("Failed to upload object")
  }
  
  # Return the hash of the complete serialized object (before chunking)
  return(file_hash)
}

#' Calculate hash of a file efficiently
#'
#' @param file_path Path to the file to hash
#'
#' @return SHA-256 hash of the file as a character string
#' @export
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
#' @export
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
  
  # Calculate file hash efficiently (does NOT load file into memory)
  message("Calculating file hash...")
  file_hash <- hash_file(file_path)
  message(sprintf("File hash: %s", file_hash))
  
  # Check if file already exists
  if (hash_exists(config, file_hash)) {
    message("File already exists in the database (based on hash).")
    return(TRUE)
  }
  
  # Detect available memory and adjust chunk size if needed
  gc_info <- gc(verbose = FALSE)
  available_mb <- sum(gc_info[, 4] - gc_info[, 2])  # trigger - used
  
  # Ensure chunk size doesn't exceed 20% of available memory
  # (need ~1.5x for base64 encoding)
  max_safe_chunk_mb <- (available_mb * 0.2) / 1.5
  
  if (chunk_size_mb > max_safe_chunk_mb) {
    old_size <- chunk_size_mb
    chunk_size_mb <- max(1, floor(max_safe_chunk_mb))  # At least 1 MB
    message(sprintf("Adjusting chunk size from %.1f MB to %.1f MB based on available memory",
                   old_size, chunk_size_mb))
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
      
      # Free memory explicitly after each chunk
      rm(chunk_data, chunk_base64, chunk_response)
      gc(verbose = FALSE)
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