#' Initialize a chunked upload session (Layer 1: Imperative API)
#'
#' @param config API configuration created by create_api_config
#' @param file_hash SHA-256 hash of the complete file
#' @param total_size Total size of the file in bytes
#' @param filename Name for the uploaded file
#' @param chunk_size_mb Size of each chunk in megabytes (default: 10)
#' @param content_type MIME type (default: "application/octet-stream")
#' @param metadata Optional metadata dictionary
#'
#' @return List with session_id, file_hash, and message
#' @export
#'
#' @details
#' This is a Layer 1 (imperative) API function that gives you complete control
#' over the chunked upload process. After initializing a session, you must:
#' 1. Upload chunks using upload_chunk()
#' 2. Finalize the session using finalize_chunked_upload()
#' Or cancel with cancel_chunked_upload() if needed.
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 8001, "key", 
#'                             auth_header = "X-API-Key", auth_prefix = "")
#' file_hash <- hash_file("data.rds")
#' file_size <- file.info("data.rds")$size
#' 
#' session <- init_chunked_upload(config, file_hash, file_size, "data.rds")
#' # Then upload chunks manually...
#' }
init_chunked_upload <- function(config, file_hash, total_size, filename,
                               chunk_size_mb = 10,
                               content_type = "application/octet-stream",
                               metadata = NULL) {
  # Validate inputs
  if (!is.character(file_hash) || length(file_hash) != 1) {
    stop("file_hash must be a single character string")
  }
  
  if (!is.numeric(total_size) || total_size <= 0) {
    stop("total_size must be a positive number")
  }
  
  # Calculate chunk size in bytes
  chunk_size <- as.integer(chunk_size_mb * 1024 * 1024)
  
  # Create request body
  body <- list(
    file_hash = file_hash,
    filename = filename,
    content_type = content_type,
    total_size = as.integer(total_size),
    chunk_size = chunk_size,
    metadata = metadata
  )
  
  # Make API call
  response <- api_post(config, "/files/upload-chunked/init", body = body)
  
  return(response)
}

#' Upload a single chunk (Layer 1: Imperative API)
#'
#' @param config API configuration created by create_api_config
#' @param session_id Session identifier from init_chunked_upload()
#' @param chunk_number Sequential chunk number (0-based)
#' @param chunk_data Raw vector containing the chunk data
#'
#' @return List with session_id, chunk_number, chunks_received, and message
#' @export
#'
#' @details
#' This is a Layer 1 (imperative) API function. You have complete control over
#' which chunks to upload and when. The chunk_data will be automatically base64
#' encoded before transmission.
#'
#' @examples
#' \dontrun{
#' # After initializing session
#' chunk <- readBin(file("data.rds", "rb"), "raw", 1024*1024)
#' result <- upload_chunk(config, session$session_id, 0, chunk)
#' cat("Chunks received:", result$chunks_received, "\n")
#' }
upload_chunk <- function(config, session_id, chunk_number, chunk_data) {
  if (!requireNamespace("base64enc", quietly = TRUE)) {
    stop("Package 'base64enc' is required. Please install it.")
  }
  
  # Validate inputs
  if (!is.character(session_id) || length(session_id) != 1) {
    stop("session_id must be a single character string")
  }
  
  if (!is.numeric(chunk_number) || chunk_number < 0) {
    stop("chunk_number must be a non-negative number")
  }
  
  if (!is.raw(chunk_data)) {
    stop("chunk_data must be a raw vector")
  }
  
  # Encode chunk to base64
  chunk_base64 <- base64enc::base64encode(chunk_data)
  
  # Create request body
  body <- list(
    chunk_number = as.integer(chunk_number),
    chunk_data = chunk_base64
  )
  
  # Make API call
  response <- api_post(config, paste0("/files/upload-chunked/", session_id, "/chunk"),
                      body = body)
  
  return(response)
}

#' Finalize a chunked upload session (Layer 1: Imperative API)
#'
#' @param config API configuration created by create_api_config
#' @param session_id Session identifier from init_chunked_upload()
#' @param total_chunks Total number of chunks uploaded
#'
#' @return File response with file_hash, filename, and other metadata
#' @export
#'
#' @details
#' This is a Layer 1 (imperative) API function. Call this after uploading all
#' chunks to finalize the upload. The server will combine all chunks, verify
#' the hash, and store the file. Temporary chunks are automatically cleaned up.
#'
#' @examples
#' \dontrun{
#' # After uploading all chunks
#' result <- finalize_chunked_upload(config, session$session_id, 5)
#' cat("Upload finalized:", result$file_hash, "\n")
#' }
finalize_chunked_upload <- function(config, session_id, total_chunks) {
  # Validate inputs
  if (!is.character(session_id) || length(session_id) != 1) {
    stop("session_id must be a single character string")
  }
  
  if (!is.numeric(total_chunks) || total_chunks <= 0) {
    stop("total_chunks must be a positive number")
  }
  
  # Create request body
  body <- list(
    total_chunks = as.integer(total_chunks)
  )
  
  # Make API call
  response <- api_post(config, paste0("/files/upload-chunked/", session_id, "/finalize"),
                      body = body)
  
  return(response)
}

#' Get status of a chunked upload session (Layer 1: Imperative API)
#'
#' @param config API configuration created by create_api_config
#' @param session_id Session identifier from init_chunked_upload()
#'
#' @return List with session info including chunks_received, status, etc.
#' @export
#'
#' @details
#' This is a Layer 1 (imperative) API function. Use this to check the status
#' of an active upload session. Useful for debugging, implementing resume logic,
#' or monitoring progress from another process.
#'
#' @examples
#' \dontrun{
#' status <- get_upload_session_status(config, session$session_id)
#' cat("Chunks received:", status$chunks_received, "\n")
#' cat("Session status:", status$status, "\n")
#' }
get_upload_session_status <- function(config, session_id) {
  # Validate input
  if (!is.character(session_id) || length(session_id) != 1) {
    stop("session_id must be a single character string")
  }
  
  # Make API call
  response <- api_request(config, paste0("/files/upload-chunked/", session_id, "/status"), "GET")
  
  return(response)
}

#' Upload with custom chunk provider (Layer 2: Functional API)
#'
#' @param config API configuration created by create_api_config
#' @param filename Name for the uploaded file
#' @param total_size Total size of the file in bytes
#' @param file_hash SHA-256 hash of the complete file
#' @param chunk_provider Function that returns chunks: function(chunk_num, chunk_size) -> raw or NULL
#' @param chunk_size_mb Size of each chunk in megabytes (default: 10)
#' @param content_type MIME type (default: "application/octet-stream")
#' @param metadata Optional metadata dictionary
#' @param on_progress Optional callback function(chunk_num, total_chunks, percent)
#' @param show_progress Show progress bar (default: TRUE)
#' @param max_retries Maximum retries per chunk (default: 3)
#'
#' @return TRUE if upload successful
#' @export
#'
#' @details
#' This is a Layer 2 (functional) API that manages the session lifecycle automatically.
#' You provide a chunk_provider function that knows how to extract each chunk.
#' The system handles session management, retry logic, progress reporting, and cleanup.
#'
#' The chunk_provider function receives (chunk_num, chunk_size) and should return:
#' - A raw vector with the chunk data, or
#' - NULL when there are no more chunks
#'
#' @examples
#' \dontrun{
#' # Example: Upload from a serialized object in memory
#' obj <- list(data = 1:1000000)
#' serialized <- serialize(obj, NULL)
#' file_hash <- hash_content(serialized)
#'
#' # Chunk provider that extracts from serialized bytes
#' provider <- function(chunk_num, chunk_size) {
#'   start <- chunk_num * chunk_size + 1
#'   end <- min(start + chunk_size - 1, length(serialized))
#'   if (start > length(serialized)) return(NULL)
#'   serialized[start:end]
#' }
#'
#' upload_with_provider(config, "data.rds", length(serialized), 
#'                      file_hash, provider)
#' }
upload_with_provider <- function(config, filename, total_size, file_hash,
                                 chunk_provider,
                                 chunk_size_mb = 10,
                                 content_type = "application/octet-stream",
                                 metadata = NULL,
                                 on_progress = NULL,
                                 show_progress = TRUE,
                                 max_retries = 3) {
  # Validate chunk_provider is a function
  if (!is.function(chunk_provider)) {
    stop("chunk_provider must be a function")
  }
  
  # Check for progress package if needed
  use_progress_bar <- show_progress && requireNamespace("progress", quietly = TRUE)
  
  # Calculate total chunks
  chunk_size <- chunk_size_mb * 1024 * 1024
  total_chunks <- ceiling(total_size / chunk_size)
  
  # Initialize progress bar
  if (use_progress_bar) {
    pb <- progress::progress_bar$new(
      format = "  Uploading [:bar] :percent eta: :eta",
      total = total_chunks,
      clear = FALSE,
      width = 60
    )
  }
  
  # Initialize session
  session <- tryCatch({
    init_chunked_upload(config, file_hash, total_size, filename,
                       chunk_size_mb, content_type, metadata)
  }, error = function(e) {
    stop(paste0("Failed to initialize upload session: ", e$message))
  })
  
  session_id <- session$session_id
  
  # Upload chunks
  chunk_num <- 0
  
  tryCatch({
    repeat {
      # Get chunk from provider
      chunk_data <- chunk_provider(chunk_num, chunk_size)
      
      # NULL means no more chunks
      if (is.null(chunk_data)) break
      
      if (!is.raw(chunk_data)) {
        stop("chunk_provider must return a raw vector or NULL")
      }
      
      # Upload chunk with retry logic
      retry_count <- 0
      upload_success <- FALSE
      
      while (retry_count < max_retries && !upload_success) {
        upload_success <- tryCatch({
          result <- upload_chunk(config, session_id, chunk_num, chunk_data)
          TRUE
        }, error = function(e) {
          retry_count <- retry_count + 1
          if (retry_count < max_retries) {
            message(sprintf("Chunk %d upload failed, retrying (%d/%d)...", 
                           chunk_num, retry_count, max_retries))
            Sys.sleep(1)
          }
          FALSE
        })
      }
      
      if (!upload_success) {
        # Cancel session on failure
        cancel_chunked_upload(config, session_id)
        stop(sprintf("Failed to upload chunk %d after %d retries", chunk_num, max_retries))
      }
      
      # Update progress
      if (use_progress_bar) {
        pb$tick()
      }
      
      # Call progress callback if provided
      if (!is.null(on_progress)) {
        percent <- ((chunk_num + 1) / total_chunks) * 100
        on_progress(chunk_num + 1, total_chunks, percent)
      }
      
      # Free memory
      rm(chunk_data)
      gc(verbose = FALSE)
      
      chunk_num <- chunk_num + 1
    }
    
    # Finalize upload
    if (use_progress_bar) {
      cat("\n")  # New line after progress bar
    }
    
    result <- finalize_chunked_upload(config, session_id, chunk_num)
    
    return(TRUE)
    
  }, error = function(e) {
    # Try to cancel session on error
    tryCatch({
      cancel_chunked_upload(config, session_id)
    }, error = function(e2) {
      # Ignore errors during cleanup
    })
    
    stop(paste0("Error during chunked upload: ", e$message))
  })
}

