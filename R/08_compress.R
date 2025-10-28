#' Compress content deterministically
#'
#' @param content Content to compress (raw vector, character, or other object)
#' @param type Compression type: "xz" (default, best compression), "gzip" (faster), or "bzip2"
#'
#' @return Raw vector with compressed content (deterministic - same input always produces same output)
#'
#' @details
#' This function provides deterministic compression, meaning that compressing the same
#' content multiple times will always produce the same compressed output and hash.
#' This is essential for content-based caching systems.
#' 
#' Compression types:
#' - "xz": Best compression ratio, deterministic, slower (recommended for large files)
#' - "gzip": Good compression, deterministic, faster (recommended for general use)
#' - "bzip2": Medium compression, deterministic
#'
#' @examples
#' \dontrun{
#' content <- "Hello, World!"
#' compressed <- compress_content(content, type = "xz")
#' hash1 <- hash_content(compressed)
#' Sys.sleep(5)
#' compressed2 <- compress_content(content, type = "xz")
#' hash2 <- hash_content(compressed2)
#' identical(hash1, hash2)  # TRUE
#' }
compress_content <- function(content, type = c("gzip", "xz", "bzip2")) {
  type <- match.arg(type)
  
  # Convert content to raw bytes if it's not already
  if (!is.raw(content)) {
    if (is.character(content)) {
      content <- charToRaw(paste(content, collapse = "\n"))
    } else {
      content <- serialize(content, NULL)
    }
  }
  
  # Use memCompress which is deterministic for all supported types
  compressed <- memCompress(content, type = type)
  
  return(compressed)
}

#' Decompress content that was compressed with compress_content
#'
#' @param compressed_content Compressed raw vector
#' @param type Compression type used: "xz", "gzip", or "bzip2" (must match compression type)
#'
#' @return Raw vector with decompressed content
#'
#' @examples
#' \dontrun{
#' content <- "Hello, World!"
#' compressed <- compress_content(content, type = "gzip")
#' decompressed <- decompress_content(compressed, type = "gzip")
#' rawToChar(decompressed)  # "Hello, World!"
#' }
decompress_content <- function(compressed_content, type = c("gzip", "xz", "bzip2")) {
  type <- match.arg(type)
  
  if (!is.raw(compressed_content)) {
    stop("compressed_content must be a raw vector")
  }
  
  decompressed <- memDecompress(compressed_content, type = type)
  
  return(decompressed)
}

#' Upload compressed content to the HPC resource
#'
#' @param config API configuration created by create_api_config
#' @param content Content to compress and upload (raw vector, character, or other object)
#' @param filename Name to use for the uploaded content (will have compression extension added)
#' @param compression_type Compression type: "gzip" (default), "xz", or "bzip2"
#' @param compress Whether to compress the content (default TRUE)
#'
#' @return TRUE if the content was successfully uploaded or already exists
#'
#' @details
#' This function compresses content deterministically before uploading, which means:
#' - Same content will always produce the same hash
#' - Content is deduplicated effectively in the cache
#' - Reduced bandwidth usage
#' - The server will need to decompress the content when needed
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' content <- "Hello, World!"
#' upload_file_compressed(config, content, "hello.txt", compression_type = "gzip")
#' }
upload_file_compressed <- function(config, content, filename, 
                                   compression_type = c("gzip", "xz", "bzip2"),
                                   compress = TRUE) {
  if (!requireNamespace("base64enc", quietly = TRUE)) {
    stop("Package 'base64enc' is required. Please install it.")
  }
  
  compression_type <- match.arg(compression_type)
  
  # Convert content to raw bytes if it's not already
  if (!is.raw(content)) {
    if (is.character(content)) {
      content <- charToRaw(paste(content, collapse = "\n"))
    } else {
      content <- serialize(content, NULL)
    }
  }
  
  original_size <- length(content)
  
  # Compress if requested
  if (compress) {
    content <- compress_content(content, type = compression_type)
    compressed_size <- length(content)
    compression_ratio <- (1 - compressed_size / original_size) * 100
    
    message(sprintf("Compressed: %.2f KB -> %.2f KB (%.1f%% reduction)", 
                    original_size / 1024, compressed_size / 1024, compression_ratio))
    
    # Add compression extension to filename
    extension <- switch(compression_type,
                       "gzip" = ".gz",
                       "xz" = ".xz",
                       "bzip2" = ".bz2")
    filename <- paste0(filename, extension)
  }
  
  # Compute content hash AFTER compression
  file_hash <- hash_content(content)
  
  # Check if content already exists
  if (hash_exists(config, file_hash)) {
    message("Content already exists in the database.")
    return(TRUE)
  }
  
  # Use upload_object to handle the compressed content (always uses chunked system)
  # The content is already in raw format (compressed)
  return(upload_object(config, content, filename, chunk_size_mb = 10, show_progress = TRUE))
}

#' Get compression statistics for content
#'
#' @param content Content to analyze (raw vector, character, or other object)
#' @param types Vector of compression types to test (default: all supported types)
#'
#' @return Data frame with compression statistics for each type
#'
#' @examples
#' \dontrun{
#' content <- paste(rep("Hello, World!", 1000), collapse = "\n")
#' stats <- get_compression_stats(content)
#' print(stats)
#' }
get_compression_stats <- function(content, types = c("gzip", "xz", "bzip2")) {
  # Convert content to raw bytes if it's not already
  if (!is.raw(content)) {
    if (is.character(content)) {
      content <- charToRaw(paste(content, collapse = "\n"))
    } else {
      content <- serialize(content, NULL)
    }
  }
  
  original_size <- length(content)
  
  results <- data.frame(
    type = character(),
    original_size_kb = numeric(),
    compressed_size_kb = numeric(),
    compression_ratio_pct = numeric(),
    time_seconds = numeric(),
    stringsAsFactors = FALSE
  )
  
  for (type in types) {
    start_time <- Sys.time()
    compressed <- compress_content(content, type = type)
    end_time <- Sys.time()
    
    compressed_size <- length(compressed)
    compression_ratio <- (1 - compressed_size / original_size) * 100
    time_taken <- as.numeric(difftime(end_time, start_time, units = "secs"))
    
    results <- rbind(results, data.frame(
      type = type,
      original_size_kb = original_size / 1024,
      compressed_size_kb = compressed_size / 1024,
      compression_ratio_pct = compression_ratio,
      time_seconds = time_taken,
      stringsAsFactors = FALSE
    ))
  }
  
  return(results)
}

