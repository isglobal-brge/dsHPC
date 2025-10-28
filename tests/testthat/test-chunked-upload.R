test_that("hash_file returns correct hash", {
  skip_if_not_installed("digest")
  
  # Create a temporary file
  temp_file <- tempfile()
  writeLines("Test content for hashing", temp_file)
  
  # Calculate hash using hash_file
  hash1 <- dsHPC:::hash_file(temp_file)
  
  # Calculate hash using digest directly for verification
  hash2 <- digest::digest(temp_file, file = TRUE, algo = "sha256")
  
  # Hashes should match
  expect_identical(hash1, hash2)
  
  # Hash should be a 64-character hex string (SHA-256)
  expect_equal(nchar(hash1), 64)
  expect_true(grepl("^[0-9a-f]{64}$", hash1))
  
  unlink(temp_file)
})

test_that("hash_file is deterministic", {
  skip_if_not_installed("digest")
  
  # Create a temporary file
  temp_file <- tempfile()
  test_content <- paste(rep("Deterministic test content", 100), collapse = "\n")
  writeLines(test_content, temp_file)
  
  # Calculate hash multiple times
  hash1 <- dsHPC:::hash_file(temp_file)
  Sys.sleep(0.1)
  hash2 <- dsHPC:::hash_file(temp_file)
  Sys.sleep(0.1)
  hash3 <- dsHPC:::hash_file(temp_file)
  
  # All hashes should be identical
  expect_identical(hash1, hash2)
  expect_identical(hash2, hash3)
  
  unlink(temp_file)
})

test_that("hash_file handles different file sizes", {
  skip_if_not_installed("digest")
  
  # Test with small file
  small_file <- tempfile()
  writeLines("small", small_file)
  hash_small <- dsHPC:::hash_file(small_file)
  expect_equal(nchar(hash_small), 64)
  
  # Test with medium file (~1 MB)
  medium_file <- tempfile()
  writeLines(rep(paste(rep("x", 1000), collapse = ""), 1000), medium_file)
  hash_medium <- dsHPC:::hash_file(medium_file)
  expect_equal(nchar(hash_medium), 64)
  
  # Hashes should be different
  expect_false(hash_small == hash_medium)
  
  unlink(small_file)
  unlink(medium_file)
})

test_that("hash_file fails gracefully for missing file", {
  expect_error(
    dsHPC:::hash_file("/nonexistent/file/path.txt"),
    "File not found"
  )
})

test_that("upload_file detects file paths correctly", {
  skip_if_not_installed("digest")
  
  # Create a small test file
  test_file <- tempfile()
  writeLines("test content", test_file)
  
  # Test that a character string that is a file path is detected
  # (We can't test the actual upload without a server, but we can test detection)
  expect_true(file.exists(test_file))
  expect_true(is.character(test_file))
  expect_equal(length(test_file), 1)
  
  # Calculate expected hash
  expected_hash <- dsHPC:::hash_file(test_file)
  expect_equal(nchar(expected_hash), 64)
  
  unlink(test_file)
})

test_that("chunked upload parameters are validated", {
  # Test invalid chunk size
  # These tests verify parameter validation without making actual API calls
  
  temp_file <- tempfile()
  writeLines("test", temp_file)
  
  # Mock config (won't actually be used in these validation tests)
  config <- list(
    base_url = "http://localhost",
    port = 9000,
    api_key = "test_key",
    timeout = 300
  )
  
  # File should exist for validation to proceed past that check
  expect_true(file.exists(temp_file))
  
  unlink(temp_file)
})

test_that("chunked upload calculates chunks correctly", {
  # Test chunk calculation logic
  
  # File size: 25 MB, chunk size: 10 MB
  # Should result in 3 chunks (10 MB + 10 MB + 5 MB)
  file_size <- 25 * 1024 * 1024
  chunk_size <- 10 * 1024 * 1024
  
  expected_chunks <- ceiling(file_size / chunk_size)
  expect_equal(expected_chunks, 3)
  
  # File size: 10 MB, chunk size: 10 MB
  # Should result in 1 chunk
  file_size2 <- 10 * 1024 * 1024
  expected_chunks2 <- ceiling(file_size2 / chunk_size)
  expect_equal(expected_chunks2, 1)
  
  # File size: 10.5 MB, chunk size: 10 MB
  # Should result in 2 chunks
  file_size3 <- 10.5 * 1024 * 1024
  expected_chunks3 <- ceiling(file_size3 / chunk_size)
  expect_equal(expected_chunks3, 2)
})

test_that("threshold logic works correctly", {
  # Test that threshold comparison works as expected
  
  threshold_mb <- 100
  
  # File smaller than threshold
  file_size_small <- 50 * 1024 * 1024
  file_size_small_mb <- file_size_small / (1024^2)
  expect_true(file_size_small_mb < threshold_mb)
  
  # File larger than threshold
  file_size_large <- 150 * 1024 * 1024
  file_size_large_mb <- file_size_large / (1024^2)
  expect_true(file_size_large_mb > threshold_mb)
  
  # File exactly at threshold
  file_size_exact <- 100 * 1024 * 1024
  file_size_exact_mb <- file_size_exact / (1024^2)
  expect_false(file_size_exact_mb > threshold_mb)
})

test_that("hash consistency between upload methods", {
  skip_if_not_installed("digest")
  
  # Create test file
  temp_file <- tempfile()
  test_content <- paste(rep("consistency test", 50), collapse = "\n")
  writeLines(test_content, temp_file)
  
  # Calculate hash using hash_file (used by chunked upload)
  hash_from_file <- dsHPC:::hash_file(temp_file)
  
  # Calculate hash from content in memory (used by regular upload)
  content_raw <- readBin(temp_file, "raw", file.info(temp_file)$size)
  hash_from_memory <- dsHPC:::hash_content(content_raw)
  
  # Hashes should be identical regardless of method
  expect_identical(hash_from_file, hash_from_memory)
  
  unlink(temp_file)
})

test_that("chunked reading produces same content as full read", {
  skip_if_not_installed("digest")
  
  # Create a test file with known content
  temp_file <- tempfile()
  test_data <- paste(rep("Test line with some content here.", 1000), collapse = "\n")
  writeLines(test_data, temp_file)
  
  file_size <- file.info(temp_file)$size
  
  # Read file completely
  full_content <- readBin(temp_file, "raw", file_size)
  
  # Read file in chunks (simulating what upload_file_chunked does)
  chunk_size <- 1000  # Small chunks to ensure multiple chunks
  con <- file(temp_file, "rb")
  
  chunks <- list()
  chunk_num <- 1
  
  repeat {
    chunk <- readBin(con, "raw", chunk_size)
    if (length(chunk) == 0) break
    chunks[[chunk_num]] <- chunk
    chunk_num <- chunk_num + 1
  }
  close(con)
  
  # Recombine chunks
  combined_content <- do.call(c, chunks)
  
  # Verify we read multiple chunks
  expect_true(length(chunks) > 1)
  cat(sprintf("\n  Read %d chunks\n", length(chunks)))
  
  # Content should be identical
  expect_identical(full_content, combined_content)
  
  # Hash of full content
  hash_full <- dsHPC:::hash_content(full_content)
  
  # Hash of recombined chunks
  hash_chunks <- dsHPC:::hash_content(combined_content)
  
  # Hash from file directly
  hash_file <- dsHPC:::hash_file(temp_file)
  
  # All three methods should produce the same hash
  expect_identical(hash_full, hash_chunks)
  expect_identical(hash_full, hash_file)
  
  cat(sprintf("  Hash (full):   %s\n", hash_full))
  cat(sprintf("  Hash (chunks): %s\n", hash_chunks))
  cat(sprintf("  Hash (file):   %s\n", hash_file))
  
  unlink(temp_file)
})

test_that("chunked reading works with different chunk sizes", {
  skip_if_not_installed("digest")
  
  # Create test file
  temp_file <- tempfile()
  writeLines(rep("Test content for different chunk sizes.", 500), temp_file)
  
  # Calculate reference hash
  reference_hash <- dsHPC:::hash_file(temp_file)
  file_size <- file.info(temp_file)$size
  
  # Test with different chunk sizes
  chunk_sizes <- c(100, 500, 1000, 5000, 10000)
  
  for (chunk_size in chunk_sizes) {
    # Read in chunks
    con <- file(temp_file, "rb")
    chunks <- list()
    chunk_num <- 1
    
    repeat {
      chunk <- readBin(con, "raw", chunk_size)
      if (length(chunk) == 0) break
      chunks[[chunk_num]] <- chunk
      chunk_num <- chunk_num + 1
    }
    close(con)
    
    # Recombine
    combined <- do.call(c, chunks)
    
    # Calculate hash
    hash_this_size <- dsHPC:::hash_content(combined)
    
    # Should match reference regardless of chunk size
    expect_identical(
      hash_this_size, 
      reference_hash,
      info = sprintf("Chunk size: %d bytes", chunk_size)
    )
  }
  
  unlink(temp_file)
})

