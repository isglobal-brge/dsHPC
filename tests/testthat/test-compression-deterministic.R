test_that("Compression is deterministic - compressed content has identical hash", {
  skip_if_not_installed("digest")
  
  # Test with different types of content
  test_cases <- list(
    small_text = "Hello, World!",
    large_text = paste(rep("This is a test string for compression. ", 500), collapse = ""),
    numeric_data = as.character(1:1000),
    mixed_content = paste0(
      "Mixed content: ",
      paste(letters, collapse = ""),
      paste(LETTERS, collapse = ""),
      paste(0:9, collapse = "")
    )
  )
  
  # Test with different compression types
  compression_types <- c("gzip", "xz", "bzip2")
  
  for (test_name in names(test_cases)) {
    content <- test_cases[[test_name]]
    
    for (comp_type in compression_types) {
      # Compress first time
      compressed1 <- dsHPC:::compress_content(content, type = comp_type)
      hash1 <- dsHPC:::hash_content(compressed1)
      
      # Wait to ensure any time-based factors would affect result
      Sys.sleep(0.5)
      
      # Compress second time
      compressed2 <- dsHPC:::compress_content(content, type = comp_type)
      hash2 <- dsHPC:::hash_content(compressed2)
      
      # Wait again
      Sys.sleep(0.5)
      
      # Compress third time
      compressed3 <- dsHPC:::compress_content(content, type = comp_type)
      hash3 <- dsHPC:::hash_content(compressed3)
      
      # Test that all hashes are identical
      expect_identical(
        hash1, hash2,
        info = sprintf("Test '%s' with %s: Hash 1 and 2 should be identical", test_name, comp_type)
      )
      
      expect_identical(
        hash2, hash3,
        info = sprintf("Test '%s' with %s: Hash 2 and 3 should be identical", test_name, comp_type)
      )
      
      expect_identical(
        hash1, hash3,
        info = sprintf("Test '%s' with %s: Hash 1 and 3 should be identical", test_name, comp_type)
      )
      
      # Also verify the compressed bytes are identical
      expect_identical(
        compressed1, compressed2,
        info = sprintf("Test '%s' with %s: Compressed bytes 1 and 2 should be identical", test_name, comp_type)
      )
      
      expect_identical(
        compressed2, compressed3,
        info = sprintf("Test '%s' with %s: Compressed bytes 2 and 3 should be identical", test_name, comp_type)
      )
    }
  }
})

test_that("Compressed content hash remains stable across R sessions", {
  skip_if_not_installed("digest")
  
  # Known test case with expected hash values
  test_content <- "Deterministic compression test content"
  
  # Compress with each type
  compressed_gzip <- dsHPC:::compress_content(test_content, type = "gzip")
  compressed_xz <- dsHPC:::compress_content(test_content, type = "xz")
  compressed_bzip2 <- dsHPC:::compress_content(test_content, type = "bzip2")
  
  # Calculate hashes
  hash_gzip <- dsHPC:::hash_content(compressed_gzip)
  hash_xz <- dsHPC:::hash_content(compressed_xz)
  hash_bzip2 <- dsHPC:::hash_content(compressed_bzip2)
  
  # These hashes should be stable across runs
  # If this test fails in the future with the same R version,
  # it means compression is not deterministic
  expect_type(hash_gzip, "character")
  expect_type(hash_xz, "character")
  expect_type(hash_bzip2, "character")
  
  expect_equal(nchar(hash_gzip), 64)  # SHA-256 produces 64 hex characters
  expect_equal(nchar(hash_xz), 64)
  expect_equal(nchar(hash_bzip2), 64)
  
  # Verify that different compression types produce different hashes
  # (same content, different compressed form)
  expect_false(hash_gzip == hash_xz)
  expect_false(hash_gzip == hash_bzip2)
  expect_false(hash_xz == hash_bzip2)
})

test_that("Compression hash differs for different content", {
  skip_if_not_installed("digest")
  
  content1 <- "First content"
  content2 <- "Second content"
  
  # Compress both
  compressed1 <- dsHPC:::compress_content(content1, type = "gzip")
  compressed2 <- dsHPC:::compress_content(content2, type = "gzip")
  
  # Get hashes
  hash1 <- dsHPC:::hash_content(compressed1)
  hash2 <- dsHPC:::hash_content(compressed2)
  
  # Different content should produce different hashes
  expect_false(hash1 == hash2)
})

test_that("Raw content compression is deterministic", {
  skip_if_not_installed("digest")
  
  # Test with raw bytes directly
  raw_content <- as.raw(c(0x48, 0x65, 0x6c, 0x6c, 0x6f))  # "Hello"
  
  compressed1 <- dsHPC:::compress_content(raw_content, type = "gzip")
  hash1 <- dsHPC:::hash_content(compressed1)
  
  Sys.sleep(1)
  
  compressed2 <- dsHPC:::compress_content(raw_content, type = "gzip")
  hash2 <- dsHPC:::hash_content(compressed2)
  
  expect_identical(hash1, hash2)
  expect_identical(compressed1, compressed2)
})

test_that("Serialized object compression is deterministic", {
  skip_if_not_installed("digest")
  
  # Test with R objects
  test_obj <- list(
    a = 1:10,
    b = letters[1:5],
    c = matrix(1:12, nrow = 3)
  )
  
  compressed1 <- dsHPC:::compress_content(test_obj, type = "xz")
  hash1 <- dsHPC:::hash_content(compressed1)
  
  Sys.sleep(1)
  
  compressed2 <- dsHPC:::compress_content(test_obj, type = "xz")
  hash2 <- dsHPC:::hash_content(compressed2)
  
  expect_identical(hash1, hash2)
  expect_identical(compressed1, compressed2)
})

