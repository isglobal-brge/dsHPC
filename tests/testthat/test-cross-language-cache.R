# Test file for cross-language caching between R and Python

# Helper functions for isolated test environments
setup_isolated_test_db <- function() {
  # Create a temporary directory for the test database
  test_db_dir <- tempfile("dsHPC_test_")
  dir.create(test_db_dir, recursive = TRUE)
  
  # Create database path with a unique name
  test_db_path <- file.path(test_db_dir, "test_db.sqlite")
  
  # Initialize with this database
  dsHPC.init(db_path = test_db_path)
  
  # Return the paths for cleanup
  return(list(db_dir = test_db_dir, db_path = test_db_path))
}

teardown_isolated_test_db <- function(paths) {
  # Clean up the database
  if (file.exists(paths$db_path)) {
    DBI::dbDisconnect(getOption("dsHPC.config")$connection)
    unlink(paths$db_dir, recursive = TRUE)
  }
}

test_that("equivalent functions in R and Python use proper caching", {
  skip_if_not_installed("reticulate")
  skip_if_not(reticulate::py_module_available("numpy"), "NumPy is not available")
  
  # Create isolated test environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Define equivalent functions in R and Python
  # In R: mean function
  # In Python: numpy.mean
  
  # First, submit the R job
  r_job <- dsHPC.submit(
    func = mean,
    args = list(x = c(1, 2, 3, 4, 5)),
    use_cache = TRUE
  )
  
  # Check that it was computed
  expect_equal(r_job$status, "COMPLETED")
  expect_equal(r_job$result, 3)
  
  # Store the job hash for comparison
  r_job_hash <- r_job$job_hash
  
  # Now submit a Python job that does the same calculation
  # Note: the R and Python implementations might have different internal logic,
  # so they might not share the cache unless we explicitly implemented that
  py_job <- dsHPC.submit_python(
    py_module = "numpy",
    py_function = "mean",
    args = list(a = c(1, 2, 3, 4, 5)),
    use_cache = TRUE
  )
  
  # Check the Python job result
  expect_equal(py_job$result, 3)
  
  # Even though they do the same calculation, currently they would have different hashes
  # because they're different implementations - this is an observation test
  expect_false(identical(r_job_hash, py_job$job_hash))
  
  # Now test that re-submitting the same calculation type reuses the cache
  
  # Re-submit R job
  r_job2 <- dsHPC.submit(
    func = mean,
    args = list(x = c(1, 2, 3, 4, 5)),
    use_cache = TRUE
  )
  
  # Check it used the cache
  expect_equal(r_job2$status, "CACHED")
  expect_equal(r_job2$result, 3)
  
  # Re-submit Python job
  py_job2 <- dsHPC.submit_python(
    py_module = "numpy", 
    py_function = "mean",
    args = list(a = c(1, 2, 3, 4, 5)),
    use_cache = TRUE
  )
  
  # Check it used the cache
  expect_equal(py_job2$status, "CACHED")
  expect_equal(py_job2$result, 3)
})

test_that("different function implementations across languages are detected", {
  skip_if_not_installed("reticulate")
  skip_if_not(reticulate::py_module_available("numpy"), "NumPy is not available")
  
  # Create isolated test environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Define slightly different functions in R and Python
  # In R: mean function with trim
  # In Python: numpy.mean with no trim
  
  # First, submit the R job with trimming
  r_job <- dsHPC.submit(
    func = mean,
    args = list(x = c(1, 2, 3, 4, 5), trim = 0.2), # Trim the extremes
    use_cache = TRUE
  )
  
  # The result should be 3 (with trim=0.2, the result is still 3 for this simple vector)
  expect_equal(r_job$status, "COMPLETED")
  expect_equal(r_job$result, 3)
  
  # Now submit a Python job with a slightly different calculation
  # In numpy, there's no direct equivalent to trim, so we use a different approach
  py_job <- dsHPC.submit_python(
    py_module = "numpy",
    py_function = "mean",
    args = list(a = c(2, 3, 4)), # We manually excluded the extremes
    use_cache = TRUE
  )
  
  # The result should be the same, but with a different calculation method
  expect_equal(py_job$status, "COMPLETED")
  expect_equal(py_job$result, 3)
  
  # The hashes should be different due to different arguments and implementations
  expect_false(identical(r_job$job_hash, py_job$job_hash))
  
  # Re-running the original function should use cache even with whitespace changes
  addition_fn <- function(x, y) { x + y }
  
  # First submit with the original function
  add_job1 <- dsHPC.submit(
    func = addition_fn,
    args = list(x = 1, y = 2),
    use_cache = TRUE
  )
  
  expect_equal(add_job1$status, "COMPLETED")
  expect_equal(add_job1$result, 3)
  
  # Define a functionally identical function with different whitespace
  addition_fn2 <- function(x, y) {
    # This is the same function with different formatting
    x + y
  }
  
  # Submit the slightly reformatted function
  add_job2 <- dsHPC.submit(
    func = addition_fn2,
    args = list(x = 1, y = 2),
    use_cache = TRUE
  )
  
  # The result should be cached based on the function body
  # This checks our enhanced function comparison
  expect_equal(add_job2$status, "CACHED")
  expect_equal(add_job2$result, 3)
}) 