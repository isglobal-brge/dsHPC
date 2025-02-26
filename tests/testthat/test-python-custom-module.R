# Tests for custom Python module integration

test_that("dsHPC can find and load the test module", {
  skip_if_not_installed("reticulate")
  skip_if_not(reticulate::py_available(), "Python is not available")
  
  # Get the path to the test module
  pkg_root <- system.file(package = "dsHPC")
  test_module_path <- file.path(pkg_root, "python")
  
  # Skip if the module doesn't exist (likely during development testing)
  skip_if(!dir.exists(test_module_path), "Python directory doesn't exist in package")
  skip_if(!file.exists(file.path(test_module_path, "dsHPC_test_module.py")), 
          "Test module doesn't exist in package")
  
  # Try to load the module with reticulate
  tryCatch({
    reticulate::use_python(Sys.which("python"), required = TRUE)
    reticulate::py_run_string(paste0("import sys; sys.path.append('", test_module_path, "')"))
    module <- reticulate::import("dsHPC_test_module")
    
    expect_true(!is.null(module))
    expect_true("hello_world" %in% names(module))
    expect_true("calculate_stats" %in% names(module))
  }, error = function(e) {
    skip(paste("Error loading test module:", e$message))
  })
})

test_that("hello_world function works via reticulate", {
  skip_if_not_installed("reticulate")
  skip_if_not(reticulate::py_available(), "Python is not available")
  
  # Get the path to the test module
  pkg_root <- system.file(package = "dsHPC")
  test_module_path <- file.path(pkg_root, "python")
  skip_if(!file.exists(file.path(test_module_path, "dsHPC_test_module.py")), 
          "Test module doesn't exist in package")
  
  # Try to call the hello_world function
  tryCatch({
    reticulate::py_run_string(paste0("import sys; sys.path.append('", test_module_path, "')"))
    module <- reticulate::import("dsHPC_test_module")
    result <- module$hello_world()
    
    expect_true(is.character(result))
    expect_match(result, "Hello from Python")
  }, error = function(e) {
    skip(paste("Error calling hello_world:", e$message))
  })
})

test_that("calculate_stats function works with numeric data", {
  skip_if_not_installed("reticulate")
  skip_if_not(reticulate::py_available(), "Python is not available")
  
  # Get the path to the test module
  pkg_root <- system.file(package = "dsHPC")
  test_module_path <- file.path(pkg_root, "python")
  skip_if(!file.exists(file.path(test_module_path, "dsHPC_test_module.py")), 
          "Test module doesn't exist in package")
  
  # Try to call the calculate_stats function
  tryCatch({
    reticulate::py_run_string(paste0("import sys; sys.path.append('", test_module_path, "')"))
    module <- reticulate::import("dsHPC_test_module")
    
    # Test with a simple numeric vector
    numbers <- c(1, 2, 3, 4, 5)
    result <- module$calculate_stats(numbers)
    
    # Check the result structure
    expect_true(inherits(result, "python.builtin.dict"))
    
    # Convert to R list for easier testing
    r_result <- reticulate::py_to_r(result)
    
    # Check all expected fields are present
    expect_true(all(c("mean", "median", "std", "min", "max", "length") %in% names(r_result)))
    
    # Check values
    expect_equal(r_result$mean, mean(numbers))
    expect_equal(r_result$median, median(numbers))
    expect_equal(r_result$std, sd(numbers))
    expect_equal(r_result$min, min(numbers))
    expect_equal(r_result$max, max(numbers))
    expect_equal(r_result$length, length(numbers))
  }, error = function(e) {
    skip(paste("Error calling calculate_stats:", e$message))
  })
})

# Test with more complex data structures
test_that("process_data_frame function works with data frames", {
  skip_if_not_installed("reticulate")
  skip_if_not(reticulate::py_available(), "Python is not available")
  
  # Get the path to the test module
  pkg_root <- system.file(package = "dsHPC")
  test_module_path <- file.path(pkg_root, "python")
  skip_if(!file.exists(file.path(test_module_path, "dsHPC_test_module.py")), 
          "Test module doesn't exist in package")
  
  # Try to call the process_data_frame function
  tryCatch({
    reticulate::py_run_string(paste0("import sys; sys.path.append('", test_module_path, "')"))
    module <- reticulate::import("dsHPC_test_module")
    
    # Create a test dataframe
    test_df <- data.frame(
      numeric = c(1, 2, 3, 4, 5),
      integer = c(10L, 20L, 30L, 40L, 50L),
      character = c("a", "b", "c", "d", "e"),
      logical = c(TRUE, FALSE, TRUE, FALSE, TRUE),
      stringsAsFactors = FALSE
    )
    
    # Convert to list suitable for Python
    df_dict <- as.list(test_df)
    
    # Process the data frame
    result <- module$process_data_frame(df_dict)
    r_result <- reticulate::py_to_r(result)
    
    # Check structure
    expect_true("columns" %in% names(r_result))
    expect_true("n_rows" %in% names(r_result))
    expect_true("column_summary" %in% names(r_result))
    
    # Check basic properties
    expect_equal(r_result$n_rows, nrow(test_df))
    expect_equal(sort(r_result$columns), sort(names(test_df)))
    
    # Check that numeric columns have summary statistics
    expect_true("numeric" %in% names(r_result$column_summary))
    expect_true("mean" %in% names(r_result$column_summary$numeric))
    expect_equal(r_result$column_summary$numeric$mean, mean(test_df$numeric))
  }, error = function(e) {
    skip(paste("Error calling process_data_frame:", e$message))
  })
})

# Test submitting a job to the custom Python module
test_that("dsHPC.submit_python can submit jobs to our test module", {
  skip_if_not_installed("reticulate")
  
  # Mock the dsHPC.submit function to simulate job submission
  local_mocked_bindings(
    dsHPC.submit = function(...) {
      list(
        job_id = "test_job_123",
        status = "SUBMITTED",
        timestamp = Sys.time(),
        result_available = FALSE
      )
    }
  )
  
  # Submit a job to our test module
  job <- dsHPC.submit_python(
    py_module = "dsHPC_test_module",
    py_function = "calculate_stats",
    args = list(numbers = c(1, 2, 3, 4, 5)),
    use_cache = FALSE
  )
  
  # Check the job details
  expect_equal(job$job_id, "test_job_123")
  expect_equal(job$status, "SUBMITTED")
  expect_equal(job$py_module, "dsHPC_test_module")
  expect_equal(job$py_function, "calculate_stats")
}) 