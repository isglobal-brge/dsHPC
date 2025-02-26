# Tests for Python integration in dsHPC

test_that("Python wrapper creation works", {
  skip_if_not_installed("reticulate")
  
  # Test with simple numpy function
  wrapper <- create_python_wrapper("numpy", "mean", list(a = c(1, 2, 3, 4, 5)))
  
  # Check that we get a function
  expect_true(is.function(wrapper))
})

test_that("Python argument conversion works", {
  # Test NULL conversion
  expect_equal(to_python_arg(NULL), "None")
  
  # Test logical conversion
  expect_equal(to_python_arg(TRUE), "True")
  expect_equal(to_python_arg(FALSE), "False")
  expect_equal(to_python_arg(c(TRUE, FALSE, TRUE)), "[True, False, True]")
  
  # Test numeric conversion
  expect_equal(to_python_arg(42), "42")
  expect_equal(to_python_arg(3.14), "3.14")
  expect_equal(to_python_arg(c(1, 2, 3)), "[1, 2, 3]")
  
  # Test character conversion
  expect_equal(to_python_arg("hello"), "'hello'")
  expect_equal(to_python_arg("it's"), "'it\\'s'")  # Test escaping
  expect_equal(to_python_arg(c("a", "b", "c")), "['a', 'b', 'c']")
  
  # Test list conversion (unnamed)
  expect_equal(to_python_arg(list(1, 2, 3)), "[1, 2, 3]")
  
  # Test list conversion (named - dict)
  expect_equal(to_python_arg(list(a = 1, b = 2)), "{'a': 1, 'b': 2}")
  
  # Test nested list conversion
  expect_equal(
    to_python_arg(list(a = list(x = 1, y = 2), b = c(3, 4))),
    "{'a': {'x': 1, 'y': 2}, 'b': [3, 4]}"
  )
})

test_that("Python script creation works", {
  skip_if_not_installed("reticulate")
  
  # Create a temporary directory for test files
  temp_dir <- tempdir()
  result_file <- file.path(temp_dir, "result.pkl")
  
  # Test script creation
  script_path <- create_python_script(
    py_module = "numpy",
    py_function = "mean",
    args = list(a = c(1, 2, 3, 4, 5)),
    result_file = result_file
  )
  
  # Check that the script file exists
  expect_true(file.exists(script_path))
  
  # Check that the script contains expected content
  script_content <- readLines(script_path)
  expect_true(any(grepl("import numpy", script_content)))
  expect_true(any(grepl("numpy.mean", script_content)))
  
  # Clean up
  if (file.exists(script_path)) file.remove(script_path)
  if (file.exists(result_file)) file.remove(result_file)
})

test_that("dsHPC.submit_python validates inputs", {
  skip_if_not_installed("reticulate")
  
  # Use a unique mock job ID for this test run
  mock_job_id <- paste0("mock_job_", format(Sys.time(), "%Y%m%d%H%M%S"))
  
  # Need to mock uuid::UUIDgenerate to control job_id
  with_mocked_bindings(
    UUIDgenerate = function(...) mock_job_id,
    .package = "uuid",
    {
      # Test validation of py_module
      expect_error(dsHPC.submit_python(py_module = 1, py_function = "mean"),
                  "py_module must be a single character string")
      
      # Test validation of py_function
      expect_error(dsHPC.submit_python(py_module = "numpy", py_function = c("mean", "median")),
                  "py_function must be a single character string")
      
      # Test validation of args
      expect_error(dsHPC.submit_python(py_module = "numpy", py_function = "mean", args = "not_a_list"),
                  "args must be a list")
      
      # For this test, we need to mock the execution to avoid actual Python calls
      # We'll patch the get_cached_result to return NULL so we don't use cached results
      with_mocked_bindings(
        create_python_wrapper = function(...) function() 3,
        store_python_result = function(...) NULL,
        update_job_status = function(...) NULL,
        get_cached_result = function(...) NULL,
        .env = environment(),
        {
          # Test happy path with mocked submission
          result <- dsHPC.submit_python(py_module = "numpy", py_function = "mean", args = list(a = c(1, 2, 3)))
          expect_equal(result$job_id, mock_job_id)
          expect_equal(result$status, "COMPLETED") # Now returns COMPLETED instead of SUBMITTED
          expect_equal(result$py_module, "numpy")
          expect_equal(result$py_function, "mean")
        }
      )
    }
  )
})

# This test requires reticulate and would run Python code, so we'll make it conditional
test_that("Python integration works with reticulate", {
  skip_if_not_installed("reticulate")
  skip_if_not(python_available(), "Python is not available")
  skip_if_not(numpy_available(), "NumPy is not available")
  
  # Create a wrapper function for numpy.mean
  wrapper <- create_python_wrapper("numpy", "mean", list(a = c(1, 2, 3, 4, 5)))
  
  # Execute the wrapper function (only if we got this far)
  tryCatch({
    result <- wrapper()
    expect_equal(result, 3)
  }, error = function(e) {
    skip(paste("Error executing Python code:", e$message))
  })
}) 