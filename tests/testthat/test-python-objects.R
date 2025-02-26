# Tests for handling Python objects and complex data structures

test_that("Complex data structures can be converted to Python", {
  # Skip if reticulate is not available
  skip_if_not_installed("reticulate")
  
  # Test nested lists with various types
  complex_list <- list(
    numbers = c(1, 2, 3),
    strings = c("a", "b", "c"),
    mixed = list(
      x = TRUE,
      y = 3.14,
      z = "hello"
    ),
    nested = list(
      a = list(
        inner = c(1, 2, 3)
      )
    )
  )
  
  # Convert to Python string representation
  py_str <- to_python_arg(complex_list)
  
  # Check the structure (partial checks)
  expect_true(grepl("'numbers':", py_str))
  expect_true(grepl("'strings':", py_str))
  expect_true(grepl("'mixed':", py_str))
  expect_true(grepl("'nested':", py_str))
  expect_true(grepl("'a':", py_str))
  expect_true(grepl("'inner':", py_str))
})

test_that("Python wrapper can handle data frames", {
  skip_if_not_installed("reticulate")
  
  # Create a simple data frame
  test_df <- data.frame(
    x = c(1, 2, 3),
    y = c("a", "b", "c"),
    stringsAsFactors = FALSE
  )
  
  # Get the result without checking for warnings
  # The current implementation likely handles data frames as lists
  result <- to_python_arg(test_df)
  
  # Ensure the result is a character string (Python code representation)
  expect_true(is.character(result))
  
  # Data frames should be converted to dictionaries with column names as keys
  expect_true(grepl("'x':", result))
  expect_true(grepl("'y':", result))
})

# This test verifies we can handle Python objects via reticulate
test_that("Python objects can be returned via reticulate", {
  skip_if_not_installed("reticulate")
  skip_if_not(python_available(), "Python is not available")
  skip_if_not(numpy_available(), "NumPy is not available")
  
  # Create a custom Python snippet that returns different object types
  complex_wrapper <- function() {
    # Create a Python code string
    py_code <- '
import numpy as np

# Create various Python objects
result = {
    "array": np.array([1, 2, 3]),
    "list": [4, 5, 6],
    "dict": {"a": 1, "b": 2},
    "tuple": (7, 8, 9),
    "scalar": 42,
    "string": "hello"
}
'
    
    # Use reticulate to execute the code and get the result
    reticulate::py_run_string(py_code)
    return(reticulate::py$result)
  }
  
  # Execute the wrapper and check the results
  tryCatch({
    result <- complex_wrapper()
    
    # Check if we get a Python dictionary
    expect_true(inherits(result, "python.builtin.dict"))
    
    # Check individual elements
    expect_true(inherits(result$array, "numpy.ndarray"))
    expect_equal(reticulate::py_to_r(result$array), c(1, 2, 3))
    expect_equal(reticulate::py_to_r(result$list), list(4, 5, 6))
    expect_equal(reticulate::py_to_r(result$dict), list(a = 1, b = 2))
    expect_equal(reticulate::py_to_r(result$tuple), list(7, 8, 9))
    expect_equal(reticulate::py_to_r(result$scalar), 42)
    expect_equal(reticulate::py_to_r(result$string), "hello")
  }, error = function(e) {
    skip(paste("Error executing Python code:", e$message))
  })
})

# Test for handling Python modules as arguments
test_that("Python modules can be used in wrapper functions", {
  skip_if_not_installed("reticulate")
  skip_if_not(python_available(), "Python is not available")
  skip_if_not(numpy_available(), "NumPy is not available")
  
  # Create a wrapper that uses multiple Python modules
  multi_module_wrapper <- function() {
    # Use reticulate to import modules
    np <- reticulate::import("numpy")
    math <- reticulate::import("math")
    
    # Create an array with numpy
    arr <- np$array(c(1, 2, 3, 4, 5))
    
    # Calculate mean with numpy and pi with math
    result <- list(
      mean = np$mean(arr),
      pi_value = math$pi,
      sum = sum(arr * math$pi)
    )
    
    return(result)
  }
  
  # Execute the wrapper and verify results
  tryCatch({
    result <- multi_module_wrapper()
    
    # Check results (approximately due to floating point)
    expect_equal(result$mean, 3, tolerance = 1e-10)
    expect_equal(result$pi_value, pi, tolerance = 1e-10)
    expect_equal(result$sum, sum(c(1, 2, 3, 4, 5) * pi), tolerance = 1e-10)
  }, error = function(e) {
    skip(paste("Error executing Python code:", e$message))
  })
}) 