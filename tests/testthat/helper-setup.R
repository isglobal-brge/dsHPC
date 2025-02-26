# Helper functions for testing

# Initialize a clean test environment
setup_test_environment <- function() {
  # Create a temporary database for testing
  test_db_path <- tempfile(fileext = ".sqlite")
  
  # Initialize dsHPC with the test database
  config <- dsHPC.init(db_path = test_db_path)
  
  # Return the config and paths for cleanup
  return(list(
    config = config,
    db_path = test_db_path
  ))
}

# Helper functions for Python tests
python_available <- function() {
  tryCatch({
    reticulate::py_available()
  }, error = function(e) {
    return(FALSE)
  })
}

numpy_available <- function() {
  tryCatch({
    if (!reticulate::py_available()) {
      return(FALSE)
    }
    np <- reticulate::import("numpy", delay_load = TRUE)
    return(TRUE)
  }, error = function(e) {
    return(FALSE)
  })
}

# Setup executed before each test file
test_setup <- function() {
  # Create a fresh test environment
  test_env <- setup_test_environment()
  
  # Store paths for cleanup
  options(dsHPC.test_env = test_env)
}

# Teardown executed after each test file
test_teardown <- function() {
  # Get test environment
  test_env <- getOption("dsHPC.test_env")
  
  # Clean up database connection
  if (!is.null(test_env$config$connection)) {
    DBI::dbDisconnect(test_env$config$connection)
  }
  
  # Remove test database file
  if (file.exists(test_env$db_path)) {
    unlink(test_env$db_path)
  }
  
  # Reset options
  options(dsHPC.config = NULL)
  options(dsHPC.test_env = NULL)
}

# Run setup
test_setup() 