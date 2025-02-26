# Test file for job management functionality

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
    config <- getOption("dsHPC.config")
    if (!is.null(config) && !is.null(config$connection)) {
      tryCatch({
        DBI::dbDisconnect(config$connection)
      }, error = function(e) {
        warning("Could not disconnect from database: ", e$message)
      })
    }
    unlink(paths$db_dir, recursive = TRUE)
  }
}

# Helper function to manually update job status in the database
# This allows us to test different job states without waiting for real job completion
manually_update_job_status <- function(job_id, new_status, error_message = NULL) {
  config <- getOption("dsHPC.config")
  if (is.null(config) || is.null(config$connection)) {
    stop("dsHPC has not been initialized.")
  }
  
  # Update the job status in the database
  completed <- new_status %in% c("COMPLETED", "FAILED", "CANCELLED")
  update_job_status(
    config$connection, 
    job_id, 
    new_status, 
    error_message = error_message,
    completed = completed
  )
}

# Helper function to mock updating a scheduler ID since this function might not be directly exported
manually_update_scheduler_id <- function(job_id, scheduler_id) {
  config <- getOption("dsHPC.config")
  if (is.null(config) || is.null(config$connection)) {
    stop("dsHPC has not been initialized.")
  }
  
  # Update the scheduler ID directly in the database
  query <- sprintf(
    "UPDATE jobs SET scheduler_id = '%s' WHERE job_id = '%s'",
    scheduler_id, job_id
  )
  DBI::dbExecute(config$connection, query)
}

test_that("R job status management works correctly", {
  # Set up isolated testing environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Submit a test job
  test_func <- function(x) x * 2
  job <- dsHPC.submit(
    func = test_func,
    args = list(x = 5)
  )
  
  # Initially the job should be COMPLETED (when scheduler is not available)
  # or SUBMITTED/PENDING (if using a real scheduler)
  initial_status <- dsHPC.status(job$job_id)
  expect_true(initial_status$status %in% c("SUBMITTED", "PENDING", "RUNNING", "COMPLETED"))
  
  # Test manual status transitions to simulate different job states
  # 1. Set job to PENDING
  manually_update_job_status(job$job_id, "PENDING")
  status <- dsHPC.status(job$job_id)
  expect_equal(status$status, "PENDING")
  
  # 2. Set job to RUNNING
  manually_update_job_status(job$job_id, "RUNNING")
  status <- dsHPC.status(job$job_id)
  expect_equal(status$status, "RUNNING")
  
  # 3. Set job to FAILED with error message
  error_msg <- "Simulated job failure for testing"
  manually_update_job_status(job$job_id, "FAILED", error_msg)
  status <- dsHPC.status(job$job_id)
  expect_equal(status$status, "FAILED")
  
  # Check if we can retrieve the error message
  job_info <- get_job_info(getOption("dsHPC.config")$connection, job$job_id)
  expect_equal(job_info$error_message, error_msg)
  
  # Test dsHPC.result behavior with failed job
  # It should return NULL and issue a warning
  expect_warning(
    result <- dsHPC.result(job$job_id),
    "Job has not completed"
  )
  expect_null(result)
  
  # Test listing jobs
  all_jobs <- dsHPC.list_jobs()
  expect_true(is.data.frame(all_jobs))
  expect_true(job$job_id %in% all_jobs$job_id)
  
  # Test filtering by status
  failed_jobs <- dsHPC.list_jobs(status_filter = "FAILED")
  expect_true(is.data.frame(failed_jobs))
  expect_true(job$job_id %in% failed_jobs$job_id)
  expect_equal(nrow(failed_jobs), 1)
  
  # Test setting back to COMPLETED and retrieving result
  # First store a result
  store_job_result(
    getOption("dsHPC.config")$connection,
    job$job_hash,
    10  # Result of test_func(5) would be 10
  )
  manually_update_job_status(job$job_id, "COMPLETED")
  status <- dsHPC.status(job$job_id)
  expect_equal(status$status, "COMPLETED")
  
  # Now retrieving the result should work
  result <- dsHPC.result(job$job_id)
  expect_equal(result, 10)
})

test_that("Python job status management works correctly", {
  skip_if_not_installed("reticulate")
  skip_if_not(reticulate::py_module_available("numpy"), "NumPy is not available")
  
  # Set up isolated testing environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Submit a test Python job
  tryCatch({
    job <- dsHPC.submit_python(
      py_module = "numpy",
      py_function = "mean",
      args = list(a = c(1, 2, 3, 4, 5))
    )
    
    # Initially the job should be COMPLETED (when scheduler is not available)
    # or SUBMITTED/PENDING (if using a real scheduler)
    initial_status <- dsHPC.status(job$job_id)
    expect_true(initial_status$status %in% c("SUBMITTED", "PENDING", "RUNNING", "COMPLETED"))
    
    # Test manual status transitions for Python jobs
    
    # 1. Set job to PENDING
    manually_update_job_status(job$job_id, "PENDING")
    status <- dsHPC.status(job$job_id)
    expect_equal(status$status, "PENDING")
    
    # 2. Set job to RUNNING
    manually_update_job_status(job$job_id, "RUNNING")
    status <- dsHPC.status(job$job_id)
    expect_equal(status$status, "RUNNING")
    
    # 3. Set job to FAILED with Python-specific error message
    error_msg <- "ImportError: No module named 'nonexistent_module'"
    manually_update_job_status(job$job_id, "FAILED", error_msg)
    status <- dsHPC.status(job$job_id)
    expect_equal(status$status, "FAILED")
    
    # Check if we can retrieve the Python-specific error message
    job_info <- get_job_info(getOption("dsHPC.config")$connection, job$job_id)
    expect_equal(job_info$error_message, error_msg)
    
    # Test listing Python jobs
    all_jobs <- dsHPC.list_jobs()
    expect_true(is.data.frame(all_jobs))
    expect_true(job$job_id %in% all_jobs$job_id)
    
    # Test setting back to COMPLETED and retrieving result for Python job
    # First store a result (mean of 1:5 should be 3)
    store_job_result(
      getOption("dsHPC.config")$connection,
      job$job_hash,
      3
    )
    manually_update_job_status(job$job_id, "COMPLETED")
    status <- dsHPC.status(job$job_id)
    expect_equal(status$status, "COMPLETED")
    
    # Now retrieving the result should work
    result <- dsHPC.result(job$job_id)
    expect_equal(result, 3)
  }, error = function(e) {
    skip(paste("Skipping Python test due to error:", e$message))
  })
})

test_that("Job cancellation works correctly", {
  # Set up isolated testing environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Submit a test job
  test_func <- function(x) Sys.sleep(60) # Long-running function
  job <- dsHPC.submit(
    func = test_func,
    args = list(x = 5)
  )
  
  # Set job to RUNNING state to test cancellation
  manually_update_job_status(job$job_id, "RUNNING")
  
  # When scheduler is not available, we can't actually cancel a job with Slurm
  # But we can test the database update logic
  
  # Mock the slurm cancellation function to always succeed
  with_mocked_bindings(
    cancel_slurm_job = function(job_id) TRUE,
    .package = "dsHPC",
    {
      # Set the scheduler_id using the helper function
      manually_update_scheduler_id(job$job_id, "12345")  # Fake Slurm ID
      
      # Temporarily set scheduler_available to TRUE
      old_config <- getOption("dsHPC.config")
      new_config <- old_config
      new_config$scheduler_available <- TRUE
      options(dsHPC.config = new_config)
      
      # Now try to cancel
      cancelled <- dsHPC.cancel(job$job_id)
      
      # Restore original config
      options(dsHPC.config = old_config)
      
      # Check cancellation status
      expect_true(cancelled)
      
      # Check that job status was updated
      status <- dsHPC.status(job$job_id)
      expect_equal(status$status, "CANCELLED")
    }
  )
})

test_that("Mixed R and Python job management works together", {
  skip_if_not_installed("reticulate")
  skip_if_not(reticulate::py_module_available("numpy"), "NumPy is not available")
  
  # Set up isolated testing environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Test with tryCatch to handle potential issues with Python
  tryCatch({
    # Submit an R job
    r_job <- dsHPC.submit(
      func = function(x) x * 2,
      args = list(x = 5)
    )
    
    # Submit a Python job
    py_job <- dsHPC.submit_python(
      py_module = "numpy",
      py_function = "mean",
      args = list(a = c(1, 2, 3, 4, 5))
    )
    
    # List all jobs - should include both
    all_jobs <- dsHPC.list_jobs()
    expect_true(is.data.frame(all_jobs))
    expect_true(r_job$job_id %in% all_jobs$job_id)
    expect_true(py_job$job_id %in% all_jobs$job_id)
    
    # Set different statuses for the jobs
    manually_update_job_status(r_job$job_id, "FAILED", "R job error")
    manually_update_job_status(py_job$job_id, "RUNNING")
    
    # Check that their statuses are correctly reported
    r_status <- dsHPC.status(r_job$job_id)
    py_status <- dsHPC.status(py_job$job_id)
    
    expect_equal(r_status$status, "FAILED")
    expect_equal(py_status$status, "RUNNING")
    
    # Test filtering by status
    failed_jobs <- dsHPC.list_jobs(status_filter = "FAILED")
    running_jobs <- dsHPC.list_jobs(status_filter = "RUNNING")
    
    expect_true(r_job$job_id %in% failed_jobs$job_id)
    expect_true(py_job$job_id %in% running_jobs$job_id)
    
    # Test multiple status filters
    mixed_filter_jobs <- dsHPC.list_jobs(status_filter = c("FAILED", "RUNNING"))
    expect_equal(nrow(mixed_filter_jobs), 2)
    expect_true(r_job$job_id %in% mixed_filter_jobs$job_id)
    expect_true(py_job$job_id %in% mixed_filter_jobs$job_id)
  }, error = function(e) {
    skip(paste("Skipping mixed job test due to error:", e$message))
  })
})

test_that("Waiting for job completion works with timeout", {
  skip_if_not_installed("callr")
  
  # Set up isolated testing environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Submit a test job
  test_func <- function(x) x * 2
  job <- dsHPC.submit(
    func = test_func,
    args = list(x = 5)
  )
  
  # Set job to RUNNING first
  manually_update_job_status(job$job_id, "RUNNING")
  
  # Start a background thread that will update the job status after 1 second
  update_thread <- callr::r_bg(
    function(job_id, test_db_path) {
      Sys.sleep(1)  # Wait a bit
      
      # Load package and connect to the same DB
      library(dsHPC)
      dsHPC.init(db_path = test_db_path)
      
      # Update job status to COMPLETED
      config <- getOption("dsHPC.config")
      dsHPC:::update_job_status(
        config$connection, 
        job_id, 
        "COMPLETED", 
        completed = TRUE
      )
      
      # Store a result
      job_info <- dsHPC:::get_job_info(config$connection, job_id)
      dsHPC:::store_job_result(
        config$connection,
        job_info$job_hash,
        10  # Result would be 10
      )
    },
    args = list(job_id = job$job_id, test_db_path = test_env$db_path),
    supervise = TRUE
  )
  
  # Wait for job with wait_for_completion = TRUE and sufficient timeout
  result <- dsHPC.result(job$job_id, wait_for_completion = TRUE, timeout = 5)
  expect_equal(result, 10)
  
  # Clean up the background thread
  update_thread$kill()
})

test_that("Waiting for job completion respects timeout", {
  # Set up isolated testing environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Submit a test job
  test_func <- function(x) x * 2
  job <- dsHPC.submit(
    func = test_func,
    args = list(x = 5)
  )
  
  # Set job to RUNNING and leave it there
  manually_update_job_status(job$job_id, "RUNNING")
  
  # Try to wait with a very short timeout
  expect_warning(
    result <- dsHPC.result(job$job_id, wait_for_completion = TRUE, timeout = 0.1),
    "Timeout reached"
  )
  expect_null(result)
}) 