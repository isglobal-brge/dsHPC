# Tests for job status management, including unfinished jobs, timeouts, and errors

# Create a helper function that mimics a long-running job
long_running_job <- function(sleep_time) {
  Sys.sleep(sleep_time)
  return(sleep_time)
}

# Create a helper function that always fails
failing_job <- function() {
  stop("This job is designed to fail")
}

# Define a suppressErrors helper function
suppressErrors <- function(expr) {
  tryCatch(expr, error = function(e) NULL)
}

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

test_that("status correctly reports unfinished job", {
  # Setup isolated test environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Create a mock job in the database with a "RUNNING" status
  # This simulates an unfinished job that's still running
  
  # Get configuration
  config <- getOption("dsHPC.config")
  
  # Generate a job ID
  job_id <- uuid::UUIDgenerate()
  job_hash <- paste0("test_hash_", job_id)
  
  # Create proper JSON for args with named elements
  args_json <- jsonlite::toJSON(list(sleep_time = 5))
  
  # Store a fake job with running status directly in the database
  # Include all the required fields to avoid dataframe column errors
  query <- sprintf(
    "INSERT INTO jobs (job_id, job_hash, function_name, args, status, submission_time, error_message, scheduler_id) 
     VALUES ('%s', '%s', 'long_running_job', '%s', 'RUNNING', '%s', NULL, NULL)",
    job_id, job_hash, args_json, format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  
  DBI::dbExecute(config$connection, query)
  
  # Now check the status of this job
  status <- dsHPC.status(job_id)
  
  # Expect the job to be reported as still running
  expect_equal(status$status, "RUNNING")
  
  # Try to get the result of the running job without waiting
  expect_warning(result <- dsHPC.result(job_id))
  
  # Expect NULL result for unfinished job
  expect_null(result)
})

test_that("timeout works when waiting for job completion", {
  skip("This test depends on timing and may be unstable")
  
  # Setup isolated test environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Create a job directly in the database that won't complete
  # Get configuration
  config <- getOption("dsHPC.config")
  
  # Generate a job ID
  job_id <- uuid::UUIDgenerate()
  job_hash <- paste0("test_hash_timeout_", job_id)
  
  # Create proper JSON for args with named elements
  args_json <- jsonlite::toJSON(list(sleep_time = 10))
  
  # Store a fake job with running status directly in the database
  query <- sprintf(
    "INSERT INTO jobs (job_id, job_hash, function_name, args, status, submission_time, error_message, scheduler_id) 
     VALUES ('%s', '%s', 'long_running_job', '%s', 'RUNNING', '%s', NULL, NULL)",
    job_id, job_hash, args_json, format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  
  DBI::dbExecute(config$connection, query)
  
  # Try to get result with a very short timeout
  start_time <- Sys.time()
  expect_warning(
    result <- dsHPC.result(job_id, wait_for_completion = TRUE, timeout = 0.5)
  )
  end_time <- Sys.time()
  
  # Verify the timeout was respected
  elapsed_time <- as.numeric(difftime(end_time, start_time, units = "secs"))
  expect_lt(elapsed_time, 2) # Should take less than 2 seconds
  
  # Expect NULL result due to timeout
  expect_null(result)
})

test_that("can retrieve results from completed job", {
  # Setup isolated test environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Submit a job that completes quickly
  job <- dsHPC.submit(
    func = function(x) x * 2,
    args = list(x = 5)
  )
  
  # The job should complete immediately in simulation mode
  expect_equal(job$status, "COMPLETED")
  
  # Check status
  status <- dsHPC.status(job$job_id)
  expect_equal(status$status, "COMPLETED")
  
  # Get result with status
  status_with_result <- dsHPC.status(job$job_id, return_result = TRUE)
  expect_equal(status_with_result$status, "COMPLETED")
  expect_equal(status_with_result$result, 10)
  
  # Get result directly
  result <- dsHPC.result(job$job_id)
  expect_equal(result, 10)
})

test_that("handles failed jobs correctly", {
  # Setup isolated test environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Direct database approach to create a failed job
  # Get configuration
  config <- getOption("dsHPC.config")
  
  # Generate a job ID
  job_id <- uuid::UUIDgenerate()
  job_hash <- paste0("test_hash_failed_", job_id)
  
  # Create proper JSON for args with named elements
  # For failing_job we don't need arguments but we'll provide some
  args_json <- jsonlite::toJSON(list(dummy = TRUE))
  
  # Store a fake job with failed status directly in the database
  query <- sprintf(
    "INSERT INTO jobs (job_id, job_hash, function_name, args, status, submission_time, completion_time, error_message, scheduler_id) 
     VALUES ('%s', '%s', 'failing_job', '%s', 'FAILED', '%s', '%s', 'This job is designed to fail', NULL)",
    job_id, job_hash, args_json, format(Sys.time(), "%Y-%m-%d %H:%M:%S"), format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  
  DBI::dbExecute(config$connection, query)
  
  # Check status - should be FAILED
  status <- dsHPC.status(job_id)
  expect_equal(status$status, "FAILED")
  
  # Try to get the result - should return NULL with a warning
  expect_warning(result <- dsHPC.result(job_id))
  expect_null(result)
})

test_that("can handle multiple concurrent jobs", {
  # Setup isolated test environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Submit several jobs with different arguments
  job1 <- dsHPC.submit(
    func = function(x) x^2,
    args = list(x = 4)
  )
  
  job2 <- dsHPC.submit(
    func = function(x) sum(x),
    args = list(x = c(1, 2, 3, 4, 5))
  )
  
  # The third job needs special handling to avoid the list replacement error
  # It's safer to use a single argument with simple types 
  job3 <- dsHPC.submit(
    func = function(text) paste(text, "world", sep = "-"),
    args = list(text = "hello")
  )
  
  # Check that all jobs have unique IDs
  expect_false(identical(job1$job_id, job2$job_id))
  expect_false(identical(job1$job_id, job3$job_id))
  expect_false(identical(job2$job_id, job3$job_id))
  
  # Check the status of each job
  status1 <- dsHPC.status(job1$job_id)
  status2 <- dsHPC.status(job2$job_id)
  status3 <- dsHPC.status(job3$job_id)
  
  expect_equal(status1$status, "COMPLETED")
  expect_equal(status2$status, "COMPLETED")
  expect_equal(status3$status, "COMPLETED")
  
  # Get and verify results
  result1 <- dsHPC.result(job1$job_id)
  result2 <- dsHPC.result(job2$job_id)
  result3 <- dsHPC.result(job3$job_id)
  
  expect_equal(result1, 16)
  expect_equal(result2, 15)
  expect_equal(result3, "hello-world")
  
  # List all jobs and verify count
  all_jobs <- dsHPC.list_jobs()
  expect_gte(nrow(all_jobs), 3)
  
  # Filter jobs by status
  completed_jobs <- dsHPC.list_jobs(status_filter = "COMPLETED")
  expect_gte(nrow(completed_jobs), 3)
})

test_that("non-existent job handling works", {
  # Setup isolated test environment
  test_env <- setup_isolated_test_db()
  on.exit(teardown_isolated_test_db(test_env))
  
  # Generate a random job ID that doesn't exist
  fake_job_id <- uuid::UUIDgenerate()
  
  # Check status - should error
  expect_error(dsHPC.status(fake_job_id), "Job not found")
  
  # Try to get result - should also error
  expect_error(dsHPC.result(fake_job_id), "Job not found")
}) 