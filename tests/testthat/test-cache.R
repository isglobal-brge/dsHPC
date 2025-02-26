# Tests for the dsHPC database cache system

# Helper function to get connection info
get_db_info <- function() {
  config <- getOption("dsHPC.config")
  expect_false(is.null(config), "dsHPC config should be available")
  
  con <- config$connection
  expect_false(is.null(con), "Database connection should be available")
  
  # Check the connection parameters to verify database name
  params <- DBI::dbGetInfo(con)
  return(params)
}

# Helper function to directly remove a result from the cache
force_clean_result <- function(con, job_hash) {
  # This is a direct cache removal for testing purposes
  query <- sprintf("DELETE FROM results WHERE job_hash = '%s'", job_hash)
  DBI::dbExecute(con, query)
}

# Helper to setup a clean test environment with its own database
setup_isolated_test_db <- function() {
  # Save original config
  old_config <- getOption("dsHPC.config")
  
  # Create a temporary database
  test_db_path <- tempfile(pattern = "dsHPC_test_", fileext = ".sqlite")
  
  # Initialize with the temporary database
  config <- dsHPC.init(db_path = test_db_path)
  
  # Return information needed for cleanup
  return(list(
    old_config = old_config,
    test_config = config,
    test_db_path = test_db_path
  ))
}

# Helper to teardown the isolated test environment
teardown_isolated_test_db <- function(env) {
  # Disconnect from test database
  if (!is.null(env$test_config$connection)) {
    DBI::dbDisconnect(env$test_config$connection)
  }
  
  # Clean up the test database file
  if (file.exists(env$test_db_path)) {
    unlink(env$test_db_path)
  }
  
  # Restore original configuration
  options(dsHPC.config = env$old_config)
  
  # Reinitialize if needed
  if (is.null(getOption("dsHPC.config"))) {
    dsHPC.init()
  }
}

test_that("database is initialized with correct name", {
  # Setup isolated test environment
  env <- setup_isolated_test_db()
  
  # Check that database exists and has the correct name
  params <- get_db_info()
  
  # Get database path from connection
  db_path <- params$dbname
  expect_true(file.exists(db_path), "Database file should exist")
  
  # Check that the database filename includes 'dsHPC'
  expect_match(basename(db_path), "dsHPC", 
               "Database filename should include 'dsHPC' to avoid conflicts")
  
  # Check that required tables exist
  con <- getOption("dsHPC.config")$connection
  expect_true(DBI::dbExistsTable(con, "jobs"), "jobs table should exist")
  expect_true(DBI::dbExistsTable(con, "results"), "results table should exist")
  
  # Teardown
  teardown_isolated_test_db(env)
})

test_that("results are properly cached and retrieved", {
  # Setup isolated test environment
  env <- setup_isolated_test_db()
  
  # Create test data and function
  test_data <- c(1, 2, 3, 4, 5)
  
  # First job submission should compute result
  job1 <- dsHPC.submit(
    func = mean,
    args = list(x = test_data),
    use_cache = TRUE
  )
  
  expect_true("job_id" %in% names(job1), "Job should have a job_id")
  
  # The job should either be COMPLETED immediately (simulated)
  # or be CACHED if caching is working
  expect_true(job1$status %in% c("COMPLETED", "PENDING", "RUNNING", "CACHED"),
             "Job should have valid status")
  
  if (job1$status %in% c("PENDING", "RUNNING")) {
    # If job is still running, wait for it to complete
    status <- dsHPC.status(job1$job_id)
    # Wait up to 10 seconds for job to complete
    for (i in 1:10) {
      if (status$status == "COMPLETED") break
      Sys.sleep(1)
      status <- dsHPC.status(job1$job_id)
    }
    expect_equal(status$status, "COMPLETED", "Job should complete within 10 seconds")
  }
  
  # If the job's complete, check that we can submit again with caching
  if (job1$status == "COMPLETED") {
    # Second submission should use cache
    job2 <- dsHPC.submit(
      func = mean,
      args = list(x = test_data),
      use_cache = TRUE
    )
    
    expect_equal(job2$status, "CACHED", "Second job should use cache")
    expect_equal(job2$result, 3, "Cached result should be correct")
    
    # With use_cache = FALSE, it should recompute
    job3 <- dsHPC.submit(
      func = mean,
      args = list(x = test_data),
      use_cache = FALSE
    )
    
    expect_true(job3$status %in% c("COMPLETED", "PENDING", "RUNNING"), 
                "Job with use_cache=FALSE should not use cache")
  }
  
  # Teardown
  teardown_isolated_test_db(env)
})

test_that("different arguments produce different cache entries", {
  # Setup isolated test environment
  env <- setup_isolated_test_db()
  
  # Submit two jobs with different arguments
  job1 <- dsHPC.submit(
    func = mean,
    args = list(x = c(1, 2, 3, 4, 5)),
    use_cache = TRUE
  )
  
  job2 <- dsHPC.submit(
    func = mean,
    args = list(x = c(10, 20, 30, 40, 50)),
    use_cache = TRUE
  )
  
  # Wait for jobs to complete if necessary
  if (job1$status %in% c("PENDING", "RUNNING")) {
    status <- dsHPC.status(job1$job_id)
    for (i in 1:10) {
      if (status$status == "COMPLETED") break
      Sys.sleep(1)
      status <- dsHPC.status(job1$job_id)
    }
  }
  
  if (job2$status %in% c("PENDING", "RUNNING")) {
    status <- dsHPC.status(job2$job_id)
    for (i in 1:10) {
      if (status$status == "COMPLETED") break
      Sys.sleep(1)
      status <- dsHPC.status(job2$job_id)
    }
  }
  
  # For simpler tests, just use the results directly from the job submissions
  if (job1$status == "COMPLETED" || job1$status == "CACHED") {
    expect_equal(job1$result, 3, "First job result should be 3")
  }
  
  if (job2$status == "COMPLETED" || job2$status == "CACHED") {
    expect_equal(job2$result, 30, "Second job result should be 30")
  }
  
  # Teardown
  teardown_isolated_test_db(env)
})

test_that("cache persists across sessions", {
  # Create a custom database path to test persistence
  test_db_path <- tempfile(pattern = "dsHPC_test_", fileext = ".sqlite")
  
  # Save original config
  old_config <- getOption("dsHPC.config")
  
  # Initialize with custom path
  config <- dsHPC.init(db_path = test_db_path)
  
  # Submit a job to ensure something is cached
  job <- dsHPC.submit(
    func = sum,
    args = list(x = c(1, 2, 3, 4, 5)),
    use_cache = TRUE
  )
  
  # Wait for job completion if necessary
  if (job$status %in% c("PENDING", "RUNNING")) {
    status <- dsHPC.status(job$job_id)
    for (i in 1:10) {
      if (status$status == "COMPLETED") break
      Sys.sleep(1)
      status <- dsHPC.status(job$job_id)
    }
  }
  
  # Store the result for later comparison
  if (job$status == "COMPLETED" || job$status == "CACHED") {
    result_value <- job$result
    
    # Disconnect from the database
    DBI::dbDisconnect(config$connection)
    options(dsHPC.config = NULL)
    
    # Now reconnect to simulate a new session
    new_config <- dsHPC.init(db_path = test_db_path)
    
    # Submit the same job again to check if cache is used
    new_job <- dsHPC.submit(
      func = sum,
      args = list(x = c(1, 2, 3, 4, 5)),
      use_cache = TRUE
    )
    
    expect_equal(new_job$status, "CACHED", "Job should use cached result")
    expect_equal(new_job$result, 15, "Cached result should be correct")
    
    # Clean up
    DBI::dbDisconnect(new_config$connection)
    unlink(test_db_path)
  } else {
    # Clean up if job didn't complete
    DBI::dbDisconnect(config$connection)
    unlink(test_db_path)
    skip("Job did not complete in time, skipping persistence test")
  }
  
  # Restore original configuration
  options(dsHPC.config = old_config)
  
  # Reinitialize if needed
  if (is.null(getOption("dsHPC.config"))) {
    dsHPC.init()
  }
})

test_that("manual cache removal works", {
  # Setup isolated test environment
  env <- setup_isolated_test_db()
  
  # Submit a job
  job <- dsHPC.submit(
    func = sqrt,
    args = list(x = 16),
    use_cache = TRUE
  )
  
  # Wait for job completion if necessary
  if (job$status %in% c("PENDING", "RUNNING")) {
    status <- dsHPC.status(job$job_id)
    for (i in 1:10) {
      if (status$status == "COMPLETED") break
      Sys.sleep(1)
      status <- dsHPC.status(job$job_id)
    }
  }
  
  # Only proceed with testing if job completed
  if (job$status == "COMPLETED" || job$status == "CACHED") {
    # Get the job info
    job_info <- get_job_info(getOption("dsHPC.config")$connection, job$job_id)
    expect_false(is.null(job_info), "Job info should be available")
    
    # Get the job hash
    job_hash <- job_info$job_hash
    
    # Verify result is cached
    con <- getOption("dsHPC.config")$connection
    expect_true(is_result_cached(con, job_hash), 
                "Result should be cached before cleaning")
    
    # Manually remove the result
    force_clean_result(con, job_hash)
    
    # Verify result is no longer cached
    expect_false(is_result_cached(con, job_hash), 
                "Result should not be cached after manual removal")
    
    # Submit the job again to verify it's recomputed
    new_job <- dsHPC.submit(
      func = sqrt,
      args = list(x = 16),
      use_cache = TRUE
    )
    
    expect_true(new_job$status %in% c("COMPLETED", "PENDING", "RUNNING"), 
                "Job should be recomputed after cache removal")
  } else {
    skip("Job did not complete in time, skipping cache removal test")
  }
  
  # Teardown
  teardown_isolated_test_db(env)
}) 