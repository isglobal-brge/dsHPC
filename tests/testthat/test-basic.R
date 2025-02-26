test_that("core utilities work", {
  # Test hash creation
  hash1 <- create_job_hash("test_func", list(a = 1, b = 2))
  hash2 <- create_job_hash("test_func", list(a = 1, b = 2))
  hash3 <- create_job_hash("test_func", list(a = 1, b = 3))
  
  expect_equal(hash1, hash2)
  expect_false(identical(hash1, hash3))
  
  # Test job ID generation
  job_id1 <- generate_job_id()
  job_id2 <- generate_job_id()
  
  expect_match(job_id1, "^dshpc_\\d+_[a-f0-9]+$")
  expect_false(identical(job_id1, job_id2))
  
  # Test function call formatting
  call1 <- format_function_call("mean", list(x = c(1, 2, 3)))
  expect_match(call1, "^mean\\(x = c\\(1, 2, 3\\)\\)$")
  
  call2 <- format_function_call("kmeans", list(x = "data", centers = 3), package = "stats")
  expect_match(call2, "^stats::kmeans\\(x = \"data\", centers = 3\\)$")
})

test_that("initialization works without errors in simulation mode", {
  # This test should run even without Slurm
  config <- dsHPC.init()
  
  expect_true(is.list(config))
  expect_true("connection" %in% names(config))
  expect_true("scheduler" %in% names(config))
})

test_that("basic job submission works in simulation mode", {
  # Skip if already initialized
  if(is.null(getOption("dsHPC.config"))) {
    dsHPC.init()
  }
  
  # Submit a simple job that should run locally if Slurm is not available
  job <- dsHPC.submit(
    func = mean,
    args = list(x = c(1, 2, 3, 4, 5))
  )
  
  expect_true(is.list(job))
  expect_true("job_id" %in% names(job))
  expect_true("status" %in% names(job))
  
  if(job$status == "COMPLETED") {
    expect_equal(job$result, 3)
  }
}) 