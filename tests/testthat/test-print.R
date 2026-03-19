test_that("dsjobs_result creation and printing", {
  result <- dsJobsClient:::dsjobs_result(
    per_site = list(
      node1 = list(state = "RUNNING", step_index = 2L, total_steps = 3L),
      node2 = list(state = "FINISHED", step_index = 3L, total_steps = 3L)
    )
  )
  expect_s3_class(result, "dsjobs_result")
  expect_output(print(result), "node1: RUNNING")
  expect_output(print(result), "node2: FINISHED")
})

test_that("dsjobs_result $ accessor works", {
  result <- dsJobsClient:::dsjobs_result(
    per_site = list(node1 = list(value = 42))
  )
  expect_equal(result$node1$value, 42)
  expect_true(is.list(result$per_site))
})

test_that("dsjobs_result as.data.frame returns empty for non-df", {
  result <- dsJobsClient:::dsjobs_result(
    per_site = list(node1 = list(state = "DONE"))
  )
  df <- as.data.frame(result)
  expect_true(is.data.frame(df))
})
