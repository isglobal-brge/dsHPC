test_that("hpcSubmitDS requires a domain label", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  expect_error(
    hpcSubmitDS(list(
      .owner = "alice",
      job_id = "job_unlabelled",
      visibility = "private",
      steps = list(list(type = "emit", plane = "session",
        output_name = "out", value = 1))
    )),
    "submission requires a domain label"
  )
})

test_that("hpcLoadOutputDS always requires a domain label", {
  expect_error(
    hpcLoadOutputDS("job_missing", "output"),
    "load operation requires a domain label"
  )
  expect_error(
    hpcLoadOutputDS("job_missing", "output", required_label = ""),
    "load operation requires a domain label"
  )
})

test_that("get_job_output_ref always requires a domain label", {
  expect_error(
    get_job_output_ref("job_missing", "output"),
    "load operation requires a domain label"
  )
})
