test_that("hpcLoadOutputDS requires domain label by default", {
  withr::local_options(list(dshpc.require_domain_label = TRUE))
  expect_error(
    hpcLoadOutputDS("job_missing", "output"),
    "requires a domain label by default"
  )
})

test_that("domain label policy can be disabled for development", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.require_domain_label = FALSE,
    dshpc.home = home))
  on.exit(cleanup_test_home(home))
  expect_error(
    hpcLoadOutputDS("job_missing", "output"),
    "Job not found"
  )
})
