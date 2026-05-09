test_that("per-user quota blocks excess submissions", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.max_jobs_per_user = 2L))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  spec <- make_test_spec()
  dsHPC:::.store_create_job(db, "job_q1", "user_a", spec, 1L)
  dsHPC:::.store_create_job(db, "job_q2", "user_a", spec, 1L)

  expect_error(dsHPC:::.check_quotas(db, "user_a"), "Per-user quota")
})

test_that("global quota blocks excess submissions", {
  home <- setup_test_home()
  withr::local_options(list(
    dshpc.home = home,
    dshpc.max_jobs_per_user = 10L,
    dshpc.max_queued_jobs_global = 2L
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  spec <- make_test_spec()
  dsHPC:::.store_create_job(db, "job_g1", "user_a", spec, 1L)
  dsHPC:::.store_create_job(db, "job_g2", "user_b", spec, 1L)

  expect_error(dsHPC:::.check_quotas(db, "user_c"), "Global job quota")
})

test_that("completed jobs don't count against quota", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.max_jobs_per_user = 2L))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  spec <- make_test_spec()
  dsHPC:::.store_create_job(db, "job_done1", "user_a", spec, 1L)
  dsHPC:::.store_update_job(db, "job_done1", state = "FINISHED")
  dsHPC:::.store_create_job(db, "job_done2", "user_a", spec, 1L)
  dsHPC:::.store_update_job(db, "job_done2", state = "FAILED")

  # Should be fine -- both are terminal
  expect_silent(dsHPC:::.check_quotas(db, "user_a"))
})
