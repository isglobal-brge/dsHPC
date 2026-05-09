test_that("cancel_jobs_by_tag requires admin key and cancels matching active jobs", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.admin_key = "secret"))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  spec_a <- c(make_test_spec(), list(tags = c("per_image", "gen_a")))
  spec_b <- c(make_test_spec(), list(tags = c("per_image", "gen_b")))
  spec_done <- c(make_test_spec(), list(tags = c("per_image", "gen_a")))
  dsHPC:::.store_create_job(db, "job_gen_a", "user_a", spec_a, 1L)
  dsHPC:::.store_create_job(db, "job_gen_b", "user_a", spec_b, 1L)
  dsHPC:::.store_create_job(db, "job_gen_a_done", "user_a", spec_done, 1L)
  dsHPC:::.store_update_job(db, "job_gen_a", state = "RUNNING")
  dsHPC:::.store_update_job(db, "job_gen_a_done", state = "FINISHED")

  expect_error(
    cancel_jobs_by_tag("%gen_a%", admin_key = list(.admin_key = "wrong")),
    "invalid admin_key"
  )

  cancelled <- cancel_jobs_by_tag("%gen_a%",
    admin_key = list(.admin_key = "secret"),
    reason = "superseded")

  expect_equal(cancelled$job_id, "job_gen_a")
  expect_equal(cancelled$previous_state, "RUNNING")
  expect_equal(cancelled$state, "CANCELLED")
  expect_equal(dsHPC:::.store_get_job(db, "job_gen_a")$state, "CANCELLED")
  expect_equal(dsHPC:::.store_get_job(db, "job_gen_a")$error_message,
    "superseded")
  expect_equal(dsHPC:::.store_get_job(db, "job_gen_b")$state, "PENDING")
  expect_equal(dsHPC:::.store_get_job(db, "job_gen_a_done")$state,
    "FINISHED")
})

test_that("admin key can be supplied by environment", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.admin_key = NULL,
    default.dshpc.admin_key = NULL))
  withr::local_envvar(c(DSHPC_ADMIN_KEY = "env-secret"))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  spec <- c(make_test_spec(), list(tags = c("per_image", "gen_env")))
  dsHPC:::.store_create_job(db, "job_gen_env", "user_a", spec, 1L)
  dsHPC:::.store_update_job(db, "job_gen_env", state = "RUNNING")

  expect_true(dsHPC:::.admin_is_configured())
  expect_error(
    cancel_jobs_by_tag("%gen_env%", admin_key = list(.admin_key = "wrong")),
    "invalid admin_key"
  )

  cancelled <- cancel_jobs_by_tag("%gen_env%",
    admin_key = list(.admin_key = "env-secret"))

  expect_equal(cancelled$job_id, "job_gen_env")
  expect_equal(dsHPC:::.store_get_job(db, "job_gen_env")$state, "CANCELLED")
})
