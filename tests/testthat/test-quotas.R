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

test_that("concurrent admissions cannot exceed the quota", {
  skip_if(.Platform$OS.type != "unix")
  home <- setup_test_home()
  withr::local_options(list(
    dshpc.home = home,
    dshpc.max_jobs_per_user = 1L,
    dshpc.max_queued_jobs_global = 1L
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  dsHPC:::.db_close(db)
  go <- file.path(home, "admit-go")
  ready <- file.path(home, paste0("ready-", 1:2))
  spec <- make_test_spec()

  admit <- function(index) {
    child_db <- dsHPC:::.db_connect()
    on.exit(dsHPC:::.db_close(child_db), add = TRUE)
    writeLines("ready", ready[index])
    deadline <- Sys.time() + 5
    while (!file.exists(go) && Sys.time() < deadline) Sys.sleep(0.01)
    if (!file.exists(go)) return("barrier timeout")
    tryCatch({
      dsHPC:::.store_create_job(child_db, paste0("job_concurrent_", index),
        "same_owner", spec, 1L, enforce_quotas = TRUE)
      TRUE
    }, error = function(e) conditionMessage(e))
  }

  children <- lapply(1:2, function(i) parallel::mcparallel(admit(i)))
  deadline <- Sys.time() + 5
  while (!all(file.exists(ready)) && Sys.time() < deadline) Sys.sleep(0.01)
  expect_true(all(file.exists(ready)))
  file.create(go)
  results <- parallel::mccollect(children)

  check_db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(check_db), add = TRUE)
  admitted <- DBI::dbGetQuery(check_db,
    "SELECT COUNT(*) AS n FROM jobs WHERE state = 'PENDING'")$n
  expect_equal(admitted, 1L)
  expect_equal(sum(vapply(results, isTRUE, logical(1))), 1L)
  expect_true(any(grepl("quota exceeded", unlist(results), fixed = TRUE)))
})
