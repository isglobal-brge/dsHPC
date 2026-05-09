test_that("SQLite database is created with correct schema", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  # Tables exist
  tables <- DBI::dbListTables(db)
  expect_true("jobs" %in% tables)
  expect_true("steps" %in% tables)
  expect_true("events" %in% tables)

  # Jobs table has expected columns
  cols <- DBI::dbListFields(db, "jobs")
  expect_true("job_id" %in% cols)
  expect_true("owner_id" %in% cols)
  expect_true("state" %in% cols)
  expect_true("worker_pid" %in% cols)
  expect_true("spec_json" %in% cols)

  # Steps table has expected columns
  cols <- DBI::dbListFields(db, "steps")
  expect_true("output_ref" %in% cols)
  expect_true("plane" %in% cols)
})

test_that("WAL mode is enabled", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  mode <- DBI::dbGetQuery(db, "PRAGMA journal_mode")
  expect_equal(tolower(mode[[1]]), "wal")
})

test_that("event logging works", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  # Need a job first for FK
  spec <- make_test_spec()
  dsHPC:::.store_create_job(db, "job_test_001", "testuser", spec, 1L)

  dsHPC:::.db_log_event(db, "job_test_001", "test_event",
                          list(detail = "hello"))

  events <- DBI::dbGetQuery(db, "SELECT * FROM events WHERE job_id = 'job_test_001'")
  expect_equal(nrow(events), 2L)  # created + test_event
  expect_true(any(events$event == "test_event"))
})
