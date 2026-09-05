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
  expect_true(all(c("tracking_roots", "tracking_jobs", "tracking_outputs") %in%
    tables))
  expect_true(all(c("execution_mode", "finish_requested",
    "finalizing_job_id") %in%
    DBI::dbListFields(db, "tracking_roots")))

  # Jobs table has expected columns
  cols <- DBI::dbListFields(db, "jobs")
  expect_true("job_id" %in% cols)
  expect_true("owner_id" %in% cols)
  expect_true("state" %in% cols)
  expect_true("worker_pid" %in% cols)
  expect_true("access_token_hash" %in% cols)
  expect_true("spec_json" %in% cols)

  # Steps table has expected columns
  cols <- DBI::dbListFields(db, "steps")
  expect_true("output_ref" %in% cols)
  expect_true("plane" %in% cols)
  expect_true("step_hash" %in% cols)
  expect_true("cache_hit" %in% cols)
  expect_true("cache_source_job_id" %in% cols)
  expect_true("cache_source_step_index" %in% cols)

  step_indexes <- DBI::dbGetQuery(db, "PRAGMA index_list(steps)")
  expect_true("idx_steps_step_hash" %in% step_indexes$name)

  output_cols <- DBI::dbListFields(db, "outputs")
  expect_true("reuse_class" %in% output_cols)
  tracking_output_indexes <- DBI::dbGetQuery(db,
    "PRAGMA index_list(tracking_outputs)")
  expect_true("idx_tracking_outputs_source" %in%
    tracking_output_indexes$name)
  expect_identical(as.integer(DBI::dbGetQuery(
    db, "PRAGMA user_version")[[1L]]), 2L)
})

test_that("a newer database schema fails closed", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  future <- DBI::dbConnect(RSQLite::SQLite(), file.path(home, "dshpc.sqlite"))
  DBI::dbExecute(future, "PRAGMA user_version = 3")
  DBI::dbDisconnect(future)

  expect_error(dsHPC:::.db_connect(), "schema is newer", fixed = TRUE)
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

test_that("tracking migration does not promote legacy outputs", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  legacy <- DBI::dbConnect(RSQLite::SQLite(), file.path(home, "dshpc.sqlite"))
  DBI::dbExecute(legacy, "
    CREATE TABLE outputs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id TEXT NOT NULL, step_index INTEGER, name TEXT NOT NULL,
      kind TEXT NOT NULL, path_or_ref TEXT, size_bytes INTEGER,
      safe_for_client INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL)")
  DBI::dbExecute(legacy,
    "INSERT INTO outputs
       (job_id, name, kind, safe_for_client, created_at)
     VALUES ('job_legacy', 'old_summary', 'summary', 1, '2025-01-01')")
  DBI::dbDisconnect(legacy)

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  row <- DBI::dbGetQuery(db,
    "SELECT safe_for_client, reuse_class FROM outputs WHERE name = 'old_summary'")
  expect_true(as.logical(row$safe_for_client))
  expect_identical(row$reuse_class, "internal_only")
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM tracking_roots")$n, 0)
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
