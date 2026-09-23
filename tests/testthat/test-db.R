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
  expect_true(all(c("worker_pid", "retry_count", "attempt_id",
    "launch_owner_pid", "resource_json") %in% cols))

  step_indexes <- DBI::dbGetQuery(db, "PRAGMA index_list(steps)")
  expect_true("idx_steps_step_hash" %in% step_indexes$name)

  output_cols <- DBI::dbListFields(db, "outputs")
  expect_true("reuse_class" %in% output_cols)
  tracking_output_indexes <- DBI::dbGetQuery(db,
    "PRAGMA index_list(tracking_outputs)")
  expect_true("idx_tracking_outputs_source" %in%
    tracking_output_indexes$name)
  expect_identical(as.integer(DBI::dbGetQuery(
    db, "PRAGMA user_version")[[1L]]), 3L)
})

test_that("a newer database schema fails closed", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  future <- DBI::dbConnect(RSQLite::SQLite(), file.path(home, "dshpc.sqlite"))
  DBI::dbExecute(future, "PRAGMA user_version = 4")
  DBI::dbDisconnect(future)

  expect_error(dsHPC:::.db_connect(), "schema is newer", fixed = TRUE)
})

test_that("v0.3.0 migration preserves attempts, capabilities and shared roots", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home), add = TRUE)
  legacy <- DBI::dbConnect(RSQLite::SQLite(), file.path(home, "dshpc.sqlite"))
  schema <- paste(readLines(test_path("fixtures", "v0.3.0-schema.sql")),
    collapse = "\n")
  for (statement in strsplit(schema, ";", fixed = TRUE)[[1]]) {
    if (nzchar(trimws(statement))) DBI::dbExecute(legacy, statement)
  }
  spec <- make_test_spec(3L)
  dag <- spec
  dag$dag_compiled <- list(node_order = c("a", "b", "c"),
    node_step_index = list(a = 1L, b = 2L, c = 3L))
  dag$.dshpc_runtime_identity <- "preserve-this-seal"
  dag_json <- as.character(jsonlite::toJSON(dag, auto_unbox = TRUE))
  for (job_id in c("job_old_dag", "job_old_linear", "job_old_retry")) {
    DBI::dbExecute(legacy,
      "INSERT INTO jobs (job_id, owner_id, state, step_index, total_steps,
         submitted_at, retry_count, worker_pid, access_token_hash, spec_json)
       VALUES (?, 'owner', 'RUNNING', 2, 3, '2026-01-01', 1, 12345,
               'preserve-capability-hash', ?)",
      params = list(job_id, if (!identical(job_id, "job_old_linear")) dag_json else
        as.character(jsonlite::toJSON(spec, auto_unbox = TRUE))))
    for (i in 1:3) {
      DBI::dbExecute(legacy,
        "INSERT INTO steps (job_id, step_index, type, plane, state,
           external_backend, external_id, output_ref)
         VALUES (?, ?, 'emit', 'session', ?, ?, ?, ?)",
        params = list(job_id, i, c("done", "running", "pending")[i],
          if (i == 2L) "slurm" else NA_character_,
          if (i == 2L) "98765" else NA_character_,
          if (i == 1L) "preserve-output-reference" else NA_character_))
    }
  }
  DBI::dbExecute(legacy,
    "UPDATE jobs SET state = 'PENDING', error_message = 'Retry pending'
      WHERE job_id = 'job_old_retry'")
  DBI::dbExecute(legacy,
    "UPDATE steps SET state = 'failed'
      WHERE job_id = 'job_old_retry' AND step_index = 2")
  DBI::dbExecute(legacy,
    "INSERT INTO resource_leases VALUES
       ('job_old_dag', 'gpu_devices', 1, '{\"devices\":[\"0\"]}', '2026-01-01')")
  DBI::dbExecute(legacy,
    "INSERT INTO tracking_roots (tracking_id, provider, visibility, kind,
       implicit, execution_mode, created_at)
     VALUES ('trk_old', 'dsHPC', 'shared', 'analysis', 1, 'primary', '2026-01-01')")
  DBI::dbExecute(legacy,
    "INSERT INTO tracking_jobs VALUES ('trk_old', 'job_old_dag', 'primary',
                                      '2026-01-01')")
  DBI::dbExecute(legacy,
    "INSERT INTO outputs (job_id, step_index, name, kind, path_or_ref,
       safe_for_client, reuse_class, created_at) VALUES
       ('job_old_dag', 1, 'private', 'artifact_file', 'preserve-path',
        0, 'internal_only', '2026-01-01')")
  roots_before <- DBI::dbGetQuery(legacy, "SELECT * FROM tracking_roots")
  outputs_before <- DBI::dbGetQuery(legacy, "SELECT * FROM outputs")
  DBI::dbDisconnect(legacy)

  db <- dsHPC:::.db_connect()
  jobs <- DBI::dbGetQuery(db, "SELECT * FROM jobs ORDER BY job_id")
  steps <- DBI::dbGetQuery(db,
    "SELECT * FROM steps WHERE job_id = 'job_old_dag' ORDER BY step_index")
  expect_identical(jobs$dag, c(1L, 0L, 1L))
  expect_identical(jobs$step_index, c(1L, 2L, 1L))
  expect_identical(jobs$worker_pid, c(NA_integer_, 12345L, NA_integer_))
  expect_identical(jobs$access_token_hash,
    rep("preserve-capability-hash", 3L))
  expect_identical(jobs$spec_json[1], dag_json)
  expect_identical(steps$state, c("done", "running", "pending"))
  expect_identical(steps$worker_pid, c(NA_integer_, 12345L, NA_integer_))
  expect_identical(steps$retry_count, c(0L, 1L, 0L))
  expect_identical(steps$external_id[2], "98765")
  expect_identical(steps$output_ref[1], "preserve-output-reference")
  retry <- DBI::dbGetQuery(db,
    "SELECT state, retry_count FROM steps
      WHERE job_id = 'job_old_retry' AND step_index = 2")
  expect_identical(retry$state, "retry_pending")
  expect_identical(retry$retry_count, 1L)
  expect_equal(dsHPC:::.scheduler_job_gpu_devices(db, "job_old_dag", 2L), "0")
  expect_identical(DBI::dbGetQuery(db, "SELECT * FROM tracking_roots"),
    roots_before)
  expect_identical(DBI::dbGetQuery(db, "SELECT * FROM outputs"), outputs_before)
  dsHPC:::.db_close(db)

  # Opening the upgraded database again must not copy the progress counter
  # back over the durable attempt or alter any stored specification.
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  expect_identical(DBI::dbGetQuery(db, "SELECT * FROM jobs ORDER BY job_id"), jobs)
  expect_identical(DBI::dbGetQuery(db,
    "SELECT * FROM steps WHERE job_id = 'job_old_dag' ORDER BY step_index"), steps)
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
