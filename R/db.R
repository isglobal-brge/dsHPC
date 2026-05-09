# Module: SQLite Database
# Source of truth. WAL mode. Expanded schema with outputs, checkpoints.

#' @keywords internal
.db_connect <- function() {
  home <- .dshpc_home()
  db_path <- file.path(home, "dshpc.sqlite")
  first_time <- !file.exists(db_path)
  db <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  DBI::dbExecute(db, "PRAGMA journal_mode=WAL")
  DBI::dbExecute(db, "PRAGMA busy_timeout=5000")
  DBI::dbExecute(db, "PRAGMA foreign_keys=ON")
  # Always run the idempotent schema path. Opal/Rock deployments keep
  # persistent volumes across package upgrades, so new scheduler tables must be
  # created on load/connect even when the database already exists.
  .db_create_schema(db)
  db
}

#' @keywords internal
.db_create_schema <- function(db) {
  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS jobs (
      job_id          TEXT PRIMARY KEY,
      owner_id        TEXT NOT NULL,
      state           TEXT NOT NULL DEFAULT 'PENDING',
      step_index      INTEGER NOT NULL DEFAULT 0,
      total_steps     INTEGER NOT NULL,
      resource_class  TEXT DEFAULT 'default',
      priority        INTEGER DEFAULT 0,
      submitted_at    TEXT NOT NULL,
      accepted_at     TEXT,
      started_at      TEXT,
      finished_at     TEXT,
      error_class     TEXT,
      error_message   TEXT,
      retry_count     INTEGER NOT NULL DEFAULT 0,
      worker_pid      INTEGER,
      label           TEXT,
      tags            TEXT,
      visibility      TEXT NOT NULL DEFAULT 'global',
      spec_json       TEXT NOT NULL,
      spec_hash       TEXT
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS steps (
      job_id       TEXT NOT NULL,
      step_index   INTEGER NOT NULL,
      type         TEXT NOT NULL,
      plane        TEXT NOT NULL,
      runner       TEXT,
      state        TEXT NOT NULL DEFAULT 'pending',
      input_refs   TEXT,
      output_ref   TEXT,
      started_at   TEXT,
      finished_at  TEXT,
      exit_code    INTEGER,
      error_class  TEXT,
      error_message TEXT,
      external_backend TEXT,
      external_id  TEXT,
      external_status TEXT,
      PRIMARY KEY (job_id, step_index),
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )")

  .db_ensure_columns(db, "steps", list(
    external_backend = "TEXT",
    external_id = "TEXT",
    external_status = "TEXT"))

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS outputs (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id        TEXT NOT NULL,
      step_index    INTEGER,
      name          TEXT NOT NULL,
      kind          TEXT NOT NULL,
      path_or_ref   TEXT,
      size_bytes    INTEGER,
      safe_for_client INTEGER NOT NULL DEFAULT 0,
      created_at    TEXT NOT NULL,
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS events (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id        TEXT NOT NULL,
      event         TEXT NOT NULL,
      timestamp     TEXT NOT NULL,
      details_json  TEXT,
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS runner_cooldowns (
      runner          TEXT PRIMARY KEY,
      concurrency_group TEXT,
      reason          TEXT NOT NULL,
      until           TEXT NOT NULL,
      last_exit_code  INTEGER,
      failure_count   INTEGER NOT NULL DEFAULT 1,
      updated_at      TEXT NOT NULL
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS resource_leases (
      job_id          TEXT NOT NULL,
      resource        TEXT NOT NULL,
      amount          REAL NOT NULL DEFAULT 0,
      details_json    TEXT,
      acquired_at     TEXT NOT NULL,
      PRIMARY KEY (job_id, resource),
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS worker_nodes (
      worker_id       TEXT PRIMARY KEY,
      cell_id         TEXT NOT NULL,
      node_id         TEXT NOT NULL,
      hostname        TEXT,
      pid             INTEGER,
      state           TEXT NOT NULL DEFAULT 'running',
      started_at      TEXT NOT NULL,
      last_heartbeat  TEXT NOT NULL,
      resources_json  TEXT,
      details_json    TEXT
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS scheduler_locks (
      name            TEXT PRIMARY KEY,
      holder          TEXT NOT NULL,
      acquired_at     TEXT NOT NULL,
      heartbeat_at    TEXT NOT NULL,
      expires_at      TEXT NOT NULL
    )")

  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_jobs_owner ON jobs(owner_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_jobs_spec_hash ON jobs(spec_hash)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_outputs_job ON outputs(job_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_steps_external ON steps(external_backend, external_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_runner_cooldowns_until ON runner_cooldowns(until)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_resource_leases_resource ON resource_leases(resource)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_worker_nodes_cell ON worker_nodes(cell_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_worker_nodes_heartbeat ON worker_nodes(last_heartbeat)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_scheduler_locks_expires ON scheduler_locks(expires_at)")
}

#' @keywords internal
.db_ensure_columns <- function(db, table, columns) {
  existing <- DBI::dbListFields(db, table)
  for (nm in names(columns)) {
    if (!nm %in% existing) {
      DBI::dbExecute(db, paste("ALTER TABLE", table, "ADD COLUMN", nm, columns[[nm]]))
    }
  }
}

#' @keywords internal
.db_close <- function(db) {
  tryCatch(DBI::dbDisconnect(db), error = function(e) NULL)
}

#' @keywords internal
.db_log_event <- function(db, job_id, event, details = NULL) {
  details_json <- if (!is.null(details))
    as.character(jsonlite::toJSON(details, auto_unbox = TRUE))
  else NA_character_
  DBI::dbExecute(db,
    "INSERT INTO events (job_id, event, timestamp, details_json)
     VALUES (?, ?, ?, ?)",
    params = list(job_id, event,
      format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"), details_json))
}

#' Register an output in the outputs table
#' @keywords internal
.db_register_output <- function(db, job_id, step_index, name, kind,
                                 path_or_ref, size_bytes = NA_integer_,
                                 safe_for_client = FALSE) {
  DBI::dbExecute(db,
    "INSERT INTO outputs (job_id, step_index, name, kind, path_or_ref,
                          size_bytes, safe_for_client, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    params = list(job_id, step_index, name, kind, path_or_ref,
      as.integer(size_bytes), as.integer(safe_for_client),
      format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")))
}
