# Module: SQLite Database
# Source of truth. WAL mode. Expanded schema with outputs, checkpoints.

.DSHPC_DB_SCHEMA_VERSION <- 2L

#' @keywords internal
.db_connect <- function() {
  home <- .dshpc_home()
  db_path <- file.path(home, "dshpc.sqlite")
  old_umask <- Sys.umask("0007")
  on.exit(Sys.umask(old_umask), add = TRUE)
  db <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  DBI::dbExecute(db, "PRAGMA journal_mode=WAL")
  DBI::dbExecute(db, "PRAGMA busy_timeout=5000")
  DBI::dbExecute(db, "PRAGMA foreign_keys=ON")
  tryCatch(.db_migrate_schema(db), error = function(e) {
    tryCatch(DBI::dbDisconnect(db), error = function(e2) NULL)
    stop(e)
  })
  for (path in c(db_path, paste0(db_path, "-wal"), paste0(db_path, "-shm"))) {
    if (file.exists(path)) {
      tryCatch(Sys.chmod(path, "0660", use_umask = FALSE),
               error = function(e) NULL)
    }
  }
  db
}

#' Apply durable schema migrations once under SQLite's writer lock
#' @keywords internal
.db_migrate_schema <- function(db) {
  current <- as.integer(DBI::dbGetQuery(db, "PRAGMA user_version")[[1L]])
  if (is.na(current) || current > .DSHPC_DB_SCHEMA_VERSION) {
    stop("dsHPC database schema is newer than this package.", call. = FALSE)
  }
  if (current == .DSHPC_DB_SCHEMA_VERSION) {
    return(invisible(FALSE))
  }
  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    current <- as.integer(DBI::dbGetQuery(db, "PRAGMA user_version")[[1L]])
    if (is.na(current) || current > .DSHPC_DB_SCHEMA_VERSION) {
      stop("dsHPC database schema is newer than this package.", call. = FALSE)
    }
    if (current < .DSHPC_DB_SCHEMA_VERSION) {
      .db_create_schema(db)
      DBI::dbExecute(db, paste0("PRAGMA user_version = ",
        .DSHPC_DB_SCHEMA_VERSION))
    }
    DBI::dbExecute(db, "COMMIT")
    invisible(TRUE)
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
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
      name            TEXT,
      label           TEXT,
      tags            TEXT,
      visibility      TEXT NOT NULL DEFAULT 'private',
      access_token_hash TEXT,
      spec_json       TEXT NOT NULL,
      spec_hash       TEXT
    )")

  .db_ensure_columns(db, "jobs", list(
    name = "TEXT",
    access_token_hash = "TEXT"))

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
      step_hash    TEXT,
      cache_hit    INTEGER NOT NULL DEFAULT 0,
      cache_source_job_id TEXT,
      cache_source_step_index INTEGER,
      PRIMARY KEY (job_id, step_index),
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )")

  .db_ensure_columns(db, "steps", list(
    external_backend = "TEXT",
    external_id = "TEXT",
    external_status = "TEXT",
    step_hash = "TEXT",
    cache_hit = "INTEGER NOT NULL DEFAULT 0",
    cache_source_job_id = "TEXT",
    cache_source_step_index = "INTEGER"))

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
      reuse_class   TEXT NOT NULL DEFAULT 'internal_only',
      created_at    TEXT NOT NULL,
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )")

  .db_ensure_columns(db, "outputs", list(
    reuse_class = "TEXT NOT NULL DEFAULT 'internal_only'"))

  # Analyst-visible tracking roots are deliberately separate from execution
  # jobs. A collection may fan out into many private jobs while contributing
  # exactly one row to the shared queue.
  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS tracking_roots (
      tracking_id  TEXT PRIMARY KEY,
      provider     TEXT NOT NULL,
      reuse_hash   TEXT,
      visibility   TEXT NOT NULL DEFAULT 'scoped',
      kind         TEXT NOT NULL DEFAULT 'analysis',
      lifecycle    TEXT NOT NULL DEFAULT 'OPEN',
      success      INTEGER,
      implicit     INTEGER NOT NULL DEFAULT 0,
      execution_mode TEXT,
      finish_requested INTEGER NOT NULL DEFAULT 0,
      finalizing_job_id TEXT,
      created_at   TEXT NOT NULL,
      finished_at  TEXT
    )")

  .db_ensure_columns(db, "tracking_roots", list(
    execution_mode = "TEXT",
    finish_requested = "INTEGER NOT NULL DEFAULT 0",
    finalizing_job_id = "TEXT"))

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS tracking_jobs (
      tracking_id  TEXT NOT NULL,
      job_id       TEXT NOT NULL,
      role         TEXT NOT NULL DEFAULT 'child',
      attached_at  TEXT NOT NULL,
      PRIMARY KEY (tracking_id, job_id),
      FOREIGN KEY (tracking_id) REFERENCES tracking_roots(tracking_id),
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )")

  # Backfill the durable mode if a development/rolling-upgrade database
  # already contains an unambiguous attachment history.
  DBI::dbExecute(db, "
    UPDATE tracking_roots
       SET execution_mode = CASE
         WHEN EXISTS(SELECT 1 FROM tracking_jobs tj
           WHERE tj.tracking_id = tracking_roots.tracking_id
             AND tj.role = 'primary') THEN 'primary'
         WHEN EXISTS(SELECT 1 FROM tracking_jobs tj
           WHERE tj.tracking_id = tracking_roots.tracking_id
             AND tj.role = 'child') THEN 'child'
       END
     WHERE execution_mode IS NULL
       AND NOT (EXISTS(SELECT 1 FROM tracking_jobs tj
          WHERE tj.tracking_id = tracking_roots.tracking_id
            AND tj.role = 'primary')
         AND EXISTS(SELECT 1 FROM tracking_jobs tj
          WHERE tj.tracking_id = tracking_roots.tracking_id
            AND tj.role = 'child'))")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS tracking_outputs (
      tracking_id       TEXT NOT NULL,
      name              TEXT NOT NULL,
      source_job_id     TEXT,
      source_output_name TEXT,
      provider          TEXT NOT NULL,
      provider_ref      TEXT,
      kind              TEXT NOT NULL,
      reuse_class       TEXT NOT NULL,
      created_at        TEXT NOT NULL,
      PRIMARY KEY (tracking_id, name),
      FOREIGN KEY (tracking_id) REFERENCES tracking_roots(tracking_id),
      FOREIGN KEY (source_job_id) REFERENCES jobs(job_id)
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
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_tracking_roots_page ON tracking_roots(visibility, created_at, tracking_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_tracking_roots_reuse ON tracking_roots(provider, reuse_hash, visibility)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_tracking_jobs_job ON tracking_jobs(job_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_tracking_outputs_root ON tracking_outputs(tracking_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_tracking_outputs_source ON tracking_outputs(source_job_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_steps_external ON steps(external_backend, external_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_steps_step_hash ON steps(step_hash)")
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
.normalize_output_size_bytes <- function(size_bytes) {
  if (length(size_bytes) != 1L || !is.numeric(size_bytes)) {
    stop("size_bytes must be one non-negative whole number or NA.",
      call. = FALSE)
  }
  value <- as.numeric(size_bytes)
  if (is.na(value)) return(NA_real_)
  if (!is.finite(value) || value < 0 || value != floor(value) ||
      value > 2^53) {
    stop("size_bytes must be one non-negative whole number no greater than 2^53.",
      call. = FALSE)
  }
  value
}

#' Register an output in the outputs table
#' @keywords internal
.db_register_output <- function(db, job_id, step_index, name, kind,
                                 path_or_ref, size_bytes = NA_real_,
                                 safe_for_client = FALSE,
                                 reuse_class = NULL) {
  path_or_ref <- .dshpc_validate_job_artifact_path(path_or_ref, job_id,
    check_tree = TRUE)
  size_bytes <- .normalize_output_size_bytes(size_bytes)
  if (!is.logical(safe_for_client) || length(safe_for_client) != 1L ||
      is.na(safe_for_client)) {
    stop("safe_for_client must be TRUE or FALSE.", call. = FALSE)
  }
  if (is.null(reuse_class)) {
    reuse_class <- if (isTRUE(safe_for_client)) "client_safe" else
      "internal_only"
  }
  reuse_class <- match.arg(as.character(reuse_class)[1],
    c("internal_only", "server_reusable", "client_safe"))
  if (identical(reuse_class, "client_safe")) safe_for_client <- TRUE
  if (isTRUE(safe_for_client) && !identical(reuse_class, "client_safe")) {
    stop("Client-safe outputs must use reuse_class='client_safe'.",
      call. = FALSE)
  }
  DBI::dbExecute(db,
    "INSERT INTO outputs (job_id, step_index, name, kind, path_or_ref,
                          size_bytes, safe_for_client, reuse_class, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    params = list(job_id, step_index, name, kind, path_or_ref,
      size_bytes, as.integer(safe_for_client), reuse_class,
      format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")))
}
