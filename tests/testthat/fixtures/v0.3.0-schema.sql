-- Exact durable schema generated from dsHPC v0.3.0 R/db.R.
CREATE TABLE jobs (
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
    );
CREATE TABLE steps (
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
    );
CREATE TABLE outputs (
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
    );
CREATE TABLE tracking_roots (
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
    );
CREATE TABLE tracking_jobs (
      tracking_id  TEXT NOT NULL,
      job_id       TEXT NOT NULL,
      role         TEXT NOT NULL DEFAULT 'child',
      attached_at  TEXT NOT NULL,
      PRIMARY KEY (tracking_id, job_id),
      FOREIGN KEY (tracking_id) REFERENCES tracking_roots(tracking_id),
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    );
CREATE TABLE tracking_outputs (
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
    );
CREATE TABLE events (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id        TEXT NOT NULL,
      event         TEXT NOT NULL,
      timestamp     TEXT NOT NULL,
      details_json  TEXT,
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    );
CREATE TABLE runner_cooldowns (
      runner          TEXT PRIMARY KEY,
      concurrency_group TEXT,
      reason          TEXT NOT NULL,
      until           TEXT NOT NULL,
      last_exit_code  INTEGER,
      failure_count   INTEGER NOT NULL DEFAULT 1,
      updated_at      TEXT NOT NULL
    );
CREATE TABLE resource_leases (
      job_id          TEXT NOT NULL,
      resource        TEXT NOT NULL,
      amount          REAL NOT NULL DEFAULT 0,
      details_json    TEXT,
      acquired_at     TEXT NOT NULL,
      PRIMARY KEY (job_id, resource),
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    );
CREATE TABLE worker_nodes (
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
    );
CREATE TABLE scheduler_locks (
      name            TEXT PRIMARY KEY,
      holder          TEXT NOT NULL,
      acquired_at     TEXT NOT NULL,
      heartbeat_at    TEXT NOT NULL,
      expires_at      TEXT NOT NULL
    );
CREATE INDEX idx_jobs_state ON jobs(state);
CREATE INDEX idx_jobs_owner ON jobs(owner_id);
CREATE INDEX idx_jobs_spec_hash ON jobs(spec_hash);
CREATE INDEX idx_outputs_job ON outputs(job_id);
CREATE INDEX idx_tracking_roots_page ON tracking_roots(visibility, created_at, tracking_id);
CREATE INDEX idx_tracking_roots_reuse ON tracking_roots(provider, reuse_hash, visibility);
CREATE INDEX idx_tracking_jobs_job ON tracking_jobs(job_id);
CREATE INDEX idx_tracking_outputs_root ON tracking_outputs(tracking_id);
CREATE INDEX idx_tracking_outputs_source ON tracking_outputs(source_job_id);
CREATE INDEX idx_events_job ON events(job_id);
CREATE INDEX idx_steps_external ON steps(external_backend, external_id);
CREATE INDEX idx_steps_step_hash ON steps(step_hash);
CREATE INDEX idx_runner_cooldowns_until ON runner_cooldowns(until);
CREATE INDEX idx_resource_leases_resource ON resource_leases(resource);
CREATE INDEX idx_worker_nodes_cell ON worker_nodes(cell_id);
CREATE INDEX idx_worker_nodes_heartbeat ON worker_nodes(last_heartbeat);
CREATE INDEX idx_scheduler_locks_expires ON scheduler_locks(expires_at);
PRAGMA user_version = 2;
