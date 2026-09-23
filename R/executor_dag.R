# Module: Durable ready-set execution
# Only compiled DAG jobs use this path. Linear jobs retain their cursor contract.

.dag_steps <- function(db, job_id) {
  DBI::dbGetQuery(db,
    "SELECT * FROM steps WHERE job_id = ? ORDER BY step_index",
    params = list(job_id))
}

.dag_leader_allows <- function(db) {
  worker_id <- .dshpc_env$.worker_id
  # Inline all-session DAGs and synchronous operator/test calls have no daemon id.
  if (is.null(worker_id)) return(TRUE)
  leader <- .scheduler_worker_leader(db)
  !is.null(leader) && identical(leader$holder, worker_id)
}

.dag_record_failure <- function(db, job_id, reason) {
  if (!.dag_leader_allows(db)) return(invisible(FALSE))
  DBI::dbExecute(db,
    "UPDATE jobs SET error_message = COALESCE(error_message, ?)
      WHERE job_id = ? AND state = 'RUNNING'", params = list(reason, job_id))
  invisible(FALSE)
}

.dag_attempt_current <- function(db, job_id, step_index, attempt_id) {
  row <- DBI::dbGetQuery(db,
    "SELECT 1 FROM steps s JOIN jobs j ON j.job_id = s.job_id
      WHERE s.job_id = ? AND s.step_index = ? AND s.attempt_id = ?
        AND s.state = 'running' AND j.state = 'RUNNING'
        AND j.cancel_requested = 0",
    params = list(job_id, as.integer(step_index), attempt_id))
  nrow(row) == 1L && .dag_leader_allows(db)
}

.dag_reap_current <- function(db, job_id, step_index, attempt_id) {
  row <- DBI::dbGetQuery(db,
    "SELECT s.attempt_id FROM steps s JOIN jobs j ON j.job_id = s.job_id
      WHERE s.job_id = ? AND s.step_index = ?
        AND s.state = 'running' AND j.state = 'RUNNING'",
    params = list(job_id, as.integer(step_index)))
  nrow(row) == 1L && identical(row$attempt_id[1L], attempt_id) &&
    .dag_leader_allows(db)
}

.dag_abandon_launch <- function(db, job_id, step_index, attempt_id) {
  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    changed <- DBI::dbExecute(db,
      "UPDATE steps SET state = 'retry_pending', launch_owner_pid = NULL
        WHERE job_id = ? AND step_index = ? AND attempt_id = ?
          AND state = 'running' AND worker_pid IS NULL",
      params = list(job_id, as.integer(step_index), attempt_id))
    if (changed == 1L) .scheduler_release_step_lease(db, job_id, step_index)
    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
  invisible(FALSE)
}

# A retry belongs to one node; siblings keep running and keep their reservations.
.dag_fail_step <- function(db, job_id, step_index, reason,
                            exit_code = NA_integer_, external_status = NULL,
                            settings = .dshpc_settings(), retry = TRUE,
                            attempt_id = NULL) {
  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    job <- .store_get_job(db, job_id)
    row <- .dag_steps(db, job_id)
    row <- row[row$step_index == step_index, , drop = FALSE]
    if (nrow(row) != 1L || row$state %in% c("done", "failed", "skipped") ||
        (!is.null(attempt_id) && !.dag_reap_current(db, job_id,
          step_index, attempt_id))) {
      DBI::dbExecute(db, "COMMIT")
      return(invisible(FALSE))
    }
    again <- isTRUE(retry) && !isTRUE(as.logical(job$cancel_requested)) &&
      row$retry_count < settings$max_retries
    .store_update_step(db, job_id, step_index,
      state = if (again) "retry_pending" else "failed",
      worker_pid = NA_integer_, launch_owner_pid = NA_integer_,
      error_message = reason, exit_code = as.integer(exit_code),
      external_status = external_status %||% NA_character_,
      retry_count = as.integer(row$retry_count) + as.integer(again),
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .scheduler_release_step_lease(db, job_id, step_index)
    if (again) {
      .store_update_job(db, job_id, retry_count = job$retry_count + 1L)
    } else {
      .dag_record_failure(db, job_id, reason)
    }
    .db_log_event(db, job_id, if (again) "requeued" else "step_failed",
      list(step_index = step_index, reason = reason))
    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
  invisible(TRUE)
}

.dag_dependencies <- function(spec) {
  n <- length(spec$steps)
  lapply(seq_len(n), function(i) {
    refs <- .normalize_step_input_refs(spec$steps[[i]]$inputs)
    indices <- vapply(refs, `[[`, integer(1), "step")
    # The compiler emits topological order. Reject malformed durable references
    # instead of silently scheduling them as roots or leaving an eternal wait.
    if (anyNA(indices) || any(indices < 1L | indices >= i)) {
      stop("Compiled DAG contains an invalid dependency.", call. = FALSE)
    }
    unique(indices)
  })
}

.executor_run_dag <- function(db, job_id, spec = NULL) {
  if (!.dag_leader_allows(db)) return(invisible(FALSE))
  job <- .store_get_job(db, job_id)
  if (is.null(job) || !identical(job$state, "RUNNING")) return(invisible(FALSE))
  if (is.null(spec)) spec <- .store_get_spec(db, job_id)
  settings <- .dshpc_settings_for_spec(spec)
  dependencies <- .dag_dependencies(spec)
  attempted <- integer(0)
  repeat {
    job <- .store_get_job(db, job_id)
    rows <- .dag_steps(db, job_id)
    .store_update_job(db, job_id, step_index = sum(rows$state == "done"))
    if (!identical(job$state, "RUNNING") ||
        isTRUE(as.logical(job$cancel_requested))) break
    active <- sum(rows$state == "running")
    if (active >= settings$max_parallel_steps) break
    candidates <- rows$step_index[rows$state %in%
      c("pending", "retry_pending", "waiting_cache")]
    if (!is.na(job$error_message)) {
      candidates <- intersect(candidates,
        rows$step_index[rows$state == "retry_pending"])
    }
    candidates <- setdiff(candidates, attempted)
    done <- rows$step_index[rows$state == "done"]
    candidates <- candidates[vapply(candidates, function(i) {
      all(dependencies[[i]] %in% done) &&
        !.step_cache_waiting_active(db, job_id, i)
    }, logical(1))]
    if (settings$max_parallel_steps == 1L && is.na(job$error_message)) {
      # In sequential mode a cache waiter also blocks later topological nodes.
      first <- rows$step_index[!rows$state %in% c("done", "skipped")][1L]
      candidates <- intersect(candidates, first)
    }
    if (!length(candidates)) break
    sidx <- candidates[[1L]]
    attempted <- c(attempted, sidx)
    attempt_id <- uuid::UUIDgenerate()
    claimed <- FALSE
    DBI::dbExecute(db, "BEGIN IMMEDIATE")
    tryCatch({
      current <- .store_get_job(db, job_id)
      current_steps <- .dag_steps(db, job_id)
      current_state <- current_steps$state[sidx]
      if (.dag_leader_allows(db) && identical(current$state, "RUNNING") &&
          !isTRUE(as.logical(current$cancel_requested)) &&
          current_state %in% c("pending", "retry_pending", "waiting_cache") &&
          (is.na(current$error_message) || current_state == "retry_pending") &&
          sum(current_steps$state == "running") < settings$max_parallel_steps &&
          all(dependencies[[sidx]] %in%
            current_steps$step_index[current_steps$state == "done"])) {
        decision <- .scheduler_can_start_job(db, job_id, spec, settings,
          step_index = sidx)
        if (isTRUE(decision$ok)) {
          # The writer lock and current leader fence protect attempt cleanup
          # from a replacement worker claiming this node at the same time.
          .reset_failed_step_attempt(db, job_id, sidx, transaction = FALSE)
          .store_update_step(db, job_id, sidx, state = "running",
            attempt_id = attempt_id, launch_owner_pid = Sys.getpid(),
            worker_pid = NA_integer_,
            started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
          .scheduler_acquire_step_lease(db, job_id, sidx, decision)
          claimed <- TRUE
        }
      }
      DBI::dbExecute(db, "COMMIT")
    }, error = function(e) {
      tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
      stop(e)
    })
    if (!claimed) next
    tryCatch({
      .executor_run_step(db, job_id, sidx, spec, attempt_id = attempt_id)
      state <- .dag_steps(db, job_id)$state[sidx]
      if (identical(state, "done")) {
        .scheduler_release_step_lease(db, job_id, sidx)
      }
    }, error = function(e) {
      .dag_fail_step(db, job_id, sidx,
        paste("Dispatch failed:", conditionMessage(e)), retry = FALSE,
        settings = settings, attempt_id = attempt_id)
    })
  }
  .dag_finish_job(db, job_id)
  invisible(TRUE)
}

.dag_finish_job <- function(db, job_id) {
  .tracking_write(db, function() {
    if (!.dag_leader_allows(db)) return(invisible(FALSE))
    job <- .store_get_job(db, job_id)
    if (is.null(job) || !identical(job$state, "RUNNING")) return(invisible(FALSE))
    rows <- .dag_steps(db, job_id)
    .store_update_job(db, job_id, step_index = sum(rows$state == "done"))
    if (any(rows$state == "running")) return(invisible(FALSE))
    cancelled <- isTRUE(as.logical(job$cancel_requested))
    failed <- !is.na(job$error_message) || any(rows$state == "failed")
    if (!cancelled && any(rows$state == "retry_pending")) return(invisible(FALSE))
    if (!failed && !cancelled && !all(rows$state == "done")) return(invisible(FALSE))
    if (!failed && !cancelled) .build_job_result(db, job_id)
    # Result construction can take time. Never publish after losing authority.
    if (!.dag_leader_allows(db)) return(invisible(FALSE))
    job <- .store_get_job(db, job_id)
    if (!identical(job$state, "RUNNING")) return(invisible(FALSE))
    cancelled <- isTRUE(as.logical(job$cancel_requested))
    failed <- failed || !is.na(job$error_message)
    state <- if (cancelled) "CANCELLED" else if (failed) "FAILED" else "FINISHED"
    DBI::dbExecute(db,
      "UPDATE steps SET state = 'skipped', resource_json = NULL
        WHERE job_id = ? AND state IN ('pending','waiting_cache','retry_pending')",
      params = list(job_id))
    .store_update_job(db, job_id, state = state, worker_pid = NA_integer_,
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .scheduler_release_leases(db, job_id)
    .db_log_event(db, job_id, tolower(state))
    invisible(TRUE)
  })
}

.worker_advance_dags <- function(db) {
  jobs <- DBI::dbGetQuery(db,
    "SELECT job_id FROM jobs WHERE dag = 1 AND state = 'RUNNING'")$job_id
  for (jid in jobs) {
    tryCatch(.executor_run_dag(db, jid), error = function(e) {
      .dag_record_failure(db, jid, conditionMessage(e))
      # Invalid durable settings cannot safely launch another attempt. These
      # rows have no live process; running siblings still drain normally.
      DBI::dbExecute(db,
        "UPDATE steps SET state = 'failed' WHERE job_id = ? AND state = 'retry_pending'",
        params = list(jid))
      .dag_finish_job(db, jid)
    })
  }
  invisible(TRUE)
}

.dag_read_pid <- function(path) {
  if (!file.exists(path)) return(NA_integer_)
  tryCatch(suppressWarnings(as.integer(readLines(path, n = 1L,
    warn = FALSE)[1L])), error = function(e) NA_integer_)
}

# A lease takeover must fence a paused preparer before replacing its attempt
# directory. Only registered local daemons are eligible, never an R session.
.dag_retire_launch_owner <- function(db, owner_pid) {
  if (is.na(owner_pid) || owner_pid == Sys.getpid() ||
      !.pid_is_alive(owner_pid)) return(invisible(TRUE))
  leader <- .scheduler_worker_leader(db)
  worker_id <- .dshpc_env$.worker_id
  if (is.null(leader) || is.null(worker_id) ||
      !identical(leader$holder, worker_id)) return(invisible(FALSE))
  owners <- DBI::dbGetQuery(db,
    "SELECT worker_id, details_json FROM worker_nodes
      WHERE pid = ? AND node_id = ? AND worker_id <> ? AND state = 'running'",
    params = list(as.integer(owner_pid), .scheduler_node_id(), worker_id))
  if (nrow(owners) == 0L) return(invisible(FALSE))
  started <- tryCatch(as.numeric(ps::ps_create_time(ps::ps_handle(owner_pid))),
    error = function(e) NA_real_)
  recorded <- tryCatch(jsonlite::fromJSON(owners$details_json[1L])$process_start_time,
    error = function(e) NA_real_)
  if (!is.numeric(recorded) || length(recorded) != 1L ||
      !is.finite(recorded) || !is.finite(started) ||
      abs(recorded - started) > 0.001) return(invisible(FALSE))
  retired <- .terminate_pid(owner_pid)
  if (isTRUE(retired)) {
    DBI::dbExecute(db,
      "UPDATE worker_nodes SET state = 'stopped' WHERE pid = ? AND node_id = ?",
      params = list(as.integer(owner_pid), .scheduler_node_id()))
  }
  invisible(isTRUE(retired))
}

.dag_terminate_child <- function(step_dir) {
  .terminate_pid(.dag_read_pid(file.path(step_dir, "child.pid")))
}

.dag_terminate_step <- function(db, job_id, step_index) {
  rows <- .dag_steps(db, job_id)
  row <- rows[rows$step_index == step_index, , drop = FALSE]
  step_dir <- file.path(.dshpc_home(), "artifacts", job_id,
    sprintf("step_%03d", step_index))
  pid <- row$worker_pid[1L]
  if (is.na(pid)) pid <- .dag_read_pid(file.path(step_dir, "runner.pid"))
  parent <- .terminate_pid(pid)
  child <- .dag_terminate_child(step_dir)
  isTRUE(parent) && isTRUE(child)
}

.executor_kill_dag <- function(db, job_id, settings = NULL) {
  if (is.null(settings)) settings <- .dshpc_settings_for_spec(
    .store_get_spec(db, job_id))
  .store_update_job(db, job_id, cancel_requested = 1L)
  rows <- .dag_steps(db, job_id)
  rows <- rows[rows$state == "running", , drop = FALSE]
  accepted <- TRUE
  requested <- FALSE
  for (i in seq_len(nrow(rows))) {
    sidx <- rows$step_index[i]
    if (identical(rows$plane[i], "session") &&
        .pid_is_alive(rows$launch_owner_pid[i])) {
      # Synchronous work has no subprocess to kill. Its completion path checks
      # the durable cancellation flag before committing the step outcome.
      requested <- TRUE
      next
    }
    if (is.na(rows$external_id[i]) && is.na(rows$worker_pid[i]) &&
        .pid_is_alive(rows$launch_owner_pid[i])) {
      # Submission may already be accepted remotely but not acknowledged yet.
      # Reconciliation cancels its backend id as soon as it becomes durable.
      requested <- TRUE
      next
    }
    if (!is.na(rows$external_id[i]) && nzchar(rows$external_id[i])) {
      ok <- .backend_cancel_step(rows$external_backend[i], rows$external_id[i],
        settings = settings)
      if (isTRUE(ok)) {
        .store_update_step(db, job_id, sidx, external_status = "CANCEL_REQUESTED")
        requested <- TRUE
      } else accepted <- FALSE
    } else if (.dag_terminate_step(db, job_id, sidx)) {
      .dag_fail_step(db, job_id, sidx, "Cancelled", exit_code = 143L,
        settings = settings, retry = FALSE)
    } else accepted <- FALSE
  }
  if (!accepted) return(invisible("failed"))
  if (requested) return(invisible("requested"))
  .dag_finish_job(db, job_id)
  invisible("terminated")
}
