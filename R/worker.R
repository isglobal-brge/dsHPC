# Module: Worker Daemon
# External process. NOT started from .onAttach().
# Started by admin via: Rscript inst/worker/main.R /srv/dsjobs
# Or by helper: dsJobs:::.dsjobs_worker_start()
# Supervised by systemd, Docker restart policy, or cron.

#' Start the worker daemon (admin/setup helper, NOT auto-start)
#' @keywords internal
.dsjobs_worker_start <- function() {
  home <- .dsjobs_home()
  active <- tryCatch({
    db <- .db_connect()
    on.exit(.db_close(db), add = TRUE)
    leader <- .scheduler_worker_leader(db)
    if (!is.null(leader) && !.leader_process_alive(db, leader)) {
      .mark_workers_stopped(worker_ids = leader$holder)
      leader <- NULL
    }
    leader
  }, error = function(e) NULL)
  if (!is.null(active)) {
    message("dsJobs worker already running for cell (", active$holder, ")")
    return(invisible(active$holder))
  }

  pid_file <- file.path(home, "worker.pid")
  if (file.exists(pid_file)) {
    pid <- tryCatch(as.integer(readLines(pid_file, n = 1, warn = FALSE)),
                     error = function(e) NA_integer_)
    if (.pid_is_alive(pid)) {
      message("dsJobs worker already running (PID ", pid, ")")
      return(invisible(NULL))
    }
    unlink(pid_file)
  }
  log_file <- file.path(home, "worker.log")
  worker_script <- system.file("worker", "main.R", package = "dsJobs")

  # Spawn the worker as a detached process via setsid + nohup so that it does
  # not inherit Rserve's session/process group. Some embedded R hosts
  # (Rserve under Rosetta amd64-on-arm64) propagate process state to grand-
  # children through processx in ways that break torch's MKL loader; running
  # the daemon under its own session sidesteps that entirely.
  #
  # MKL workaround vars are also exported so any nested torch loads in the
  # daemon itself inherit them. They are no-ops on native hosts.
  rscript <- file.path(R.home("bin"), "Rscript")
  setsid <- Sys.which("setsid")
  if (!nzchar(setsid)) setsid <- NULL
  if (!is.null(setsid) && file.exists("/usr/bin/env")) {
    cmd <- "/usr/bin/env"
    args <- c("MKL_SERVICE_FORCE_INTEL=0", "MKL_THREADING_LAYER=GNU",
              "DSJOBS_WORKER=1",
              setsid, "-f", rscript, worker_script, home)
  } else {
    cmd <- rscript
    args <- c(worker_script, home)
  }
  proc <- processx::process$new(
    command = cmd, args = args,
    stdout = log_file, stderr = log_file,
    env = c("current",
            DSJOBS_WORKER = "1",
            MKL_SERVICE_FORCE_INTEL = "0",
            MKL_THREADING_LAYER = "GNU"),
    cleanup = FALSE, cleanup_tree = FALSE)
  writeLines(as.character(proc$get_pid()), pid_file)
  .dsjobs_env$.worker <- proc

  # When launched through `setsid -f`, processx sees the short-lived launcher
  # PID, while the actual R worker gets a different PID. Wait briefly for the
  # worker heartbeat and rewrite worker.pid with the real daemon PID.
  actual_pid <- .wait_for_worker_pid(home, timeout_secs = 5)
  if (!is.na(actual_pid)) {
    writeLines(as.character(actual_pid), pid_file)
    message("dsJobs worker started (PID ", actual_pid, ")")
    return(invisible(actual_pid))
  }

  message("dsJobs worker start requested (launcher PID ", proc$get_pid(), ")")
  invisible(proc$get_pid())
}

#' Stop the worker daemon
#' @keywords internal
.dsjobs_worker_stop <- function() {
  home <- .dsjobs_home()
  pid_file <- file.path(home, "worker.pid")

  snapshot <- .worker_runtime_snapshot(home)
  pids <- unique(stats::na.omit(as.integer(snapshot$pids)))

  if (length(pids) == 0 && file.exists(pid_file)) {
    pids <- tryCatch(
      as.integer(readLines(pid_file, n = 1, warn = FALSE)),
      error = function(e) integer(0))
  }

  for (pid in pids) .terminate_pid(pid)
  .mark_workers_stopped(pids = pids, worker_ids = snapshot$worker_ids)

  if (file.exists(pid_file)) {
    unlink(pid_file)
  }
  message("dsJobs worker stopped.")
}

#' Check whether a scheduler leader still has a live process
#' @keywords internal
.leader_process_alive <- function(db, leader) {
  if (is.null(leader$holder)) return(FALSE)
  worker <- tryCatch(DBI::dbGetQuery(db,
    "SELECT pid FROM worker_nodes WHERE worker_id = ?",
    params = list(leader$holder)), error = function(e) data.frame())
  nrow(worker) > 0 && .pid_is_alive(as.integer(worker$pid[1]))
}

#' Wait for a detached worker to publish its actual PID
#' @keywords internal
.wait_for_worker_pid <- function(home, timeout_secs = 5) {
  deadline <- Sys.time() + timeout_secs
  repeat {
    snapshot <- .worker_runtime_snapshot(home)
    pids <- unique(stats::na.omit(as.integer(snapshot$pids)))
    alive <- pids[vapply(pids, .pid_is_alive, logical(1))]
    if (length(alive) > 0) return(alive[1])
    if (Sys.time() >= deadline) break
    Sys.sleep(0.2)
  }
  NA_integer_
}

#' Runtime snapshot of known worker processes
#' @keywords internal
.worker_runtime_snapshot <- function(home = .dsjobs_home()) {
  pids <- integer(0)
  worker_ids <- character(0)

  tryCatch({
    db <- .db_connect()
    on.exit(.db_close(db), add = TRUE)
    leader <- .scheduler_worker_leader(db)
    if (!is.null(leader)) worker_ids <- c(worker_ids, leader$holder)

    ttl <- .scheduler_worker_ttl_secs(.dsjobs_settings())
    stale_cutoff <- format(Sys.time() - ttl * 2, "%Y-%m-%dT%H:%M:%OS3Z",
                           tz = "UTC")
    workers <- DBI::dbGetQuery(db,
      "SELECT worker_id, pid FROM worker_nodes
       WHERE state = 'running' AND last_heartbeat > ?",
      params = list(stale_cutoff))
    if (nrow(workers) > 0) {
      worker_ids <- c(worker_ids, workers$worker_id)
      pids <- c(pids, as.integer(workers$pid))
    }
  }, error = function(e) NULL)

  health_path <- file.path(home, "worker.health")
  if (file.exists(health_path)) {
    h <- tryCatch(jsonlite::fromJSON(readLines(health_path, warn = FALSE)),
                  error = function(e) NULL)
    if (!is.null(h)) {
      pids <- c(pids, suppressWarnings(as.integer(h$pid)))
      worker_ids <- c(worker_ids, h$worker_id %||% character(0))
    }
  }

  pid_file <- file.path(home, "worker.pid")
  if (file.exists(pid_file)) {
    pids <- c(pids, tryCatch(
      as.integer(readLines(pid_file, n = 1, warn = FALSE)),
      error = function(e) NA_integer_))
  }

  list(
    pids = unique(stats::na.omit(as.integer(pids))),
    worker_ids = unique(stats::na.omit(as.character(worker_ids)))
  )
}

#' Mark stopped workers in the scheduler catalog
#' @keywords internal
.mark_workers_stopped <- function(pids = integer(0), worker_ids = character(0)) {
  tryCatch({
    db <- .db_connect()
    on.exit(.db_close(db), add = TRUE)
    now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

    for (pid in unique(stats::na.omit(as.integer(pids)))) {
      ids <- DBI::dbGetQuery(db,
        "SELECT worker_id FROM worker_nodes WHERE pid = ?",
        params = list(pid))$worker_id
      worker_ids <- c(worker_ids, ids)
      DBI::dbExecute(db,
        "UPDATE worker_nodes SET state = 'stopped', last_heartbeat = ?
         WHERE pid = ?",
        params = list(now, pid))
    }

    for (worker_id in unique(stats::na.omit(as.character(worker_ids)))) {
      DBI::dbExecute(db,
        "UPDATE worker_nodes SET state = 'stopped', last_heartbeat = ?
         WHERE worker_id = ?",
        params = list(now, worker_id))
      DBI::dbExecute(db,
        "DELETE FROM scheduler_locks
         WHERE name = 'worker_leader' AND holder = ?",
        params = list(worker_id))
    }
  }, error = function(e) NULL)
  invisible(TRUE)
}

#' Terminate a process reliably across native and emulated containers
#' @keywords internal
.terminate_pid <- function(pid, grace_secs = 2) {
  pid <- suppressWarnings(as.integer(pid))
  if (is.na(pid) || !.pid_is_alive(pid)) return(invisible(TRUE))

  .signal_pid(pid, signal = 15L)
  deadline <- Sys.time() + grace_secs
  while (.pid_is_alive(pid) && Sys.time() < deadline) Sys.sleep(0.1)

  if (.pid_is_alive(pid)) {
    .signal_pid(pid, signal = 9L)
    deadline <- Sys.time() + 1
    while (.pid_is_alive(pid) && Sys.time() < deadline) Sys.sleep(0.1)
  }

  invisible(!.pid_is_alive(pid))
}

#' Send a signal to a process
#' @keywords internal
.signal_pid <- function(pid, signal = 15L) {
  pid <- suppressWarnings(as.integer(pid))
  if (is.na(pid)) return(invisible(FALSE))

  kill <- Sys.which("kill")
  if (nzchar(kill)) {
    status <- tryCatch(
      system2(kill, c(paste0("-", as.integer(signal)), as.character(pid)),
              stdout = FALSE, stderr = FALSE),
      error = function(e) 1L)
    if (identical(as.integer(status), 0L)) return(invisible(TRUE))
  }

  tryCatch({
    tools::pskill(pid, signal = as.integer(signal))
    TRUE
  }, error = function(e) FALSE)
}

#' Write worker health file (for monitoring)
#' @keywords internal
.worker_write_health <- function() {
  home <- .dsjobs_home()
  health <- list(
    pid = Sys.getpid(),
    worker_id = .dsjobs_env$.worker_id %||% NA_character_,
    cell_id = .dsjobs_env$.cell_id %||% NA_character_,
    leader = isTRUE(.dsjobs_env$.worker_is_leader),
    alive = TRUE,
    last_heartbeat = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"),
    uptime_secs = as.numeric(difftime(Sys.time(),
      .dsjobs_env$.worker_started_at %||% Sys.time(), units = "secs"))
  )
  health_path <- file.path(home, "worker.health")
  writeLines(jsonlite::toJSON(health, auto_unbox = TRUE, pretty = TRUE), health_path)
}

#' Check worker health status
#' @keywords internal
.dsjobs_worker_health <- function() {
  home <- .dsjobs_home(must_exist = FALSE)
  if (is.null(home)) return(list(alive = FALSE, reason = "no DSJOBS_HOME"))
  db_health <- tryCatch({
    db <- .db_connect()
    on.exit(.db_close(db), add = TRUE)
    leader <- .scheduler_worker_leader(db)
    if (!is.null(leader)) {
      worker <- DBI::dbGetQuery(db,
        "SELECT worker_id, cell_id, node_id, hostname, pid, state,
                started_at, last_heartbeat, resources_json
         FROM worker_nodes WHERE worker_id = ?",
        params = list(leader$holder))
      if (nrow(worker) > 0) {
        row <- as.list(worker[1, ])
        return(c(list(alive = TRUE, leader = leader), row))
      }
      return(list(alive = TRUE, leader = leader, worker_id = leader$holder))
    }
    NULL
  }, error = function(e) NULL)
  if (!is.null(db_health)) return(db_health)

  health_path <- file.path(home, "worker.health")
  if (!file.exists(health_path)) return(list(alive = FALSE, reason = "no health file"))
  tryCatch({
    h <- jsonlite::fromJSON(readLines(health_path, warn = FALSE))
    last <- as.POSIXct(h$last_heartbeat, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
    stale <- as.numeric(difftime(Sys.time(), last, units = "secs")) > 30
    list(alive = !stale && .pid_is_alive(h$pid), pid = h$pid,
         last_heartbeat = h$last_heartbeat, stale = stale)
  }, error = function(e) list(alive = FALSE, reason = e$message))
}

#' Main worker loop (runs inside the worker process)
#' @keywords internal
.worker_main <- function() {
  # Ensure DSJOBS_HOME directories exist with correct permissions
  home <- .dsjobs_home()
  for (subdir in c("artifacts", "publish", "staging", "runners")) {
    d <- file.path(home, subdir)
    dir.create(d, recursive = TRUE, showWarnings = FALSE)
    tryCatch(Sys.chmod(d, "0777"), error = function(e) NULL)
  }
  pid_file <- file.path(home, "worker.pid")
  tryCatch(writeLines(as.character(Sys.getpid()), pid_file),
           error = function(e) NULL)

  db <- .db_connect()
  settings <- .dsjobs_settings()
  gc_counter <- 0L
  resources <- .scheduler_node_budget(settings)
  .dsjobs_env$.cell_id <- .scheduler_cell_id(settings)
  .dsjobs_env$.worker_id <- .scheduler_worker_id(settings)
  .dsjobs_env$.worker_started_at <- Sys.time()
  on.exit({
    current <- tryCatch(readLines(pid_file, n = 1, warn = FALSE),
                        error = function(e) character(0))
    if (length(current) > 0 && identical(current[1], as.character(Sys.getpid()))) {
      tryCatch(unlink(pid_file), error = function(e) NULL)
    }
    tryCatch(.scheduler_release_worker_leader(db, .dsjobs_env$.worker_id),
             error = function(e) NULL)
    .db_close(db)
  })
  .worker_log("Worker started (PID ", Sys.getpid(), ", worker ",
    .dsjobs_env$.worker_id, ", cell ", .dsjobs_env$.cell_id, ")")

  repeat {
    tryCatch({
      is_leader <- .scheduler_renew_worker_leader(db, .dsjobs_env$.worker_id,
        resources = resources)
      .dsjobs_env$.worker_is_leader <- is_leader
      .worker_write_health()

      if (isTRUE(is_leader)) {
        .worker_reap(db)
        .worker_dispatch(db)

        gc_counter <- gc_counter + 1L
        if (gc_counter >= 100L) { .worker_gc(db); gc_counter <- 0L }
      }
    }, error = function(e) .worker_log("ERROR: ", conditionMessage(e)))
    Sys.sleep(settings$worker_poll_secs)
  }
}

#' @keywords internal
.worker_dispatch <- function(db) {
  settings <- .dsjobs_settings()
  pending <- DBI::dbGetQuery(db,
    "SELECT job_id FROM jobs WHERE state = 'PENDING'
     ORDER BY priority DESC, submitted_at LIMIT ?",
    params = list(settings$scheduler_scan_limit))
  if (nrow(pending) == 0) return(invisible(FALSE))

  for (jid in pending$job_id) {
    spec <- NULL
    decision <- NULL
    claimed <- FALSE

    DBI::dbExecute(db, "BEGIN IMMEDIATE")
    tx_error <- tryCatch({
      job <- .store_get_job(db, jid)
      if (!is.null(job) && identical(job$state, "PENDING")) {
        spec <- .store_get_spec(db, jid)
        if (!is.null(spec)) {
          decision <- .scheduler_can_start_job(db, jid, spec, settings)
          if (isTRUE(decision$ok)) {
            .scheduler_acquire_leases(db, jid, decision)
            .store_update_job(db, jid, state = "RUNNING", step_index = 1L,
              started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z",
                                   tz = "UTC"))
            .db_log_event(db, jid, "started",
              list(scheduler = settings$scheduler,
                   memory_mb = decision$plan$memory_mb %||% 0L,
                   cpu_slots = decision$plan$cpu_slots %||% 0L,
                   gpus = decision$plan$gpus %||% 0L,
                   optional_gpus = decision$plan$optional_gpus %||% 0L,
                   assigned_gpu_devices = decision$gpu_devices %||% character(0),
                   gpu_memory_mb = decision$plan$gpu_memory_mb %||% 0L))
            claimed <- TRUE
          }
        }
      }
      NULL
    }, error = function(e) e)

    if (inherits(tx_error, "error")) {
      tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
      .worker_log("Dispatch claim error for ", jid, ": ",
                  conditionMessage(tx_error))
      next
    }
    DBI::dbExecute(db, "COMMIT")

    if (!isTRUE(claimed)) next

    # Run outside the scheduler transaction. Artifact processes must not inherit
    # an open SQLite write transaction; otherwise a long Python/R job can stall
    # the worker heartbeat and block other scheduler activity.
    tryCatch({
      .executor_run_step(db, jid, 1L, spec)
    }, error = function(e) {
      tryCatch(.scheduler_release_leases(db, jid), error = function(e2) NULL)
      .store_update_job(db, jid, state = "FAILED",
        error_message = paste("Dispatch failed:", conditionMessage(e)),
        worker_pid = NA_integer_,
        finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    })
  }
  invisible(TRUE)
}

#' @keywords internal
.worker_reap <- function(db) {
  running <- DBI::dbGetQuery(db,
    "SELECT j.job_id, j.worker_pid, j.step_index,
            s.runner, s.state AS step_state,
            s.external_backend, s.external_id
     FROM jobs j
     LEFT JOIN steps s
       ON s.job_id = j.job_id AND s.step_index = j.step_index
     WHERE j.state = 'RUNNING'")
  if (nrow(running) == 0) return()

  for (i in seq_len(nrow(running))) {
    jid <- running$job_id[i]
    sidx <- as.integer(running$step_index[i])
    runner_name <- running$runner[i]
    step_state <- running$step_state[i]
    external_id <- running$external_id[i]
    external_backend <- running$external_backend[i]
    step_dir <- file.path(.dsjobs_home(), "artifacts", jid,
                           sprintf("step_%03d", sidx))

    if (!is.na(step_state) && identical(step_state, "done")) {
      tryCatch(.executor_advance(db, jid),
        error = function(e) .worker_log("Advance recovery error for ", jid,
          ": ", conditionMessage(e)))
      next
    }

    if ((is.na(external_id) || !nzchar(external_id)) &&
        (is.na(external_backend) || !nzchar(external_backend))) {
      marker <- .backend_read_external_marker(step_dir)
      if (!is.null(marker)) {
        external_backend <- marker$backend
        external_id <- marker$external_id
        .store_update_step(db, jid, sidx,
          external_backend = external_backend,
          external_id = external_id,
          external_status = marker$status %||% "submitted")
        .db_log_event(db, jid, "external_marker_recovered",
          list(step_index = sidx, backend = external_backend,
               external_id = external_id))
      }
    }

    if (!is.na(external_id) && nzchar(external_id)) {
      status <- .backend_step_status(external_backend, external_id, step_dir)
      if (identical(status$state, "running")) {
        if (!is.null(status$external_state)) {
          tryCatch(.store_update_step(db, jid, sidx,
            external_status = status$external_state), error = function(e) NULL)
        }
        next
      }
      exit_code <- as.integer(status$exit_code %||% 1L)
      .worker_finalize_artifact_step(db, jid, sidx, runner_name, exit_code,
        external_status = status$external_state %||% status$state)
      next
    }

    if (is.na(running$worker_pid[i])) {
      .worker_requeue_interrupted_step(db, jid, sidx,
        "Running job has no live worker or external backend id")
      next
    }
    pid <- as.integer(running$worker_pid[i])

    # Use processx handle if available (reliable), fall back to PID check
    still_alive <- .proc_is_alive(jid, sidx)
    if (!still_alive) still_alive <- .pid_is_alive(pid)

    if (!still_alive) {
      # Use processx exit status if available
      proc_exit <- .proc_get_exit(jid, sidx)
      exit_code <- if (!is.na(proc_exit)) proc_exit else .read_exit_code(step_dir)
      if (is.na(exit_code)) {
        .worker_requeue_interrupted_step(db, jid, sidx,
          "Artifact process disappeared without exit_code")
      } else {
        .worker_finalize_artifact_step(db, jid, sidx, runner_name, exit_code)
      }
    }
  }
}

#' Requeue or fail a running step whose process disappeared
#' @keywords internal
.worker_requeue_interrupted_step <- function(db, jid, sidx, reason) {
  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    settings <- .dsjobs_settings()
    job <- .store_get_job(db, jid)
    retries <- as.integer(job$retry_count %||% 0L)
    now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

    .store_update_step(db, jid, sidx, state = "failed",
      error_message = reason, finished_at = now)
    .scheduler_release_leases(db, jid)

    if (retries < settings$max_retries) {
      .store_update_job(db, jid, state = "PENDING",
        retry_count = retries + 1L, worker_pid = NA_integer_)
      .db_log_event(db, jid, "requeued",
        list(step_index = sidx, reason = reason,
             retry_count = retries + 1L))
    } else {
      .store_update_job(db, jid, state = "FAILED",
        error_message = reason, worker_pid = NA_integer_,
        finished_at = now)
      .db_log_event(db, jid, "failed",
        list(step_index = sidx, reason = reason))
    }
    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    .worker_log("Recovery error for ", jid, " step ", sidx, ": ",
      conditionMessage(e))
  })
}

#' @keywords internal
.worker_finalize_artifact_step <- function(db, jid, sidx, runner_name,
                                           exit_code,
                                           external_status = NULL) {
  advance_after_commit <- FALSE
  committed <- FALSE
  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    if (identical(as.integer(exit_code), 0L)) {
      output_ref <- file.path("artifacts", jid,
                               sprintf("step_%03d", sidx), "output")
      out_dir <- file.path(.dsjobs_home(), output_ref)
      if (dir.exists(out_dir)) {
        files <- list.files(out_dir, full.names = TRUE)
        for (f in files) {
          .db_register_output(db, jid, sidx, basename(f),
            "artifact_file", f, file.info(f)$size, safe_for_client = FALSE)
        }
      }
      updates <- list(state = "done", exit_code = 0L,
        output_ref = output_ref,
        finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
      if (!is.null(external_status)) updates$external_status <- external_status
      do.call(.store_update_step, c(list(db, jid, sidx), updates))
      .store_update_job(db, jid, worker_pid = NA_integer_)
      .db_log_event(db, jid, "step_done", list(step_index = sidx))
      advance_after_commit <- TRUE
    } else {
      settings <- .dsjobs_settings()
      job <- .store_get_job(db, jid)
      retries <- as.integer(job$retry_count %||% 0L)
      .scheduler_record_runner_failure(db, runner_name, exit_code,
        reason = "artifact_step_failed")
      updates <- list(state = "failed",
        exit_code = as.integer(exit_code),
        error_message = paste("Exit:", exit_code),
        finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
      if (!is.null(external_status)) updates$external_status <- external_status
      do.call(.store_update_step, c(list(db, jid, sidx), updates))
      .scheduler_release_leases(db, jid)
      if (retries < settings$max_retries) {
        .store_update_job(db, jid, state = "PENDING",
          retry_count = retries + 1L, worker_pid = NA_integer_)
      } else {
        .store_update_job(db, jid, state = "FAILED",
          error_message = paste("Step", sidx, "failed (exit", exit_code, ")"),
          worker_pid = NA_integer_,
          finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
      }
    }
    DBI::dbExecute(db, "COMMIT")
    committed <- TRUE
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    .worker_log("Reap error for ", jid, " step ", sidx, ": ",
      conditionMessage(e))
  })
  if (isTRUE(committed) && isTRUE(advance_after_commit)) {
    tryCatch(.executor_advance(db, jid),
      error = function(e) .worker_log("Advance error for ", jid, ": ",
        conditionMessage(e)))
  }
}

#' @keywords internal
.read_exit_code <- function(step_dir) {
  # 1. Explicit exit code file (written by well-behaved runners)
  ef <- file.path(step_dir, "exit_code")
  if (file.exists(ef)) {
    code <- tryCatch(as.integer(readLines(ef, n = 1, warn = FALSE)),
                      error = function(e) NA_integer_)
    if (!is.na(code)) return(code)
  }

  # 2. Compatibility for legacy jobs launched before dsJobs wrote durable
  # exit_code files. New wrappers must write exit_code; output files alone are
  # not enough to prove success after a crash.
  output_dir <- file.path(step_dir, "output")
  run_script <- file.path(step_dir, "run.sh")
  legacy_launcher <- !file.exists(run_script)
  if (!legacy_launcher) {
    lines <- tryCatch(readLines(run_script, warn = FALSE),
                      error = function(e) character(0))
    legacy_launcher <- !any(grepl("exit_code", lines, fixed = TRUE))
  }
  if (legacy_launcher && dir.exists(output_dir) &&
      length(list.files(output_dir)) > 0) return(0L)

  # 3. Check stderr for real errors (not just warnings)
  stderr_path <- file.path(step_dir, "stderr.log")
  if (file.exists(stderr_path)) {
    stderr_lines <- readLines(stderr_path, warn = FALSE)
    # Only count as error if stderr contains ERROR/Traceback/Exception
    has_error <- any(grepl("ERROR|Traceback|Exception|FATAL|panic",
                            stderr_lines, ignore.case = FALSE))
    if (has_error) return(1L)
  }

  NA_integer_
}

#' @keywords internal
.worker_gc <- function(db) {
  settings <- .dsjobs_settings()
  cutoff <- format(Sys.time() - settings$job_expiry_hours * 3600,
                    "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  expired <- DBI::dbGetQuery(db,
    "SELECT job_id FROM jobs
     WHERE state IN ('FINISHED','PUBLISHED','FAILED','CANCELLED')
       AND finished_at IS NOT NULL AND finished_at < ?",
    params = list(cutoff))
  for (jid in expired$job_id) {
    DBI::dbExecute(db, "DELETE FROM outputs WHERE job_id = ?", params = list(jid))
    DBI::dbExecute(db, "DELETE FROM events WHERE job_id = ?", params = list(jid))
    DBI::dbExecute(db, "DELETE FROM steps WHERE job_id = ?", params = list(jid))
    DBI::dbExecute(db, "DELETE FROM jobs WHERE job_id = ?", params = list(jid))
    ad <- file.path(.dsjobs_home(), "artifacts", jid)
    if (dir.exists(ad)) unlink(ad, recursive = TRUE)
  }
  if (nrow(expired) > 0) .worker_log("GC removed ", nrow(expired), " jobs")

  # Expire jobs stuck in PENDING too long
  pending_cutoff <- format(
    Sys.time() - settings$pending_timeout_hours * 3600,
    "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  stale_pending <- DBI::dbExecute(db,
    "UPDATE jobs SET state = 'FAILED',
     error_message = 'Pending timeout exceeded',
     finished_at = ?
     WHERE state = 'PENDING' AND submitted_at < ?",
    params = list(
      format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"),
      pending_cutoff))
  if (stale_pending > 0)
    .worker_log("Expired ", stale_pending, " stale PENDING jobs")

  # Also clean stale asset generations (dsImaging)
  if (requireNamespace("dsImaging", quietly = TRUE)) {
    tryCatch({
      n_stale <- dsImaging::cleanup_stale_generations(max_age_hours = 2)
      if (n_stale > 0) .worker_log("GC cleaned ", n_stale, " stale generations")
    }, error = function(e) NULL)
  }
}

#' @keywords internal
.worker_log <- function(...) {
  message("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", paste0(...))
}
