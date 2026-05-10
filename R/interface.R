# Module: DataSHIELD Methods
# DS methods are READ-ONLY helpers for the shared SQLite.

#' @keywords internal
.resolve_job_id <- function(x) {
  if (is.character(x) && length(x) == 1 && startsWith(x, "job_")) return(x)
  if (is.character(x) && length(x) == 1) {
    for (depth in 1:3) {
      env <- tryCatch(sys.frame(-(depth)), error = function(e) NULL)
      if (!is.null(env) && exists(x, envir = env, inherits = FALSE)) {
        obj <- get(x, envir = env, inherits = FALSE)
        if (is.list(obj) && !is.null(obj$job_id)) return(obj$job_id)
      }
    }
    if (exists(x, envir = .GlobalEnv, inherits = FALSE)) {
      obj <- get(x, envir = .GlobalEnv, inherits = FALSE)
      if (is.list(obj) && !is.null(obj$job_id)) return(obj$job_id)
    }
  }
  x
}

# =============================================================================
# ASSIGN methods
# =============================================================================

#' Submit a Job
#'
#' DataSHIELD assign method used by clients and domain packages to enqueue a
#' validated job specification. The specification may be a decoded list, JSON,
#' or a `B64:`-prefixed JSON payload.
#'
#' @param spec_encoded Job specification as a list, JSON string, or `B64:`
#'   encoded JSON string.
#' @return Named list containing `job_id`, `state`, and `submitted_at`.
#' @export
hpcSubmitDS <- function(spec_encoded) {
  spec <- .ds_arg(spec_encoded)
  spec <- .validate_job_spec(spec)
  owner_id <- .get_owner_id(spec$.owner)
  job_id <- if (!is.null(spec$job_id) && grepl("^job_", spec$job_id))
    spec$job_id else .generate_job_id()

  db <- .db_connect()
  on.exit(.db_close(db))

  # Skip if job already exists (idempotent -- dual-path submit)
  existing <- .store_get_job(db, job_id)
  if (!is.null(existing)) {
    return(list(job_id = job_id, state = existing$state,
                submitted_at = existing$submitted_at))
  }

  .check_quotas(db, owner_id)

  # Deduplication by spec_hash
  spec_for_hash <- spec[setdiff(names(spec), c("job_id", ".owner"))]
  spec_hash <- digest::digest(jsonlite::toJSON(spec_for_hash, auto_unbox = TRUE),
                               algo = "sha256", serialize = FALSE)
  existing_dup <- DBI::dbGetQuery(db,
    "SELECT job_id, state FROM jobs
     WHERE spec_hash = ?
       AND state IN ('FINISHED', 'PUBLISHED')
     LIMIT 1",
    params = list(spec_hash))
  if (nrow(existing_dup) > 0) {
    # Dedup: create a lightweight entry for the new job_id that
    # mirrors the existing job's state.
    existing_job <- .store_get_job(db, existing_dup$job_id[1])
    .store_create_job(db, job_id, owner_id, spec, length(spec$steps),
      spec_hash = spec_hash)
    .store_update_job(db, job_id,
      state = existing_job$state,
      step_index = as.integer(existing_job$step_index),
      started_at = existing_job$started_at,
      finished_at = existing_job$finished_at)
    .db_log_event(db, job_id, "deduplicated",
      list(original_job_id = existing_dup$job_id[1]))

    # Copy outputs from existing job
    existing_outputs <- DBI::dbGetQuery(db,
      "SELECT name, kind, path_or_ref, size_bytes, safe_for_client
       FROM outputs WHERE job_id = ?",
      params = list(existing_dup$job_id[1]))
    for (i in seq_len(nrow(existing_outputs))) {
      o <- existing_outputs[i, ]
      .db_register_output(db, job_id, NA_integer_, o$name, o$kind,
        o$path_or_ref, o$size_bytes, as.logical(o$safe_for_client))
    }

    job <- .store_get_job(db, job_id)
    return(list(job_id = job_id, state = job$state,
                 deduplicated = TRUE,
                 submitted_at = job$submitted_at))
  }

  .store_create_job(db, job_id, owner_id, spec, length(spec$steps),
                     spec_hash = spec_hash)

  # If all steps are session-plane, execute inline (synchronous).
  # Artifact-plane steps are deferred to the worker daemon.
  all_session <- all(vapply(spec$steps, function(s)
    identical(s$plane, "session"), logical(1)))

  if (all_session) {
    # Execute synchronously -- session steps are brief and idempotent
    .store_update_job(db, job_id, state = "RUNNING", step_index = 1L,
      started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .db_log_event(db, job_id, "started")
    tryCatch(
      .executor_run_step(db, job_id, 1L, spec),
      error = function(e) {
        .store_update_job(db, job_id, state = "FAILED",
          error_message = conditionMessage(e),
          finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
      }
    )
  } else {
    # Has artifact steps -- needs the worker daemon
    tryCatch(.dshpc_worker_start(), error = function(e) NULL)
  }

  job <- .store_get_job(db, job_id)
  list(job_id = job_id,
       state = job$state %||% "PENDING",
       submitted_at = job$submitted_at %||% format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
}

#' Load a Job Output into the Server Session
#'
#' When \code{as_descriptor = TRUE} and the output is a Parquet file,
#' returns a \code{FlowerDatasetDescriptor} instead of loading the data
#' into memory. This enables zero-copy column projection downstream.
#'
#' @param job_id_or_symbol Character; job ID or symbol name
#' @param output_name Character; name of the output to load
#' @param as_descriptor Logical; if TRUE and output is Parquet, return a
#'   FlowerDatasetDescriptor instead of loading data into memory
#' Load a job output (server-side only)
#'
#' NOT a DataSHIELD method -- not directly callable by users.
#' Domain packages should use this internally
#' after verifying ownership and applying their own disclosure controls.
#'
#' @param job_id_or_symbol Job ID or symbol.
#' @param output_name Output name.
#' @param as_descriptor If TRUE, return FlowerDatasetDescriptor for Parquet.
#' @param required_label If non-NULL, verify the job has this label (ownership check).
#' @return The loaded object.
#' @export
hpcLoadOutputDS <- function(job_id_or_symbol, output_name,
                             as_descriptor = FALSE, required_label = NULL) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))

  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  if (!job$state %in% c("FINISHED", "PUBLISHED"))
    stop("Job not finished (state: ", job$state, ").", call. = FALSE)

  # Ownership check: if required_label is set, verify the job belongs to that package
  if (!is.null(required_label)) {
    job_label <- job$label %||% ""
    if (!grepl(required_label, job_label, fixed = TRUE))
      stop("Job '", job_id, "' does not belong to '", required_label,
           "'. Access denied.", call. = FALSE)
  }

  out <- DBI::dbGetQuery(db,
    "SELECT path_or_ref, kind FROM outputs WHERE job_id = ? AND name = ?
     ORDER BY id DESC LIMIT 1",
    params = list(job_id, output_name))
  if (nrow(out) == 0)
    stop("Output '", output_name, "' not found for job ", job_id, ".", call. = FALSE)

  path <- out$path_or_ref[1]
  if (is.na(path) || !file.exists(path))
    stop("Output file not found on disk.", call. = FALSE)

  # Disclosure control: tabular outputs must have >= nfilter rows
  # Non-tabular outputs (RDS, JSON, binary) also blocked if row count unknown
  n_rows <- .count_output_rows(path)
  nfilter <- .dshpc_disclosure_settings()$nfilter_subset
  if (is.na(n_rows)) {
    # For RDS files, try to count if it's a data.frame
    if (grepl("\\.rds$", path, ignore.case = TRUE)) {
      obj <- tryCatch(readRDS(path), error = function(e) NULL)
      if (is.data.frame(obj)) n_rows <- nrow(obj)
    }
  }
  if (!is.na(n_rows) && n_rows < nfilter)
    stop("Output has ", n_rows, " rows, below minimum (nfilter.subset = ",
         nfilter, "). Cannot load disclosive data.", call. = FALSE)

  # Descriptor mode: return a FlowerDatasetDescriptor for Parquet outputs
  if (isTRUE(as_descriptor) && grepl("\\.parquet$", path, ignore.case = TRUE)) {
    pf <- arrow::read_parquet(path, as_data_frame = FALSE)
    col_names <- names(pf)
    n_rows <- nrow(pf)

    desc <- list(
      dataset_id  = paste0("dshpc.", job_id, ".", output_name),
      source_kind = "staged_parquet",
      metadata    = list(
        file    = path,
        format  = "parquet",
        n_rows  = n_rows,
        columns = col_names
      ),
      staged_token = paste0("job_", job_id),
      origin       = "dsHPC"
    )
    class(desc) <- "FlowerDatasetDescriptor"
    return(desc)
  }

  # Load the file as an R object based on extension
  obj <- if (grepl("\\.rds$", path, ignore.case = TRUE)) {
    readRDS(path)
  } else if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
    as.data.frame(arrow::read_parquet(path))
  } else if (grepl("\\.csv$", path, ignore.case = TRUE)) {
    utils::read.csv(path, stringsAsFactors = FALSE)
  } else if (grepl("\\.json$", path, ignore.case = TRUE)) {
    jsonlite::fromJSON(readLines(path, warn = FALSE), simplifyVector = TRUE)
  } else {
    list(type = "job_output_ref", job_id = job_id, output_name = output_name,
         kind = out$kind[1], path = path)
  }

  obj
}

#' Count rows in a tabular output file for disclosure control
#' @return Integer row count, or NA for non-tabular files.
#' @keywords internal
.count_output_rows <- function(path) {
  if (grepl("\\.csv$", path, ignore.case = TRUE)) {
    # Fast row count without loading
    return(length(readLines(path, warn = FALSE)) - 1L)
  }
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
    return(nrow(arrow::read_parquet(path, as_data_frame = FALSE)))
  }
  # Non-tabular: can't count rows
  NA_integer_
}

# =============================================================================
# AGGREGATE methods (read-only, no ownership check)
# =============================================================================

#' Get Job Status
#'
#' Return disclosure-safe state for one job.
#'
#' @param job_id_or_symbol Job id, or a symbol resolving to an object with a
#'   `job_id` field in the server session.
#' @return Named list with job id, state, progress, timestamps, sanitized error
#'   string, and retry count.
#' @export
hpcStatusDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found: ", job_id, call. = FALSE)

  # Sanitize error message -- may contain data values from Python runners
  safe_error <- if (!is.na(job$error_message) && nzchar(job$error_message))
    sub(":.*", "", job$error_message) else NA_character_

  list(job_id = job$job_id, state = job$state,
    step_index = as.integer(job$step_index),
    total_steps = as.integer(job$total_steps),
    label = job$label,
    submitted_at = job$submitted_at, started_at = job$started_at,
    finished_at = job$finished_at, error = safe_error,
    retries = as.integer(job$retry_count))
}

#' Get Job Result
#'
#' Return the disclosure-safe result object for a completed job. Raw artifact
#' outputs are never returned directly through this method.
#'
#' @param job_id_or_symbol Job id, or a symbol resolving to an object with a
#'   `job_id` field in the server session.
#' @return A named list. If the job is not complete, the list contains
#'   `ready = FALSE`; otherwise it contains the safe result metadata.
#' @export
hpcResultDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)

  if (!job$state %in% c("FINISHED", "PUBLISHED")) {
    safe_err <- if (!is.na(job$error_message) && nzchar(job$error_message))
      sub(":.*", "", job$error_message) else NA_character_
    return(list(job_id = job_id, state = job$state, ready = FALSE,
                error = safe_err))
  }

  home <- .dshpc_home()
  result_path <- file.path(home, "artifacts", job_id, "result", "result.rds")
  if (file.exists(result_path)) {
    result <- readRDS(result_path)
    result$ready <- TRUE
    return(result)
  }
  .build_job_result(db, job_id)
}

#' Get Job Logs
#'
#' Return a sanitized tail of stdout/stderr logs for a job.
#'
#' @param job_id_or_symbol Job id, or a symbol resolving to an object with a
#'   `job_id` field in the server session.
#' @param last_n Maximum number of log lines to return. Values above 200 are
#'   capped server-side.
#' @return Character vector of sanitized log lines.
#' @export
hpcLogsDS <- function(job_id_or_symbol, last_n = 50L) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  last_n <- as.integer(last_n %||% 50L)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)

  home <- .dshpc_home()
  lines <- character(0)
  art_dir <- file.path(home, "artifacts", job_id)
  if (dir.exists(art_dir)) {
    step_dirs <- sort(list.dirs(art_dir, full.names = TRUE, recursive = FALSE))
    step_dirs <- step_dirs[grepl("^step_", basename(step_dirs))]
    for (sd in step_dirs) {
      for (lf in c("stdout.log", "stderr.log")) {
        lp <- file.path(sd, lf)
        if (file.exists(lp)) {
          sl <- readLines(lp, warn = FALSE)
          if (length(sl) > 0)
            lines <- c(lines, paste0("[", basename(sd), "/", lf, "] ", sl))
        }
      }
    }
  }
  .sanitize_job_logs(lines, last_n)
}

#' List Jobs
#'
#' Returns all jobs, optionally filtered by label.
#'
#' @param label Character or NULL; filter by label.
#' @export
hpcListDS <- function(label = NULL) {
  db <- .db_connect()
  on.exit(.db_close(db))

  jobs <- .store_list_jobs(db, label = label)
  if (nrow(jobs) == 0)
    return(data.frame(job_id = character(0), state = character(0),
      label = character(0), submitted_at = character(0),
      progress = character(0), stringsAsFactors = FALSE))
  jobs$progress <- paste0(jobs$step_index, "/", jobs$total_steps)
  # Safe fields only -- no tags, owner_id, visibility (could be disclosive)
  jobs[, c("job_id", "state", "label", "submitted_at", "progress"), drop = FALSE]
}

#' List Available Outputs for a Job
#'
#' Return output names and metadata for a job without loading the output values.
#'
#' @param job_id_or_symbol Job id, or a symbol resolving to an object with a
#'   `job_id` field in the server session.
#' @return Data frame with output name, kind, disclosure flag, and size.
#' @export
hpcOutputsDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  DBI::dbGetQuery(db,
    "SELECT name, kind, safe_for_client, size_bytes FROM outputs
     WHERE job_id = ? ORDER BY id",
    params = list(job_id))
}

#' Get Server Job Capabilities
#' @export
hpcCapabilitiesDS <- function() {
  settings <- .dshpc_settings()
  runners <- .list_runners()
  runner_details <- lapply(runners, function(r) {
    cfg <- .load_runner_config(r)
    if (is.null(cfg)) return(list(name = r))
    profile <- .scheduler_runner_profile(r, settings)
    container <- .backend_runner_container(cfg, settings)
    list(name = cfg$name %||% r, plane = cfg$plane %||% "artifact",
         resource_class = cfg$resource_class %||% "default",
         container = if (!is.null(container)) list(
           image = container$image,
           runtime = container$runtime$name,
           runtime_available = nzchar(container$runtime$command),
           pull = container$pull
         ) else NULL,
         resources = list(
           memory_mb = profile$memory_mb,
           cpu_slots = profile$cpu_slots,
           gpus = profile$gpus,
           optional_gpus = profile$optional_gpus,
           max_concurrent = profile$max_concurrent,
           concurrency_group = profile$concurrency_group,
           accelerator = profile$accelerator))
  })
  names(runner_details) <- runners

  worker_health <- .dshpc_worker_health()
  active <- tryCatch({
    db <- .db_connect()
    on.exit(.db_close(db), add = TRUE)
    DBI::dbGetQuery(db,
      "SELECT state, COUNT(*) AS n FROM jobs
       WHERE state IN ('PENDING','RUNNING') GROUP BY state")
  }, error = function(e) {
    data.frame(state = character(0), n = integer(0), stringsAsFactors = FALSE)
  })

  list(dshpc_version = as.character(utils::packageVersion("dsHPC")),
       runners = runner_details, publishers = .list_publishers(),
       home = .dshpc_home(must_exist = FALSE),
       directories = .dshpc_home_health(),
       active_jobs = active,
       max_jobs_global = settings$max_jobs_global,
       max_steps_per_job = settings$max_steps_per_job,
       executor = .executor_backend_status(settings),
       scheduler = .scheduler_status(),
       worker = worker_health,
       runner_registry = list(
         paths = settings$runner_registry_paths,
         autosync = settings$runner_registry_autosync,
         installed = .dshpc_env$.runner_registry_installed %||% character(0)
       ),
       admin_enabled = .admin_is_configured())
}

#' Get Scheduler Status
#' @export
hpcSchedulerStatusDS <- function() {
  .scheduler_status()
}

# =============================================================================
# Admin methods (disabled by default, enabled by dshpc.admin_key option or
# DSHPC_ADMIN_KEY environment variable)
# =============================================================================

#' Verify admin key. Disabled if no key configured.
#' Key arrives B64-encoded from client to avoid Opal parser issues.
#' @keywords internal
.verify_admin_key <- function(admin_key) {
  expected <- .dshpc_option("admin_key", NULL)

  if (is.null(expected) || !nzchar(expected))
    stop("Admin access is not enabled on this server.", call. = FALSE)

  # Decode B64 transport
  decoded <- .ds_arg(admin_key)
  if (is.list(decoded)) decoded <- decoded$.admin_key

  if (is.null(decoded) || !nzchar(decoded))
    stop("Access denied: admin_key required.", call. = FALSE)

  if (!identical(decoded, expected))
    stop("Access denied: invalid admin_key.", call. = FALSE)

  invisible(TRUE)
}

#' Check if admin is configured
#' @keywords internal
.admin_is_configured <- function() {
  key <- .dshpc_option("admin_key", NULL)
  !is.null(key) && nzchar(key)
}

#' List ALL Jobs (admin only)
#'
#' Disabled by default. Enable by setting dshpc.admin_key on the server:
#'   dsadmin.set_option(con, "dshpc.admin_key", "your_secret_key")
#' or by setting DSHPC_ADMIN_KEY in the Rock/HPC environment.
#'
#' @param admin_key Character; the admin key.
#' @param label Character or NULL; filter by label.
#' @export
hpcAdminListDS <- function(admin_key = NULL, label = NULL) {
  .verify_admin_key(admin_key)
  db <- .db_connect()
  on.exit(.db_close(db))
  jobs <- .store_list_jobs(db, label = label)
  if (nrow(jobs) == 0)
    return(data.frame(job_id = character(0), state = character(0),
      label = character(0), submitted_at = character(0),
      progress = character(0), stringsAsFactors = FALSE))
  jobs$progress <- paste0(jobs$step_index, "/", jobs$total_steps)
  # Safe fields only -- no tags, owner_id, visibility (could be disclosive)
  jobs[, c("job_id", "state", "label", "submitted_at", "progress"), drop = FALSE]
}

#' Cancel Any Job (admin only)
#'
#' Disabled by default. Enable by setting dshpc.admin_key or DSHPC_ADMIN_KEY.
#'
#' @param job_id Character; job ID.
#' @param admin_key Character; the admin key.
#' @export
hpcAdminCancelDS <- function(job_id, admin_key = NULL) {
  .verify_admin_key(admin_key)
  job_id <- .resolve_job_id(job_id)
  db <- .db_connect()
  on.exit(.db_close(db))

  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  if (job$state %in% c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED"))
    stop("Job already in terminal state: ", job$state, call. = FALSE)

  .executor_kill(db, job_id)
  .scheduler_release_leases(db, job_id)
  .store_update_job(db, job_id, state = "CANCELLED", worker_pid = NA_integer_,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
  .db_log_event(db, job_id, "admin_cancelled")
  list(job_id = job_id, state = "CANCELLED")
}
