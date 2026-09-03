# Module: DataSHIELD Methods
#
# Intended use:
# - hpcSubmitInternal and hpcLoadOutputInternal are low-level infrastructure
#   functions for trusted server-side packages after domain authorization.
# - Legacy *DS wrappers that were registered by older deployments remain
#   exported only to report that the corresponding methods were retired.

#' Stop a legacy DataSHIELD method that must remain disabled
#' @keywords internal
.legacy_ds_method_disabled <- function(method) {
  stop(method, " was retired from the DataSHIELD API. Use the replacement server API.",
       call. = FALSE)
}

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

#' Generate a high-entropy per-job bearer capability
#' @keywords internal
.generate_job_capability <- function() {
  ids <- uuid::UUIDgenerate(use.time = FALSE, n = 2L)
  paste0("cap_", paste0(gsub("-", "", ids, fixed = TRUE), collapse = ""))
}

#' Hash a bearer capability for durable storage
#' @keywords internal
.hash_job_capability <- function(capability) {
  digest::digest(capability, algo = "sha256", serialize = FALSE)
}

#' Encode a job id and capability as one DataSHIELD-safe scalar
#' @keywords internal
.encode_job_bearer <- function(job_id, capability) {
  payload <- list(job_id = job_id, .dshpc_capability = capability)
  json <- as.character(jsonlite::toJSON(payload, auto_unbox = TRUE,
    null = "null"))
  encoded <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(json)))
  encoded <- gsub("\\+", "-", encoded)
  encoded <- gsub("/", "_", encoded)
  encoded <- gsub("=+$", "", encoded)
  paste0("B64:", encoded)
}

#' Resolve a job symbol, raw id, or encoded bearer
#' @keywords internal
.resolve_job_access <- function(x) {
  if (is.character(x) && length(x) == 1L && !is.na(x)) {
    if (startsWith(x, "B64:")) {
      x <- tryCatch(.ds_arg(x), error = function(e) NULL)
    } else {
      for (env in rev(sys.frames())) {
        # The process-global workspace is not a DataSHIELD session. In
        # single-process servers (notably DSLite), consulting it would let one
        # session resolve a private handle left by unrelated server code.
        if (identical(env, .GlobalEnv)) next
        obj <- if (exists(x, envir = env, inherits = FALSE))
          get(x, envir = env, inherits = FALSE) else NULL
        if (is.list(obj) && !is.null(obj$job_id)) {
          x <- obj
          break
        }
      }
    }
  }

  if (is.list(x)) {
    return(list(
      job_id = x$job_id %||% NULL,
      capability = x$.dshpc_capability %||% NULL
    ))
  }
  list(job_id = x, capability = NULL)
}

#' Require possession of a job bearer capability
#' @keywords internal
.require_job_access <- function(db, job_id_or_symbol) {
  deny <- function() stop("Job not found or access denied.", call. = FALSE)
  ref <- .resolve_job_access(job_id_or_symbol)
  if (!is.character(ref$job_id) || length(ref$job_id) != 1L ||
      is.na(ref$job_id) || !nzchar(ref$job_id) ||
      !is.character(ref$capability) || length(ref$capability) != 1L ||
      is.na(ref$capability) || !nzchar(ref$capability)) {
    deny()
  }

  job <- .store_get_job(db, ref$job_id)
  stored_hash <- if (is.null(job)) NULL else job$access_token_hash
  if (is.null(stored_hash) || length(stored_hash) != 1L ||
      is.na(stored_hash) || !nzchar(stored_hash) ||
      !identical(.hash_job_capability(ref$capability),
                 as.character(stored_hash))) {
    deny()
  }

  list(job_id = ref$job_id, capability = ref$capability, job = job,
       bearer = .encode_job_bearer(ref$job_id, ref$capability))
}

#' Prepare an immutable, confined clone plan for a completed global job
#' @noRd
.prepare_deduplicated_job_clone <- function(db, source_job_id) {
  source_root <- .dshpc_validate_job_artifact_path(
    file.path(.dshpc_home(), "artifacts", source_job_id), source_job_id,
    check_tree = TRUE)
  outputs <- DBI::dbGetQuery(db,
    "SELECT step_index, name, kind, path_or_ref, size_bytes, safe_for_client
     FROM outputs WHERE job_id = ? ORDER BY id",
    params = list(source_job_id))
  steps <- DBI::dbGetQuery(db,
    "SELECT step_index, state, output_ref, started_at, finished_at, exit_code,
            step_hash
     FROM steps WHERE job_id = ? ORDER BY step_index",
    params = list(source_job_id))

  relative_to_source <- function(path) {
    validated <- .dshpc_validate_job_artifact_path(path, source_job_id,
      check_tree = TRUE)
    if (identical(validated, source_root)) return("")
    prefix <- paste0(source_root, "/")
    if (!startsWith(validated, prefix)) {
      stop("Deduplicated job artifacts failed validation.", call. = FALSE)
    }
    substring(validated, nchar(prefix) + 1L)
  }
  output_rel <- if (nrow(outputs) > 0L) {
    vapply(outputs$path_or_ref, relative_to_source, character(1))
  } else character(0)
  step_rel <- rep(NA_character_, nrow(steps))
  for (i in seq_len(nrow(steps))) {
    ref <- steps$output_ref[i]
    if (is.na(ref) || !nzchar(ref)) next
    source_path <- .dshpc_resolve_job_artifact_ref(ref, source_job_id,
      check_tree = TRUE)
    step_rel[i] <- relative_to_source(source_path)
  }
  list(root = source_root, outputs = outputs, output_rel = output_rel,
    steps = steps, step_rel = step_rel)
}

#' Clone a completed global job into a separately owned artifact tree
#' @noRd
.clone_deduplicated_job <- function(db, source_job_id, target_job_id, plan) {
  target_root <- file.path(.dshpc_home(), "artifacts", target_job_id)
  if (file.exists(target_root) || dir.exists(target_root) ||
      .dshpc_path_is_symlink(target_root)) {
    stop("Deduplicated job target is unavailable.", call. = FALSE)
  }
  .copy_input_tree(plan$root, target_root,
    target_root = file.path(.dshpc_home(), "artifacts"))
  target_root <- .dshpc_validate_job_artifact_path(target_root, target_job_id,
    check_tree = TRUE)

  if (nrow(plan$outputs) > 0L) {
    for (i in seq_len(nrow(plan$outputs))) {
      o <- plan$outputs[i, ]
      target_path <- if (!nzchar(plan$output_rel[i])) target_root else
        file.path(target_root, plan$output_rel[i])
      .db_register_output(db, target_job_id,
        as.integer(o$step_index), o$name, o$kind, target_path,
        size_bytes = if (file.exists(target_path)) file.info(target_path)$size
          else o$size_bytes,
        safe_for_client = as.logical(o$safe_for_client))
    }
  }

  for (i in seq_len(nrow(plan$steps))) {
    s <- plan$steps[i, ]
    target_ref <- if (is.na(plan$step_rel[i])) NA_character_ else
      file.path("artifacts", target_job_id, plan$step_rel[i])
    .store_update_step(db, target_job_id, as.integer(s$step_index),
      state = s$state,
      output_ref = target_ref,
      started_at = s$started_at,
      finished_at = s$finished_at,
      exit_code = as.integer(s$exit_code),
      step_hash = s$step_hash,
      cache_hit = 1L,
      cache_source_job_id = source_job_id,
      cache_source_step_index = as.integer(s$step_index))
  }
  invisible(TRUE)
}

#' Complete a clone only while it still owns the CLONING state
#' @noRd
.complete_deduplicated_job_clone <- function(db, target_job_id, source_job,
                                              recovered = FALSE) {
  if (is.null(source_job) ||
      !source_job$state %in% c("FINISHED", "PUBLISHED")) {
    stop("Deduplicated job source state is invalid.", call. = FALSE)
  }
  updated <- DBI::dbExecute(db,
    "UPDATE jobs
     SET state = ?, step_index = ?, started_at = ?, finished_at = ?
     WHERE job_id = ? AND state = 'CLONING'",
    params = list(source_job$state, as.integer(source_job$step_index),
      source_job$started_at, source_job$finished_at, target_job_id))
  if (!identical(as.integer(updated), 1L)) {
    stop("Deduplicated job state changed during clone completion.",
      call. = FALSE)
  }
  details <- list(original_job_id = source_job$job_id)
  if (isTRUE(recovered)) details$recovered_after_restart <- TRUE
  .db_log_event(db, target_job_id, "deduplicated", details)
  invisible(TRUE)
}

#' Remove a failed, not-yet-visible deduplicated job clone
#' @noRd
.discard_deduplicated_job_clone <- function(db, job_id) {
  for (table in c("outputs", "events", "steps", "jobs")) {
    DBI::dbExecute(db, paste0("DELETE FROM ", table, " WHERE job_id = ?"),
      params = list(job_id))
  }
  target_root <- file.path(.dshpc_home(), "artifacts", job_id)
  if (dir.exists(target_root) || file.exists(target_root) ||
      .dshpc_path_is_symlink(target_root)) {
    unlink(target_root, recursive = TRUE, force = TRUE)
  }
  invisible(TRUE)
}

#' Recover interrupted whole-job clones
#' @noRd
.recover_deduplicated_job_clones <- function(db) {
  cloning <- DBI::dbGetQuery(db,
    "SELECT job_id, spec_hash, label, visibility, submitted_at
     FROM jobs WHERE state = 'CLONING' ORDER BY submitted_at")
  if (nrow(cloning) == 0L) return(invisible(FALSE))

  clone_is_active <- function(job_id, submitted_at) {
    event <- DBI::dbGetQuery(db,
      "SELECT details_json FROM events
       WHERE job_id = ? AND event = 'created' ORDER BY id LIMIT 1",
      params = list(job_id))
    if (nrow(event) != 1L || is.na(event$details_json[1])) return(FALSE)
    details <- tryCatch(jsonlite::fromJSON(event$details_json[1],
      simplifyVector = FALSE), error = function(e) NULL)
    owner <- details$clone_owner %||% NULL
    if (!is.list(owner)) return(FALSE)

    submitted <- as.POSIXct(submitted_at, format = "%Y-%m-%dT%H:%M:%OSZ",
      tz = "UTC")
    age <- suppressWarnings(as.numeric(difftime(Sys.time(), submitted,
      units = "secs")))
    node <- as.character(owner$node %||% "")
    pid <- suppressWarnings(as.integer(owner$pid %||% NA_integer_))
    # PID reuse cannot postpone recovery forever. A live local PID protects an
    # active clone for one day; a different shared-cell node gets a one-hour
    # grace because its process namespace cannot be inspected from here.
    if (nzchar(node) && identical(node, .scheduler_node_id())) {
      return(!is.na(pid) && .pid_is_alive(pid) &&
        is.finite(age) && age < 86400)
    }
    is.finite(age) && age < 3600
  }

  reset_target <- function(job_id) {
    job_id <- .validate_identifier(job_id, "job_id")
    current <- .store_get_job(db, job_id)
    if (is.null(current) || !identical(current$state, "CLONING")) {
      return(invisible(FALSE))
    }
    DBI::dbExecute(db, "DELETE FROM outputs WHERE job_id = ?",
      params = list(job_id))
    DBI::dbExecute(db,
      "UPDATE steps
       SET state = 'pending', output_ref = NULL, started_at = NULL,
           finished_at = NULL, exit_code = NULL, error_class = NULL,
           error_message = NULL, external_backend = NULL, external_id = NULL,
           external_status = NULL, step_hash = NULL, cache_hit = 0,
           cache_source_job_id = NULL, cache_source_step_index = NULL
       WHERE job_id = ?",
      params = list(job_id))
    target_root <- file.path(.dshpc_home(), "artifacts", job_id)
    if (file.exists(target_root) || dir.exists(target_root) ||
        .dshpc_path_is_symlink(target_root)) {
      unlink(target_root, recursive = TRUE, force = TRUE)
    }
    if (file.exists(target_root) || dir.exists(target_root) ||
        .dshpc_path_is_symlink(target_root)) {
      stop("Interrupted clone storage could not be reset.", call. = FALSE)
    }
    invisible(TRUE)
  }

  for (i in seq_len(nrow(cloning))) {
    target <- cloning[i, ]
    job_id <- target$job_id[1]
    if (clone_is_active(job_id, target$submitted_at[1])) next
    recovered <- tryCatch({
      reset_target(job_id)
      if (!identical(target$visibility[1], "global") ||
          is.na(target$spec_hash[1]) || !nzchar(target$spec_hash[1]) ||
          is.na(target$label[1]) || !nzchar(target$label[1])) {
        stop("Interrupted clone metadata is invalid.", call. = FALSE)
      }
      source <- DBI::dbGetQuery(db,
        "SELECT job_id FROM jobs
         WHERE job_id <> ? AND spec_hash = ? AND visibility = 'global'
           AND label = ? AND state IN ('FINISHED', 'PUBLISHED')
         ORDER BY finished_at DESC, submitted_at DESC LIMIT 1",
        params = list(job_id, target$spec_hash[1], target$label[1]))
      if (nrow(source) != 1L) {
        stop("Interrupted clone source is unavailable.", call. = FALSE)
      }
      source_job_id <- source$job_id[1]
      source_job <- .store_get_job(db, source_job_id)
      plan <- .prepare_deduplicated_job_clone(db, source_job_id)
      .clone_deduplicated_job(db, source_job_id, job_id, plan)
      .complete_deduplicated_job_clone(db, job_id, source_job,
        recovered = TRUE)
      TRUE
    }, error = function(e) FALSE)

    if (!isTRUE(recovered)) {
      tryCatch(reset_target(job_id), error = function(e) NULL)
      tryCatch({
        updated <- DBI::dbExecute(db,
          "UPDATE jobs
           SET state = 'FAILED',
               error_message = 'Deduplicated job recovery failed.',
               worker_pid = NULL, finished_at = ?
           WHERE job_id = ? AND state = 'CLONING'",
          params = list(format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z",
            tz = "UTC"), job_id))
        if (identical(as.integer(updated), 1L)) {
          .db_log_event(db, job_id, "clone_recovery_failed")
        }
      }, error = function(e) NULL)
    }
  }
  invisible(TRUE)
}

# =============================================================================
# SERVER-TO-SERVER JOB API AND DISABLED LEGACY WRAPPERS
# =============================================================================

#' Disabled Legacy DataSHIELD Job Submission
#'
#' This compatibility symbol always reports that the method was retired. Older
#' DataSHIELD server configurations may retain an allowlist entry after a
#' package upgrade, so the exported name remains available with no submission
#' behavior.
#'
#' @param spec_encoded Ignored.
#' @return This function never returns successfully.
#' @export
hpcSubmitDS <- function(spec_encoded) {
  .legacy_ds_method_disabled("hpcSubmitDS")
}

#' Submit a Job from Trusted Server Code
#'
#' Server-side function used by domain packages to enqueue a validated job
#' specification. Domain packages must authorize the request and compose a
#' fixed workflow before calling this function. It is not registered as a
#' DataSHIELD assign method because a generic client-controlled workflow can
#' bypass domain disclosure controls. The specification may be a decoded list, JSON,
#' or a `B64:`-prefixed JSON payload. The decoded specification must contain a
#' non-empty character `label` field identifying the submitting server-side
#' domain package.
#'
#' @param spec_encoded Job specification as a list, JSON string, or `B64:`
#'   encoded JSON string.
#' @return Server-session handle containing `job_id`, a per-job bearer
#'   capability, resolved `name`, `state`, and `submitted_at`. The capability
#'   is stored durably only as a SHA-256 hash.
#' @export
hpcSubmitInternal <- function(spec_encoded) {
  .dshpc_require_trusted_server_caller()
  spec <- .ds_arg(spec_encoded)
  spec <- .validate_job_spec(spec)
  .dshpc_require_label_value(spec$label,
    "dsHPC submission requires a domain label (spec$label). Every job must declare the server-side package that submitted it. This is a hard requirement; there is no opt-out.")
  .dshpc_require_trusted_server_caller(spec$label)
  owner_id <- .get_owner_id(spec$.owner)
  # Job identifiers are server-generated. Caller-selected identifiers enable
  # collision/oracle behaviour and are not part of the domain API contract.
  spec$job_id <- NULL
  job_id <- .generate_job_id()

  db <- .db_connect()
  on.exit(.db_close(db))

  # Never re-issue a capability for a caller-selected existing id.
  existing <- .store_get_job(db, job_id)
  if (!is.null(existing)) {
    stop("Job id already exists; submit with a new job id.", call. = FALSE)
  }

  .check_quotas(db, owner_id)

  capability <- .generate_job_capability()
  capability_hash <- .hash_job_capability(capability)

  # Deduplication by spec_hash
  spec_for_hash <- spec[setdiff(names(spec), c("job_id", ".owner", "name"))]
  spec_for_hash <- .canonicalise_spec(spec_for_hash)
  spec_hash <- digest::digest(jsonlite::toJSON(spec_for_hash, auto_unbox = TRUE),
                              algo = "sha256", serialize = FALSE)
  existing_dup <- if (identical(spec$visibility, "global")) {
    DBI::dbGetQuery(db,
      "SELECT job_id, state FROM jobs
       WHERE spec_hash = ?
         AND visibility = 'global'
         AND label = ?
         AND state IN ('FINISHED', 'PUBLISHED')
       LIMIT 1",
      params = list(spec_hash, spec$label))
  } else {
    data.frame(job_id = character(0), state = character(0))
  }
  if (nrow(existing_dup) > 0) {
    source_job_id <- existing_dup$job_id[1]
    clone_plan <- tryCatch(
      .prepare_deduplicated_job_clone(db, source_job_id),
      error = function(e) NULL)
    if (!is.null(clone_plan)) {
      existing_job <- .store_get_job(db, source_job_id)
      .store_create_job(db, job_id, owner_id, spec, length(spec$steps),
        spec_hash = spec_hash, access_token_hash = capability_hash,
        initial_state = "CLONING",
        clone_owner = list(node = .scheduler_node_id(), pid = Sys.getpid()))
      cloned <- tryCatch({
        .clone_deduplicated_job(db, source_job_id, job_id, clone_plan)
        TRUE
      }, error = function(e) FALSE)
      if (isTRUE(cloned)) {
        .complete_deduplicated_job_clone(db, job_id, existing_job)
        job <- .store_get_job(db, job_id)
        return(list(job_id = job_id, .dshpc_capability = capability,
                    state = job$state,
                    name = job$name,
                    deduplicated = TRUE,
                    submitted_at = job$submitted_at))
      }
      .discard_deduplicated_job_clone(db, job_id)
    }
  }

  .store_create_job(db, job_id, owner_id, spec, length(spec$steps),
                     spec_hash = spec_hash,
                     access_token_hash = capability_hash)

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
  list(job_id = job_id, .dshpc_capability = capability,
       state = job$state %||% "PENDING",
       name = job$name,
       submitted_at = job$submitted_at %||% format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
}

#' Disabled Legacy DataSHIELD Output Loading
#'
#' This compatibility symbol always reports that the method was retired,
#' including when an upgraded server still has the former assign method in its
#' persisted allowlist.
#'
#' @param job_id_or_symbol Ignored.
#' @param output_name Ignored.
#' @param as_descriptor Ignored.
#' @param required_label Ignored.
#' @return This function never returns successfully.
#' @export
hpcLoadOutputDS <- function(job_id_or_symbol, output_name,
                            as_descriptor = FALSE, required_label = NULL) {
  .legacy_ds_method_disabled("hpcLoadOutputDS")
}

#' Load a Job Output from Trusted Server Code
#'
#' Server-side function for domain packages, intentionally absent from the
#' package's DataSHIELD \code{AssignMethods}. Callers invoke it R-internally
#' after domain authorization. They must possess the job capability through a
#' server-session symbol or B64 bearer. A
#' mandatory non-empty \code{required_label} adds a secondary domain check;
#' it is not treated as authentication. The job must be in a terminal
#' \code{FINISHED} or \code{PUBLISHED} state, and output cardinality must be
#' established at or above the \code{nfilter.subset} disclosure floor.
#'
#' When \code{as_descriptor = TRUE} and the output is a Parquet file,
#' returns a \code{FlowerDatasetDescriptor} instead of loading the data
#' into memory. This enables zero-copy column projection downstream. The
#' descriptor contains a node-local absolute path and is strictly a trusted
#' server-to-server value; callers must not return it through DataSHIELD.
#'
#' @param job_id_or_symbol Character; submission symbol or B64 bearer.
#' @param output_name Character; name of the output to load.
#' @param as_descriptor Logical; if TRUE and output is Parquet, return a
#'   FlowerDatasetDescriptor instead of loading data into memory.
#' @param required_label Mandatory exact label identifying the caller
#'   domain (typically the calling server-side package's name), matched
#'   against the job's label as a secondary domain check.
#' @return The loaded object.
#' @export
hpcLoadOutputInternal <- function(job_id_or_symbol, output_name,
                                  as_descriptor = FALSE,
                                  required_label = NULL) {
  .dshpc_require_trusted_server_caller()
  required_label <- .dshpc_require_label_value(required_label,
    "dsHPC load operation requires a domain label (required_label). The caller must identify the domain it belongs to (typically the calling server-side package's name). This is a hard requirement; there is no opt-out.")
  .dshpc_require_trusted_server_caller(required_label)
  output_name <- .validate_identifier(output_name, "output_name")
  db <- .db_connect()
  on.exit(.db_close(db))

  access <- .require_job_access(db, job_id_or_symbol)
  job_id <- access$job_id
  job <- access$job
  if (!job$state %in% c("FINISHED", "PUBLISHED"))
    stop("Job not finished (state: ", job$state, ").", call. = FALSE)

  # Ownership check: verify the job belongs to the caller domain.
  job_label <- job$label %||% ""
  if (!identical(job_label, required_label))
    stop("Job '", job_id, "' does not belong to '", required_label,
         "'. Access denied.", call. = FALSE)

  out <- DBI::dbGetQuery(db,
    "SELECT path_or_ref, kind FROM outputs WHERE job_id = ? AND name = ?
     ORDER BY id DESC LIMIT 1",
    params = list(job_id, output_name))
  if (nrow(out) == 0)
    stop("Output '", output_name, "' not found for job ", job_id, ".", call. = FALSE)

  path <- out$path_or_ref[1]
  path <- .dshpc_validate_job_artifact_path(path, job_id,
    check_tree = TRUE)

  is_rds <- grepl("\\.rds$", path, ignore.case = TRUE)
  is_json <- grepl("\\.json$", path, ignore.case = TRUE)
  object_loaded <- is_rds || is_json
  obj <- if (is_rds) {
    tryCatch(suppressWarnings(readRDS(path)), error = function(e)
      stop("Output object could not be read.", call. = FALSE))
  } else if (is_json) {
    tryCatch(jsonlite::fromJSON(
      paste(readLines(path, warn = FALSE), collapse = "\n"),
      simplifyVector = TRUE), error = function(e)
        stop("Output object could not be read.", call. = FALSE))
  } else {
    NULL
  }

  # Disclosure control: objects loaded into the session must have a cardinality
  # that can be established before they are assigned.
  n_rows <- if (object_loaded) .output_object_cardinality(obj)
            else .count_output_rows(path)
  nfilter <- .dshpc_disclosure_settings()$nfilter_subset
  if (is.na(n_rows))
    stop("Output cardinality cannot be established safely.", call. = FALSE)
  if (!is.na(n_rows) && n_rows < nfilter)
    stop("Output cardinality is below the configured disclosure minimum.",
         call. = FALSE)

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
  obj <- if (object_loaded) {
    obj
  } else if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
    as.data.frame(arrow::read_parquet(path))
  } else if (grepl("\\.csv$", path, ignore.case = TRUE)) {
    utils::read.csv(path, stringsAsFactors = FALSE)
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
    # Counting physical lines is not a valid row count for CSV because quoted
    # fields may contain newlines. Parse the table so the disclosure check uses
    # its actual number of records.
    return(nrow(utils::read.csv(path, stringsAsFactors = FALSE)))
  }
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
    return(nrow(arrow::read_parquet(path, as_data_frame = FALSE)))
  }
  # Non-tabular: can't count rows
  NA_integer_
}

#' Establish the row cardinality of a deserialized output
#' @return Integer cardinality, or NA when the object has no unambiguous rows.
#' @keywords internal
.output_object_cardinality <- function(obj) {
  if (is.data.frame(obj)) return(nrow(obj))
  if (is.matrix(obj)) return(nrow(obj))
  # Higher-dimensional arrays may be images/volumes rather than records.
  # Treating their first axis as patients would let a single image clear the
  # subset threshold merely because it has enough pixels or voxels.
  if (is.array(obj)) return(NA_integer_)
  if (is.atomic(obj)) return(length(obj))
  # A list/map can represent rows, columns, metadata, or nested records. Its
  # length is not a defensible row count, so fail closed.
  NA_integer_
}

# =============================================================================
# CAPABILITY-PROTECTED AGGREGATE methods
# =============================================================================

#' Export a Portable Job Reference
#'
#' Explicitly return the transferable bearer for one capability-authorized job.
#' Status and result calls intentionally omit this secret so routine monitoring
#' objects and notebook output do not retain it unnecessarily.
#'
#' @param job_id_or_symbol Submission symbol resolving to the server-side
#'   handle, or an existing B64 bearer.
#' @return A scalar B64 bearer for the authorized job.
#' @export
hpcJobReferenceDS <- function(job_id_or_symbol) {
  .dshpc_require_literal_or_symbol(substitute(job_id_or_symbol),
    "job_id_or_symbol")
  db <- .db_connect()
  on.exit(.db_close(db))
  .require_job_access(db, job_id_or_symbol)$bearer
}

#' Get Job Status
#'
#' Return disclosure-safe state for one job.
#'
#' @param job_id_or_symbol Submission symbol resolving to the server-side
#'   handle, or a B64 bearer returned by \code{\link{hpcJobReferenceDS}()}.
#' @return Named list with coarse state, completion flag, and a sanitized error
#'   string. The job reference, step progress, retry counters, labels, and
#'   timestamps remain omitted because they can form operational or credential
#'   disclosure channels.
#' @export
hpcStatusDS <- function(job_id_or_symbol) {
  .dshpc_require_literal_or_symbol(substitute(job_id_or_symbol),
    "job_id_or_symbol")
  db <- .db_connect()
  on.exit(.db_close(db))
  access <- .require_job_access(db, job_id_or_symbol)
  job <- access$job

  safe_error <- .safe_job_error(job$error_message)

  list(
    state = job$state,
    is_done = job$state %in% c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED"),
    error = safe_error
  )
}

#' Get Job Result
#'
#' Return the disclosure-safe result object for a completed job. Raw artifact
#' outputs are never returned directly through this method. Marked-safe values
#' must also pass the configured disclosure-cardinality floor; ambiguous
#' structures fail closed.
#'
#' @param job_id_or_symbol Submission symbol resolving to the server-side
#'   handle, or a B64 bearer returned by \code{\link{hpcJobReferenceDS}()}.
#' @return A named list. If the job is not complete, the list contains
#'   `ready = FALSE`; otherwise it contains the safe result metadata. It never
#'   contains the job identifier or bearer.
#' @export
hpcResultDS <- function(job_id_or_symbol) {
  .dshpc_require_literal_or_symbol(substitute(job_id_or_symbol),
    "job_id_or_symbol")
  db <- .db_connect()
  on.exit(.db_close(db))
  access <- .require_job_access(db, job_id_or_symbol)
  job_id <- access$job_id
  job <- access$job

  if (!job$state %in% c("FINISHED", "PUBLISHED")) {
    safe_err <- .safe_job_error(job$error_message)
    return(list(state = job$state, ready = FALSE, error = safe_err))
  }

  # Rebuild from the output registry so a stale result.rds cannot bypass the
  # current safe_for_client/kind policy. Suppress and replace every filesystem
  # or deserialization condition at this public boundary so paths cannot cross
  # the DataSHIELD interface.
  tryCatch(
    suppressWarnings(.build_job_result(db, job_id)),
    error = function(e) stop("Job result is unavailable.", call. = FALSE)
  )
}

#' Get Job Logs
#'
#' Authorize access to a job without returning runner stdout/stderr content.
#'
#' @param job_id_or_symbol Submission symbol resolving to the server-side
#'   handle, or a B64 bearer returned by \code{\link{hpcJobReferenceDS}()}.
#' @param last_n Maximum number of log lines to return. Values above 200 are
#'   capped server-side.
#' @return Empty character vector. Runner logs remain server/admin-only.
#' @export
hpcLogsDS <- function(job_id_or_symbol, last_n = 50L) {
  .dshpc_require_literal_or_symbol(substitute(job_id_or_symbol),
    "job_id_or_symbol")
  .dshpc_require_literal_or_symbol(substitute(last_n), "last_n")
  db <- .db_connect()
  on.exit(.db_close(db))
  .require_job_access(db, job_id_or_symbol)

  # Runner stdout/stderr can contain record-level values. There is no general
  # sanitizer that can prove those lines disclosure-safe for an analyst.
  .sanitize_job_logs(character(0), last_n %||% 50L)
}

#' Disabled Legacy DataSHIELD Job Enumeration
#'
#' This compatibility symbol always reports that the method was retired,
#' including under a persisted allowlist from an older dsHPC deployment.
#'
#' @param label Ignored.
#' @param scope Ignored.
#' @param mode Ignored.
#' @return This function never returns successfully.
#' @export
hpcListDS <- function(label = NULL, scope = NULL, mode = "mine+global") {
  .legacy_ds_method_disabled("hpcListDS")
}

#' List Explicitly Global Jobs from Trusted Server Code
#'
#' Returns operational rows for explicitly global jobs to trusted server-side
#' packages. Client-provided ownership scopes cannot be authenticated by Rock
#' and are ignored.
#'
#' @param label Character or NULL; filter by label.
#' @param scope Retained for client compatibility; ignored.
#' @param mode Retained for client compatibility; global scope is enforced.
#' @export
hpcListInternal <- function(label = NULL, scope = NULL, mode = "mine+global") {
  .dshpc_require_trusted_server_caller()
  .dshpc_require_trusted_server_caller(label)
  mode <- match.arg(mode, c("mine", "mine+global", "global"))

  db <- .db_connect()
  on.exit(.db_close(db))

  # The server-to-server listing is intentionally limited to explicitly global
  # jobs; the retained scope and mode arguments do not widen that selection.
  jobs <- .store_list_jobs(db, label = label, scope = "global")
  if (nrow(jobs) == 0)
    return(data.frame(job_id = character(0), state = character(0),
      name = character(0), label = character(0), submitted_at = character(0),
      progress = character(0), stringsAsFactors = FALSE))
  jobs$progress <- paste0(jobs$step_index, "/", jobs$total_steps)
  # Safe fields only -- no tags, owner_id, visibility (could be disclosive)
  jobs[, c("job_id", "state", "name", "label", "submitted_at", "progress"),
    drop = FALSE]
}

#' List Available Outputs for a Job
#'
#' Return output names and metadata for a job without loading the output values.
#'
#' @param job_id_or_symbol Submission symbol resolving to the server-side
#'   handle, or a B64 bearer returned by \code{\link{hpcJobReferenceDS}()}.
#' @return Data frame with output name, kind, disclosure flag, and a
#'   compatibility `size_bytes` column containing only `NA`.
#' @export
hpcOutputsDS <- function(job_id_or_symbol) {
  .dshpc_require_literal_or_symbol(substitute(job_id_or_symbol),
    "job_id_or_symbol")
  db <- .db_connect()
  on.exit(.db_close(db))
  access <- .require_job_access(db, job_id_or_symbol)
  outputs <- DBI::dbGetQuery(db,
    "SELECT name, kind, safe_for_client FROM outputs
     WHERE job_id = ?
       AND safe_for_client = 1
       AND kind IN ('summary', 'aggregate_result', 'job_metadata')
     ORDER BY id",
    params = list(access$job_id))
  # Keep the existing client schema without exposing exact artifact sizes or
  # even the names of outputs that were not explicitly approved for clients.
  outputs$size_bytes <- rep(NA_real_, nrow(outputs))
  outputs
}

#' Get Server Job Capabilities
#' @export
hpcCapabilitiesDS <- function() {
  # Operational topology, paths, load, worker state and runner configuration
  # are administrator data. Domain packages expose their own safe capability
  # names; the generic runtime only confirms its public contract here.
  list(
    dshpc_version = as.character(utils::packageVersion("dsHPC")),
    status = "available",
    submission = "domain_methods_only",
    job_access = "capability",
    admin_enabled = .admin_is_configured()
  )
}

#' Disabled Legacy DataSHIELD Scheduler Status
#'
#' This compatibility symbol always reports that the method was retired,
#' including when it remains in a persisted DataSHIELD allowlist.
#'
#' @return This function never returns successfully.
#' @export
hpcSchedulerStatusDS <- function() {
  .legacy_ds_method_disabled("hpcSchedulerStatusDS")
}

#' Get Scheduler Status from Trusted Server Code
#'
#' Returns raw operational scheduler state for trusted server-side consumers.
#' This function is not registered as a DataSHIELD aggregate method.
#'
#' @return Scheduler status list.
#' @export
hpcSchedulerStatusInternal <- function() {
  .dshpc_require_trusted_server_caller()
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
  .dshpc_require_literal_or_symbol(substitute(admin_key), "admin_key")
  .dshpc_require_literal_or_symbol(substitute(label), "label")
  .verify_admin_key(admin_key)
  db <- .db_connect()
  on.exit(.db_close(db))
  jobs <- .store_list_jobs(db, label = label)
  if (nrow(jobs) == 0)
    return(data.frame(job_id = character(0), state = character(0),
      name = character(0), label = character(0), submitted_at = character(0),
      progress = character(0), stringsAsFactors = FALSE))
  jobs$progress <- paste0(jobs$step_index, "/", jobs$total_steps)
  # Safe fields only -- no tags, owner_id, visibility (could be disclosive)
  jobs[, c("job_id", "state", "name", "label", "submitted_at", "progress"),
    drop = FALSE]
}

#' Cancel Any Job (admin only)
#'
#' Disabled by default. Enable by setting dshpc.admin_key or DSHPC_ADMIN_KEY.
#'
#' @param job_id Character; job ID.
#' @param admin_key Character; the admin key.
#' @export
hpcAdminCancelDS <- function(job_id, admin_key = NULL) {
  .dshpc_require_literal_or_symbol(substitute(job_id), "job_id")
  .dshpc_require_literal_or_symbol(substitute(admin_key), "admin_key")
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
