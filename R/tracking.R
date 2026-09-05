# Module: disclosure-safe shared tracking and server-side output reuse.

.TRACKING_REUSE_CLASSES <- c("internal_only", "server_reusable", "client_safe")
.TRACKING_PUBLIC_CLASSES <- c("server_reusable", "client_safe")
.TRACKING_KINDS <- c("analysis", "imaging")
.TRACKING_FORBIDDEN_REUSE_KINDS <- c(
  "credential", "credentials", "secret", "secrets", "log", "logs",
  "stdout", "stderr", "path", "filesystem_path"
)

#' @keywords internal
.dshpc_queue_visibility <- function() {
  value <- tolower(trimws(as.character(
    .dshpc_option("queue_visibility", "shared"))[1]))
  if (!value %in% c("shared", "scoped")) {
    stop("dshpc.queue_visibility must be 'shared' or 'scoped'.",
      call. = FALSE)
  }
  value
}

#' @keywords internal
.generate_tracking_id <- function() {
  paste0("trk_", uuid::UUIDgenerate())
}

#' @keywords internal
.tracking_validate_id <- function(tracking_id) {
  if (!is.character(tracking_id) || length(tracking_id) != 1L ||
      is.na(tracking_id) ||
      !grepl("^trk_[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
        tracking_id)) {
    stop("Tracked job not found.", call. = FALSE)
  }
  tracking_id
}

#' @keywords internal
.tracking_validate_name <- function(name) {
  name <- .validate_identifier(name, "output_name")
  if (nchar(name, type = "bytes") > 128L) {
    stop("output_name is too long.", call. = FALSE)
  }
  name
}

#' @keywords internal
.tracking_validate_kind <- function(kind) {
  if (!is.character(kind) || length(kind) != 1L || is.na(kind) ||
      !kind %in% .TRACKING_KINDS) {
    stop("Tracking kind is invalid.", call. = FALSE)
  }
  kind
}

#' @keywords internal
.tracking_provider <- function(api_frame = sys.parent()) {
  # Resolve only frames outside the protected API. Calling the generic guard
  # from this helper would accidentally count that API's own dsHPC namespace
  # frame as a trusted caller.
  .dshpc_trusted_server_caller(registration_frame = api_frame)
}

#' @keywords internal
.tracking_hash_reuse_key <- function(provider, reuse_key) {
  if (is.null(reuse_key)) return(NULL)
  if (!is.character(reuse_key) || length(reuse_key) != 1L ||
      is.na(reuse_key) || !nzchar(reuse_key) ||
      nchar(reuse_key, type = "bytes") > 4096L) {
    stop("reuse_key must be one non-empty bounded string.", call. = FALSE)
  }
  digest::digest(paste(provider, reuse_key, sep = "\n"), algo = "sha256",
    serialize = FALSE)
}

#' Run one tracking mutation under SQLite's writer lock
#' @keywords internal
.tracking_write <- function(db, fn) {
  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    value <- fn()
    DBI::dbExecute(db, "COMMIT")
    value
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
}

#' @keywords internal
.tracking_get_root <- function(db, tracking_id, shared_only = FALSE) {
  tracking_id <- .tracking_validate_id(tracking_id)
  sql <- "SELECT * FROM tracking_roots WHERE tracking_id = ?"
  params <- list(tracking_id)
  if (isTRUE(shared_only)) sql <- paste(sql, "AND visibility = 'shared'")
  row <- DBI::dbGetQuery(db, sql, params = params)
  if (nrow(row) != 1L) stop("Tracked job not found.", call. = FALSE)
  as.list(row[1, , drop = FALSE])
}

#' @keywords internal
.tracking_assert_provider <- function(root, provider) {
  if (!identical(as.character(root$provider), as.character(provider))) {
    stop("Tracked workflow belongs to another server package.", call. = FALSE)
  }
  invisible(TRUE)
}

#' @keywords internal
.tracking_create <- function(db, provider, reuse_key = NULL,
                             implicit = FALSE, kind = "analysis") {
  visibility <- .dshpc_queue_visibility()
  kind <- .tracking_validate_kind(kind)
  reuse_hash <- .tracking_hash_reuse_key(provider, reuse_key)
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    if (!is.null(reuse_hash)) {
      reusable <- if (isTRUE(implicit)) {
        "implicit = 1 AND lifecycle = 'CREATING'"
      } else {
        "implicit = 0 AND (lifecycle = 'OPEN' OR (success = 1 AND EXISTS
           (SELECT 1 FROM tracking_outputs t
             WHERE t.tracking_id = tracking_roots.tracking_id)))"
      }
      existing <- DBI::dbGetQuery(db, paste0(
        "SELECT tracking_id FROM tracking_roots
         WHERE provider = ? AND reuse_hash = ? AND visibility = ?
           AND kind = ? AND ",
        reusable,
        " ORDER BY created_at DESC, tracking_id DESC LIMIT 1"),
        params = list(provider, reuse_hash, visibility, kind))
      if (nrow(existing) == 1L) {
        DBI::dbExecute(db, "COMMIT")
        status <- .tracking_status(db, existing$tracking_id[1])
        return(c(status, list(reused = TRUE)))
      }
    }

    tracking_id <- .generate_tracking_id()
    DBI::dbExecute(db,
      "INSERT INTO tracking_roots
       (tracking_id, provider, reuse_hash, visibility, kind, lifecycle,
          success, implicit, execution_mode, created_at)
       VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
      params = list(tracking_id, provider, reuse_hash %||% NA_character_,
        visibility, kind, if (isTRUE(implicit)) "CREATING" else "OPEN",
        as.integer(isTRUE(implicit)),
        if (isTRUE(implicit)) "primary" else NA_character_, now))
    DBI::dbExecute(db, "COMMIT")
    list(tracking_id = tracking_id, state = "queued", is_done = FALSE,
      kind = kind, reused = FALSE)
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
}

#' Persist the terminal state of an implicit execution root
#' @keywords internal
.tracking_reconcile_implicit <- function(db, tracking_id = NULL) {
  where_id <- ""
  params <- list(format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
  if (!is.null(tracking_id)) {
    tracking_id <- .tracking_validate_id(tracking_id)
    where_id <- "AND tracking_id = ?"
    params <- c(params, list(tracking_id))
  }
  changed <- DBI::dbExecute(db, paste(
    "UPDATE tracking_roots
        SET lifecycle = 'SEALED',
            success = CASE WHEN EXISTS(SELECT 1
              FROM tracking_jobs tj JOIN jobs j ON j.job_id = tj.job_id
              WHERE tj.tracking_id = tracking_roots.tracking_id
                AND tj.role = 'primary'
                AND j.state IN ('FAILED','CANCELLED')) THEN 0 ELSE 1 END,
            finished_at = ?
      WHERE lifecycle = 'OPEN' AND implicit = 1", where_id,
    "AND EXISTS(SELECT 1 FROM tracking_jobs tj JOIN jobs j
          ON j.job_id = tj.job_id
          WHERE tj.tracking_id = tracking_roots.tracking_id
            AND tj.role = 'primary'
            AND j.state IN ('FINISHED','PUBLISHED','FAILED','CANCELLED'))
        AND NOT EXISTS(SELECT 1 FROM tracking_jobs tj JOIN jobs j
          ON j.job_id = tj.job_id
          WHERE tj.tracking_id = tracking_roots.tracking_id
            AND tj.role = 'primary'
            AND j.state IN ('PENDING','RUNNING','CLONING'))"),
    params = params)
  invisible(changed > 0L)
}

#' Reconcile a completed explicit primary into a sealed logical result
#' @keywords internal
.tracking_reconcile_primary <- function(db, tracking_id = NULL) {
  where_id <- ""
  params <- list(format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
  if (!is.null(tracking_id)) {
    tracking_id <- .tracking_validate_id(tracking_id)
    where_id <- "AND tracking_id = ?"
    params <- c(params, list(tracking_id))
  }
  changed <- DBI::dbExecute(db, paste(
    "UPDATE tracking_roots
        SET lifecycle = 'SEALED',
            success = CASE WHEN EXISTS(SELECT 1 FROM jobs j
              WHERE j.job_id = tracking_roots.finalizing_job_id
                AND j.state IN ('FAILED','CANCELLED')) THEN 0 ELSE 1 END,
            finished_at = ?
      WHERE lifecycle = 'OPEN' AND implicit = 0
        AND execution_mode = 'primary' AND finish_requested = 1
        AND finalizing_job_id IS NOT NULL", where_id,
    "AND EXISTS(SELECT 1 FROM jobs j
          WHERE j.job_id = tracking_roots.finalizing_job_id
            AND j.state IN ('FINISHED','PUBLISHED','FAILED','CANCELLED'))
        AND NOT EXISTS(SELECT 1 FROM tracking_jobs tj JOIN jobs j
          ON j.job_id = tj.job_id
          WHERE tj.tracking_id = tracking_roots.tracking_id
            AND j.state IN ('PENDING','RUNNING','CLONING'))
        AND (EXISTS(SELECT 1 FROM jobs j
              WHERE j.job_id = tracking_roots.finalizing_job_id
                AND j.state IN ('FAILED','CANCELLED'))
          OR EXISTS(SELECT 1 FROM tracking_outputs out
              WHERE out.tracking_id = tracking_roots.tracking_id
                AND out.reuse_class IN ('server_reusable','client_safe')))"),
    params = params)
  invisible(changed > 0L)
}

#' Durably mark an existing explicit primary as the final workflow attempt
#' @keywords internal
.tracking_request_finalize <- function(db, tracking_id, job_id) {
  tracking_id <- .tracking_validate_id(tracking_id)
  job_id <- .validate_identifier(job_id, "job_id")
  updated <- DBI::dbExecute(db,
    "UPDATE tracking_roots
        SET finish_requested = 1, finalizing_job_id = ?
      WHERE tracking_id = ? AND lifecycle = 'OPEN' AND implicit = 0
        AND execution_mode = 'primary'
        AND EXISTS(SELECT 1 FROM tracking_jobs tj
          WHERE tj.tracking_id = tracking_roots.tracking_id
            AND tj.job_id = ? AND tj.role = 'primary')
        AND (finish_requested = 0 OR finalizing_job_id = ?)",
    params = list(job_id, tracking_id, job_id, job_id))
  if (!identical(as.integer(updated), 1L)) {
    stop("Tracked workflow cannot change its finalizing execution.",
      call. = FALSE)
  }
  invisible(TRUE)
}

#' @keywords internal
.tracking_status <- function(db, tracking_id, shared_only = FALSE) {
  root <- .tracking_get_root(db, tracking_id, shared_only = shared_only)
  if (identical(root$lifecycle, "OPEN")) {
    if (isTRUE(as.logical(root$implicit))) {
      .tracking_reconcile_implicit(db, tracking_id)
    } else {
      .tracking_reconcile_primary(db, tracking_id)
    }
    # Another writer may have sealed the root between the initial read and
    # reconciliation, so always refresh instead of trusting this connection's
    # UPDATE row count.
    root <- .tracking_get_root(db, tracking_id, shared_only = shared_only)
  }
  jobs <- DBI::dbGetQuery(db,
    "SELECT j.state, tj.role FROM tracking_jobs tj
       JOIN jobs j ON j.job_id = tj.job_id
      WHERE tj.tracking_id = ?",
    params = list(tracking_id))

  if (identical(root$lifecycle, "SEALED")) {
    state <- "terminal"
  } else if (isTRUE(as.logical(root$implicit))) {
    primary <- jobs$state[jobs$role == "primary"]
    state <- if (length(primary) == 0L ||
        all(primary %in% c("PENDING", "CLONING"))) {
      "queued"
    } else if (any(primary %in% "RUNNING")) {
      "running"
    } else if (all(primary %in%
        c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED"))) {
      "terminal"
    } else {
      "running"
    }
  } else if (nrow(jobs) == 0L || all(jobs$state == "PENDING")) {
    state <- "queued"
  } else {
    # An explicit workflow remains non-terminal until its domain seals it.
    # This prevents a drip-fed collection appearing complete between batches.
    state <- "running"
  }

  list(tracking_id = tracking_id, state = state,
    is_done = identical(state, "terminal"),
    kind = .tracking_validate_kind(as.character(root$kind)))
}

#' @keywords internal
.tracking_succeeded <- function(db, root) {
  if (identical(root$lifecycle, "SEALED") && !is.na(root$success)) {
    return(isTRUE(as.logical(root$success)))
  }
  if (!isTRUE(as.logical(root$implicit))) {
    return(identical(root$lifecycle, "SEALED") &&
      !is.na(root$success) && isTRUE(as.logical(root$success)))
  }
  states <- DBI::dbGetQuery(db,
    "SELECT j.state FROM tracking_jobs tj JOIN jobs j ON j.job_id = tj.job_id
      WHERE tj.tracking_id = ? AND tj.role = 'primary'",
    params = list(root$tracking_id))$state
  length(states) > 0L && all(states %in% c("FINISHED", "PUBLISHED"))
}

#' Create or Reuse a Logical Tracking Root
#'
#' Trusted domain packages create one root for an entire logical workflow,
#' before submitting any private execution children. A bounded reuse key is
#' hashed before storage and is never analyst-visible.
#'
#' @param reuse_key Optional deterministic domain key for whole-workflow reuse.
#' @param kind Neutral closed-vocabulary root kind. Domain packages may use
#'   `"imaging"`; the generic default is `"analysis"`.
#' @return Tracking handle with id, coarse state, and reuse marker.
#' @export
hpcTrackingCreateInternal <- function(reuse_key = NULL, kind = "analysis") {
  provider <- .tracking_provider()
  db <- .db_connect()
  on.exit(.db_close(db))
  .tracking_create(db, provider, reuse_key = reuse_key, implicit = FALSE,
    kind = kind)
}

#' Seal a Logical Tracking Root
#'
#' Explicit roots remain open across temporary gaps in drip-fed execution.
#' Their owning domain package seals the root once no further jobs will be
#' attached.
#'
#' @param tracking_id Tracking root id.
#' @param success Whether the logical workflow completed successfully.
#' @return Invisibly, the final coarse tracking status.
#' @export
hpcTrackingFinishInternal <- function(tracking_id, success = TRUE) {
  provider <- .tracking_provider()
  tracking_id <- .tracking_validate_id(tracking_id)
  if (!is.logical(success) || length(success) != 1L || is.na(success)) {
    stop("success must be TRUE or FALSE.", call. = FALSE)
  }
  db <- .db_connect()
  on.exit(.db_close(db))
  value <- .tracking_write(db, function() {
    root <- .tracking_get_root(db, tracking_id)
    .tracking_assert_provider(root, provider)
    if (isTRUE(as.logical(root$implicit))) {
      stop("Implicit job roots are completed by their execution.",
        call. = FALSE)
    }
    if (identical(root$lifecycle, "SEALED")) {
      if (!is.na(root$success) &&
          identical(isTRUE(as.logical(root$success)), isTRUE(success))) {
        return(.tracking_status(db, tracking_id))
      }
      stop("Tracked workflow is already complete.", call. = FALSE)
    }
    active <- DBI::dbGetQuery(db,
      "SELECT COUNT(*) AS n FROM tracking_jobs tj JOIN jobs j
          ON j.job_id = tj.job_id
        WHERE tj.tracking_id = ?
          AND j.state IN ('PENDING','RUNNING','CLONING')",
      params = list(tracking_id))$n
    if (active > 0L) {
      stop("Tracked workflow still has active execution jobs.",
        call. = FALSE)
    }
    if (isTRUE(success)) {
      published <- DBI::dbGetQuery(db,
        "SELECT COUNT(*) AS n FROM tracking_outputs WHERE tracking_id = ?
          AND reuse_class IN ('server_reusable','client_safe')",
        params = list(tracking_id))$n
      if (published < 1L) {
        stop("A successful workflow must publish a reusable output.",
          call. = FALSE)
      }
    }
    DBI::dbExecute(db,
      "UPDATE tracking_roots SET lifecycle = 'SEALED', success = ?,
         finished_at = ? WHERE tracking_id = ? AND lifecycle = 'OPEN'",
      params = list(as.integer(success),
        format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"), tracking_id))
    .tracking_status(db, tracking_id)
  })
  invisible(value)
}

#' Find the Tracking Root for an Execution Job
#'
#' @param job_id Server-side execution job id.
#' @return Tracking id, or `NULL` when the execution is untracked.
#' @export
hpcTrackingForJobInternal <- function(job_id) {
  .tracking_provider()
  job_id <- .validate_identifier(job_id, "job_id")
  db <- .db_connect()
  on.exit(.db_close(db))
  row <- DBI::dbGetQuery(db,
    "SELECT tracking_id FROM tracking_jobs WHERE job_id = ?
       ORDER BY attached_at LIMIT 1", params = list(job_id))
  if (nrow(row) == 0L) NULL else row$tracking_id[1]
}

#' Read Tracking Status from Trusted Server Code
#'
#' @param tracking_id Tracking root id.
#' @return Disclosure-safe coarse status.
#' @export
hpcTrackingStatusInternal <- function(tracking_id) {
  .tracking_provider()
  db <- .db_connect()
  on.exit(.db_close(db))
  .tracking_status(db, tracking_id)
}

#' @keywords internal
.tracking_find_execution <- function(db, spec_hash, label,
                                     allow_completed = FALSE) {
  visibility <- .dshpc_queue_visibility()
  row <- DBI::dbGetQuery(db,
    "SELECT j.job_id, tj.tracking_id
       FROM jobs j
      JOIN tracking_jobs tj ON tj.job_id = j.job_id AND tj.role = 'primary'
      JOIN tracking_roots tr ON tr.tracking_id = tj.tracking_id
      WHERE j.spec_hash = ? AND j.visibility = 'global' AND j.label = ?
        AND (j.state IN ('PENDING','RUNNING','CLONING') OR
          (? = 1 AND j.state IN ('FINISHED','PUBLISHED') AND EXISTS(
            SELECT 1 FROM outputs o WHERE o.job_id = j.job_id
              AND o.reuse_class IN ('server_reusable','client_safe'))))
        AND tr.visibility = ? AND tr.implicit = 1
      ORDER BY j.submitted_at DESC LIMIT 1",
    params = list(spec_hash, label, as.integer(isTRUE(allow_completed)),
      visibility))
  if (nrow(row) == 0L) NULL else as.list(row[1, , drop = FALSE])
}

#' @keywords internal
.tracking_cursor_encode <- function(tracking_id) {
  sub("^trk_", "cur_", tracking_id)
}

#' @keywords internal
.tracking_cursor_id <- function(cursor) {
  if (!is.character(cursor) || length(cursor) != 1L || is.na(cursor) ||
      !grepl("^cur_[0-9a-f-]{36}$", cursor)) {
    stop("Tracking cursor is invalid.", call. = FALSE)
  }
  .tracking_validate_id(sub("^cur_", "trk_", cursor))
}

#' List Shared Logical Jobs
#'
#' Return one disclosure-safe row per shared logical root. Execution children,
#' exact progress, timestamps, labels, owners, errors, and scheduler topology
#' are never returned. Pagination is deterministic and exposes no raw count.
#'
#' @param limit Page size from 1 through 500.
#' @param cursor Opaque continuation cursor returned by a previous page.
#' @return A `root_v1` page with items, continuation cursor, and has-more flag.
#' @export
hpcTrackingListDS <- function(limit = 100L, cursor = NULL) {
  .dshpc_require_literal_or_symbol(substitute(limit), "limit")
  .dshpc_require_literal_or_symbol(substitute(cursor), "cursor")
  if (!identical(.dshpc_queue_visibility(), "shared")) {
    stop("Shared job tracking is disabled.", call. = FALSE)
  }
  if (!is.numeric(limit) || length(limit) != 1L || is.na(limit) ||
      !is.finite(limit) || limit != floor(limit) || limit < 1L ||
      limit > 500L) {
    stop("limit must be one whole number between 1 and 500.", call. = FALSE)
  }
  limit <- as.integer(limit)

  db <- .db_connect()
  on.exit(.db_close(db))
  params <- list()
  where <- "visibility = 'shared' AND lifecycle <> 'CREATING'"
  if (!is.null(cursor)) {
    if (!is.character(cursor) || length(cursor) != 1L || is.na(cursor) ||
        !nzchar(cursor)) {
      stop("Tracking cursor is invalid.", call. = FALSE)
    }
    cursor_id <- .tracking_cursor_id(cursor)
    pivot <- DBI::dbGetQuery(db,
      "SELECT created_at, tracking_id FROM tracking_roots
        WHERE tracking_id = ? AND visibility = 'shared'",
      params = list(cursor_id))
    if (nrow(pivot) != 1L) stop("Tracking cursor is invalid.", call. = FALSE)
    where <- paste(where,
      "AND (created_at < ? OR (created_at = ? AND tracking_id < ?))")
    params <- list(pivot$created_at[1], pivot$created_at[1], cursor_id)
  }
  rows <- DBI::dbGetQuery(db, paste(
    "SELECT tracking_id FROM tracking_roots WHERE", where,
    "ORDER BY created_at DESC, tracking_id DESC LIMIT ?"),
    params = c(params, list(limit + 1L)))
  has_more <- nrow(rows) > limit
  if (has_more) rows <- rows[seq_len(limit), , drop = FALSE]

  if (nrow(rows) == 0L) {
    items <- data.frame(tracking_id = character(0), state = character(0),
      is_done = logical(0), kind = character(0), stringsAsFactors = FALSE)
  } else {
    statuses <- lapply(rows$tracking_id, function(id) {
      .tracking_status(db, id, shared_only = TRUE)
    })
    items <- data.frame(
      tracking_id = vapply(statuses, `[[`, character(1), "tracking_id"),
      state = vapply(statuses, `[[`, character(1), "state"),
      is_done = vapply(statuses, `[[`, logical(1), "is_done"),
      kind = vapply(statuses, `[[`, character(1), "kind"),
      stringsAsFactors = FALSE)
  }
  next_cursor <- if (has_more && nrow(items) > 0L) {
    .tracking_cursor_encode(items$tracking_id[nrow(items)])
  } else NULL
  list(items = items, next_cursor = next_cursor, has_more = has_more,
    schema = "root_v1")
}

#' Get Shared Logical Job Status
#'
#' @param tracking_id Public tracking root id.
#' @return Tracking id, coarse state, completion flag, and neutral kind.
#' @export
hpcTrackingStatusDS <- function(tracking_id) {
  .dshpc_require_literal_or_symbol(substitute(tracking_id), "tracking_id")
  if (!identical(.dshpc_queue_visibility(), "shared")) {
    stop("Shared job tracking is disabled.", call. = FALSE)
  }
  db <- .db_connect()
  on.exit(.db_close(db))
  .tracking_status(db, tracking_id, shared_only = TRUE)
}

#' @keywords internal
.tracking_output_entries <- function(db, tracking_id) {
  root <- .tracking_get_root(db, tracking_id)
  explicit <- DBI::dbGetQuery(db,
    "SELECT t.name, t.kind, t.reuse_class, t.source_job_id,
            t.source_output_name, t.provider, t.provider_ref,
            o.path_or_ref, o.safe_for_client
       FROM tracking_outputs t
       LEFT JOIN outputs o ON o.job_id = t.source_job_id
                          AND o.name = t.source_output_name
                          AND o.id = (SELECT MAX(o2.id) FROM outputs o2
                            WHERE o2.job_id = t.source_job_id
                              AND o2.name = t.source_output_name)
      WHERE t.tracking_id = ?
      ORDER BY t.name", params = list(tracking_id))

  direct <- data.frame()
  if (isTRUE(as.logical(root$implicit))) {
    direct <- DBI::dbGetQuery(db,
      "SELECT o.name, o.kind, o.reuse_class, o.job_id AS source_job_id,
              o.name AS source_output_name, tr.provider, NULL AS provider_ref,
              o.path_or_ref, o.safe_for_client
         FROM tracking_jobs tj
         JOIN tracking_roots tr ON tr.tracking_id = tj.tracking_id
         JOIN outputs o ON o.job_id = tj.job_id
        WHERE tj.tracking_id = ? AND tj.role = 'primary'
          AND o.reuse_class IN ('server_reusable','client_safe')
          AND o.id = (SELECT MAX(o2.id) FROM outputs o2
            WHERE o2.job_id = o.job_id AND o2.name = o.name)
        ORDER BY o.id", params = list(tracking_id))
  }
  if (nrow(explicit) > 0L && nrow(direct) > 0L) {
    direct <- direct[!direct$name %in% explicit$name, , drop = FALSE]
  }
  rows <- if (nrow(explicit) == 0L) direct else if (nrow(direct) == 0L) {
    explicit
  } else rbind(direct, explicit)
  if (nrow(rows) == 0L) return(rows)
  rows[rows$reuse_class %in% .TRACKING_PUBLIC_CLASSES, , drop = FALSE]
}

#' Project internal output records onto the shared disclosure-safe schema
#' @keywords internal
.tracking_public_output_entries <- function(db, tracking_id) {
  rows <- .tracking_output_entries(db, tracking_id)
  if (nrow(rows) == 0L) {
    rows$public_name <- character(0)
    rows$public_kind <- character(0)
    return(rows)
  }

  # The shared value boundary is deliberately narrower than the capability
  # API: only dsHPC's closed count-summary schema may cross. Other reusable
  # values remain available as opaque server objects.
  rows <- rows[rows$reuse_class == "server_reusable" |
    (rows$reuse_class == "client_safe" & rows$kind == "summary"), ,
    drop = FALSE]
  if (nrow(rows) == 0L) {
    rows$public_name <- character(0)
    rows$public_kind <- character(0)
    return(rows)
  }

  # A variable-length output list can itself encode cohort cardinality (for
  # example, one output per patient). The generic shared catalogue therefore
  # exposes at most one composite server object and one closed summary. Domain
  # packages can still resolve every explicitly known internal output.
  server_index <- which(rows$reuse_class == "server_reusable")
  summary_index <- which(rows$reuse_class == "client_safe" &
    rows$kind == "summary")
  selected <- c(if (length(server_index) > 0L) server_index[[1L]],
    if (length(summary_index) > 0L) summary_index[[1L]])
  rows <- rows[selected, , drop = FALSE]

  # Neither runner-selected names nor trusted-domain aliases cross the shared
  # metadata boundary. Fixed class-specific aliases remain stable even when a
  # root publishes only one of the two permitted output classes.
  rows$public_name <- ifelse(rows$reuse_class == "client_safe",
    "output_002", "output_001")
  rows$public_kind <- ifelse(rows$reuse_class == "client_safe",
    "summary", "server_object")
  rows
}

#' Resolve one shared output alias without exposing its internal name
#' @keywords internal
.tracking_public_entry <- function(db, tracking_id, output_name) {
  rows <- .tracking_public_output_entries(db, tracking_id)
  if (nrow(rows) == 0L) stop("Reusable output not found.", call. = FALSE)
  row <- rows[rows$public_name == output_name, , drop = FALSE]
  if (nrow(row) != 1L) stop("Reusable output not found.", call. = FALSE)
  as.list(row[1, , drop = FALSE])
}

#' @keywords internal
.tracking_entry <- function(db, tracking_id, output_name) {
  rows <- .tracking_output_entries(db, tracking_id)
  if (nrow(rows) == 0L) stop("Reusable output not found.", call. = FALSE)
  row <- rows[rows$name == output_name, , drop = FALSE]
  if (nrow(row) != 1L) stop("Reusable output not found.", call. = FALSE)
  as.list(row[1, , drop = FALSE])
}

#' Publish a Job Output on a Logical Root
#'
#' Trusted domain code explicitly chooses which child output becomes a named
#' root output. Child outputs are otherwise undiscoverable.
#'
#' @param tracking_id Logical tracking root id.
#' @param job_id_or_symbol Linked execution job id or server handle.
#' @param output_name Existing job output name.
#' @param public_name Neutral logical name exposed by tracking APIs.
#' @param classification `server_reusable` or `client_safe`.
#' @return Invisibly, the public output name.
#' @export
hpcTrackingPublishOutputInternal <- function(tracking_id, job_id_or_symbol,
                                              output_name,
                                              public_name = output_name,
                                              classification = c(
                                                "server_reusable",
                                                "client_safe")) {
  provider <- .tracking_provider()
  tracking_id <- .tracking_validate_id(tracking_id)
  output_name <- .tracking_validate_name(output_name)
  public_name <- .tracking_validate_name(public_name)
  classification <- match.arg(classification)
  job_id <- .resolve_job_access(job_id_or_symbol)$job_id
  job_id <- .validate_identifier(job_id, "job_id")

  db <- .db_connect()
  on.exit(.db_close(db))
  .tracking_write(db, function() {
    root <- .tracking_get_root(db, tracking_id)
    .tracking_assert_provider(root, provider)
    linked <- DBI::dbGetQuery(db,
      "SELECT 1 AS linked FROM tracking_jobs
        WHERE tracking_id = ? AND job_id = ?",
      params = list(tracking_id, job_id))
    if (nrow(linked) != 1L) {
      stop("Job is not part of this workflow.", call. = FALSE)
    }
    job <- .store_get_job(db, job_id)
    if (is.null(job) || !job$state %in% c("FINISHED", "PUBLISHED")) {
      stop("Job output is not ready.", call. = FALSE)
    }
    output <- DBI::dbGetQuery(db,
      "SELECT name, kind, safe_for_client FROM outputs
        WHERE job_id = ? AND name = ? ORDER BY id DESC LIMIT 1",
      params = list(job_id, output_name))
    if (nrow(output) != 1L) stop("Job output not found.", call. = FALSE)
    kind <- output$kind[1]
    if (tolower(kind) %in% .TRACKING_FORBIDDEN_REUSE_KINDS) {
      stop("This output kind cannot be reused.", call. = FALSE)
    }
    if (identical(classification, "client_safe") &&
        (!isTRUE(as.logical(output$safe_for_client[1])) ||
         !kind %in% .CLIENT_SAFE_KINDS)) {
      stop("Output is not approved for client disclosure.", call. = FALSE)
    }

    existing <- DBI::dbGetQuery(db,
      "SELECT source_job_id, source_output_name, provider, provider_ref,
              kind, reuse_class
         FROM tracking_outputs WHERE tracking_id = ? AND name = ?",
      params = list(tracking_id, public_name))
    if (nrow(existing) == 1L) {
      same <- identical(as.character(existing$source_job_id[1]), job_id) &&
        identical(as.character(existing$source_output_name[1]), output_name) &&
        identical(as.character(existing$provider[1]), provider) &&
        is.na(existing$provider_ref[1]) &&
        identical(as.character(existing$kind[1]), as.character(kind)) &&
        identical(as.character(existing$reuse_class[1]), classification)
      if (same) return(invisible(public_name))
      stop("Tracked output name is already published.", call. = FALSE)
    }
    if (identical(root$lifecycle, "SEALED")) {
      stop("Tracked workflow is already complete.", call. = FALSE)
    }
    DBI::dbExecute(db,
      "INSERT INTO tracking_outputs
         (tracking_id, name, source_job_id, source_output_name, provider,
          provider_ref, kind, reuse_class, created_at)
       VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?)",
      params = list(tracking_id, public_name, job_id, output_name, provider,
        kind, classification,
        format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")))
    invisible(public_name)
  })
  invisible(public_name)
}

#' Publish an Opaque Domain Reference on a Logical Root
#'
#' @param tracking_id Logical tracking root id.
#' @param output_name Neutral logical output name.
#' @param reference Bounded server-only identifier understood by the provider.
#' @param classification Currently only `server_reusable`.
#' @return Invisibly, the output name.
#' @export
hpcTrackingPublishReferenceInternal <- function(
    tracking_id, output_name, reference,
    classification = "server_reusable") {
  provider <- .tracking_provider()
  tracking_id <- .tracking_validate_id(tracking_id)
  output_name <- .tracking_validate_name(output_name)
  if (!identical(classification, "server_reusable")) {
    stop("Domain references must be server_reusable.", call. = FALSE)
  }
  if (!is.character(reference) || length(reference) != 1L ||
      is.na(reference) || !nzchar(reference) ||
      nchar(reference, type = "bytes") > 512L ||
      !grepl("^[A-Za-z0-9][A-Za-z0-9_.:-]*$", reference)) {
    stop("reference must be one bounded opaque identifier.", call. = FALSE)
  }
  db <- .db_connect()
  on.exit(.db_close(db))
  .tracking_write(db, function() {
    root <- .tracking_get_root(db, tracking_id)
    .tracking_assert_provider(root, provider)
    existing <- DBI::dbGetQuery(db,
      "SELECT source_job_id, source_output_name, provider, provider_ref,
              kind, reuse_class
         FROM tracking_outputs WHERE tracking_id = ? AND name = ?",
      params = list(tracking_id, output_name))
    if (nrow(existing) == 1L) {
      same <- is.na(existing$source_job_id[1]) &&
        is.na(existing$source_output_name[1]) &&
        identical(as.character(existing$provider[1]), provider) &&
        identical(as.character(existing$provider_ref[1]), reference) &&
        identical(as.character(existing$kind[1]), "domain_reference") &&
        identical(as.character(existing$reuse_class[1]), "server_reusable")
      if (same) return(invisible(output_name))
      stop("Tracked output name is already published.", call. = FALSE)
    }
    if (identical(root$lifecycle, "SEALED")) {
      stop("Tracked workflow is already complete.", call. = FALSE)
    }
    DBI::dbExecute(db,
      "INSERT INTO tracking_outputs
         (tracking_id, name, source_job_id, source_output_name, provider,
          provider_ref, kind, reuse_class, created_at)
       VALUES (?, ?, NULL, NULL, ?, ?, 'domain_reference',
               'server_reusable', ?)",
      params = list(tracking_id, output_name, provider, reference,
        format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")))
    invisible(output_name)
  })
  invisible(output_name)
}

#' List Reusable Outputs on a Shared Logical Job
#'
#' @param tracking_id Public tracking root id.
#' @return Neutral output aliases, closed public kinds, and classifications;
#'   never runner-selected names, paths, or values.
#' @export
hpcTrackingOutputsDS <- function(tracking_id) {
  .dshpc_require_literal_or_symbol(substitute(tracking_id), "tracking_id")
  if (!identical(.dshpc_queue_visibility(), "shared")) {
    stop("Shared job tracking is disabled.", call. = FALSE)
  }
  db <- .db_connect()
  on.exit(.db_close(db))
  status <- .tracking_status(db, tracking_id, shared_only = TRUE)
  root <- .tracking_get_root(db, tracking_id, shared_only = TRUE)
  if (!status$is_done || !.tracking_succeeded(db, root)) {
    return(data.frame(name = character(0), kind = character(0),
      classification = character(0), stringsAsFactors = FALSE))
  }
  rows <- .tracking_public_output_entries(db, tracking_id)
  if (nrow(rows) == 0L) {
    return(data.frame(name = character(0), kind = character(0),
      classification = character(0), stringsAsFactors = FALSE))
  }
  data.frame(name = rows$public_name, kind = rows$public_kind,
    classification = rows$reuse_class, stringsAsFactors = FALSE)
}

#' @keywords internal
.tracking_safe_result <- function(db, tracking_id) {
  status <- .tracking_status(db, tracking_id, shared_only = TRUE)
  root <- .tracking_get_root(db, tracking_id, shared_only = TRUE)
  if (!status$is_done) {
    return(list(state = status$state, ready = FALSE, error = NA_character_))
  }
  if (!.tracking_succeeded(db, root)) {
    return(list(state = "terminal", ready = FALSE,
      error = "Job execution failed."))
  }
  rows <- .tracking_public_output_entries(db, tracking_id)
  rows <- rows[rows$reuse_class == "client_safe", , drop = FALSE]
  result <- list(ready = TRUE, summaries = list(), available_outputs = list())
  summaries <- list()
  if (nrow(rows) > 0L) {
    summaries <- lapply(seq_len(nrow(rows)), function(i) {
      row <- rows[i, ]
      if (!isTRUE(as.logical(row$safe_for_client)) ||
          !identical(as.character(row$kind), "summary") ||
          is.na(row$source_job_id) || is.na(row$path_or_ref)) {
        stop("Shared result failed disclosure validation.", call. = FALSE)
      }
      output <- list(name = row$public_name, kind = row$public_kind)
      path <- .dshpc_validate_job_artifact_path(row$path_or_ref,
        row$source_job_id, check_tree = TRUE)
      if (grepl("\\.rds$", path, ignore.case = TRUE)) {
        value <- suppressWarnings(readRDS(path))
        if (!.dshpc_client_safe_value(value, "summary")) {
          stop("Shared result failed disclosure validation.", call. = FALSE)
        }
        output$value <- value
      }
      output
    })
  }
  result$summaries <- summaries
  result$available_outputs <- lapply(seq_len(nrow(rows)), function(i) {
    list(name = rows$public_name[i], kind = rows$public_kind[i])
  })
  result
}

#' Get a Shared Disclosure-Controlled Result
#'
#' @param tracking_id Public tracking root id.
#' @return Only outputs already marked client-safe and revalidated against the
#'   closed dsHPC result schema. Server-reusable values never cross this API.
#' @export
hpcTrackingResultDS <- function(tracking_id) {
  .dshpc_require_literal_or_symbol(substitute(tracking_id), "tracking_id")
  if (!identical(.dshpc_queue_visibility(), "shared")) {
    stop("Shared job tracking is disabled.", call. = FALSE)
  }
  db <- .db_connect()
  on.exit(.db_close(db))
  tryCatch(suppressWarnings(.tracking_safe_result(db, tracking_id)),
    error = function(e) stop("Job result is unavailable.", call. = FALSE))
}

#' Assign a Shared Output Reference in the Server Session
#'
#' This assign method returns only an opaque reference. It never deserializes
#' or returns the underlying output across the client boundary.
#'
#' @param tracking_id Public tracking root id.
#' @param output_name Reusable logical output name.
#' @return An opaque `dshpc_output_reference` for server-side consumers.
#' @export
hpcTrackingAssignOutputDS <- function(tracking_id, output_name) {
  .dshpc_require_literal_or_symbol(substitute(tracking_id), "tracking_id")
  .dshpc_require_literal_or_symbol(substitute(output_name), "output_name")
  if (!identical(.dshpc_queue_visibility(), "shared")) {
    stop("Shared job tracking is disabled.", call. = FALSE)
  }
  output_name <- .tracking_validate_name(output_name)
  db <- .db_connect()
  on.exit(.db_close(db))
  status <- .tracking_status(db, tracking_id, shared_only = TRUE)
  root <- .tracking_get_root(db, tracking_id, shared_only = TRUE)
  if (!status$is_done || !.tracking_succeeded(db, root)) {
    stop("Reusable output is not ready.", call. = FALSE)
  }
  entry <- .tracking_public_entry(db, tracking_id, output_name)
  structure(list(tracking_id = tracking_id, output_name = output_name,
    kind = as.character(entry$public_kind),
    classification = as.character(entry$reuse_class)),
    class = c("dshpc_output_reference", "list"))
}

#' @keywords internal
.tracking_load_job_output <- function(entry, as_descriptor = FALSE) {
  job_id <- as.character(entry$source_job_id)
  path <- .dshpc_validate_job_artifact_path(entry$path_or_ref, job_id,
    check_tree = TRUE)
  if (isTRUE(as_descriptor) && grepl("\\.parquet$", path,
      ignore.case = TRUE)) {
    pf <- arrow::read_parquet(path, as_data_frame = FALSE)
    value <- list(
      dataset_id = paste0("dshpc.", entry$tracking_id, ".", entry$name),
      source_kind = "staged_parquet",
      metadata = list(file = path, format = "parquet", n_rows = nrow(pf),
        columns = names(pf)),
      staged_token = entry$tracking_id,
      origin = "dsHPC")
    class(value) <- "FlowerDatasetDescriptor"
  } else if (grepl("\\.rds$", path, ignore.case = TRUE)) {
    value <- readRDS(path)
  } else if (grepl("\\.json$", path, ignore.case = TRUE)) {
    value <- jsonlite::fromJSON(
      paste(readLines(path, warn = FALSE), collapse = "\n"),
      simplifyVector = TRUE)
  } else if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
    value <- as.data.frame(arrow::read_parquet(path))
  } else if (grepl("\\.csv$", path, ignore.case = TRUE)) {
    value <- utils::read.csv(path, stringsAsFactors = FALSE)
  } else {
    value <- list(type = "job_output_ref", kind = entry$kind, path = path)
  }
  attr(value, "dshpc.provenance") <- list(
    tracking_id = entry$tracking_id, output_name = entry$name,
    provider = entry$provider, classification = entry$reuse_class)
  value
}

#' Resolve a Shared Output in Trusted Server Code
#'
#' Any trusted DataSHIELD domain package may consume a shared output, including
#' outputs produced by another domain. No cardinality rule is applied because
#' the value remains on the server; downstream client results must pass their
#' own disclosure control.
#'
#' @param reference_or_tracking_id Opaque assigned reference or tracking id.
#' @param output_name Output name when the first argument is a tracking id.
#' @param as_descriptor Return a Parquet descriptor instead of materializing it.
#' @return A server-only value carrying provenance, or a provider reference.
#' @export
hpcTrackingResolveOutputInternal <- function(reference_or_tracking_id,
                                              output_name = NULL,
                                              as_descriptor = FALSE) {
  .tracking_provider()
  public_reference <- inherits(reference_or_tracking_id,
    "dshpc_output_reference")
  if (public_reference) {
    tracking_id <- reference_or_tracking_id$tracking_id
    output_name <- reference_or_tracking_id$output_name
  } else {
    tracking_id <- reference_or_tracking_id
  }
  tracking_id <- .tracking_validate_id(tracking_id)
  output_name <- .tracking_validate_name(output_name)
  if (!is.logical(as_descriptor) || length(as_descriptor) != 1L ||
      is.na(as_descriptor)) {
    stop("as_descriptor must be TRUE or FALSE.", call. = FALSE)
  }
  db <- .db_connect()
  on.exit(.db_close(db))
  status <- .tracking_status(db, tracking_id)
  root <- .tracking_get_root(db, tracking_id)
  if (!status$is_done || !.tracking_succeeded(db, root)) {
    stop("Reusable output is not ready.", call. = FALSE)
  }
  entry <- if (public_reference) {
    .tracking_public_entry(db, tracking_id, output_name)
  } else {
    .tracking_entry(db, tracking_id, output_name)
  }
  entry$tracking_id <- tracking_id
  if (!is.na(entry$source_job_id) && nzchar(entry$source_job_id)) {
    return(.tracking_load_job_output(entry,
      as_descriptor = as_descriptor))
  }
  structure(list(provider = as.character(entry$provider),
    reference = as.character(entry$provider_ref),
    classification = as.character(entry$reuse_class),
    output_name = as.character(entry$name), tracking_id = tracking_id),
    class = c("dshpc_domain_output_reference", "list"))
}
