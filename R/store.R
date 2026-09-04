# Module: Transactional Store
# CRUD on jobs/steps tables. All mutations via transactions.

#' @keywords internal
.store_create_job <- function(db, job_id, owner_id, spec, total_steps,
                               spec_hash = NULL, access_token_hash = NULL,
                               initial_state = "PENDING", clone_owner = NULL,
                               enforce_quotas = FALSE) {
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  if (!is.character(initial_state) || length(initial_state) != 1L ||
      !initial_state %in% c("PENDING", "CLONING")) {
    stop("Invalid initial job state.", call. = FALSE)
  }

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    if (isTRUE(enforce_quotas)) .check_quotas(db, owner_id)
    label <- spec$label %||% NA_character_
    filter_label <- if (is.na(label)) NULL else label
    raw_name <- spec$name %||% NA_character_
    name <- .job_name_resolve_template(db, raw_name,
      owner_id = owner_id, label = filter_label)
    if (!is.null(spec$name)) spec$name <- name
    spec_json <- as.character(jsonlite::toJSON(spec, auto_unbox = TRUE,
      null = "null"))
    tags <- if (!is.null(spec$tags))
      paste(spec$tags, collapse = ",") else NA_character_
    visibility <- spec$visibility %||% "private"
    if (!visibility %in% c("private", "global"))
      stop("visibility must be 'private' or 'global'.", call. = FALSE)
    DBI::dbExecute(db,
      "INSERT INTO jobs (job_id, owner_id, state, step_index, total_steps,
                         resource_class, name, label, tags, visibility,
                         spec_hash, access_token_hash, submitted_at, spec_json)
       VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      params = list(job_id, owner_id, initial_state, total_steps,
                     spec$resource_class %||% "default",
                     name,
                     label, tags, visibility,
                     spec_hash %||% NA_character_,
                     access_token_hash %||% NA_character_,
                     now, spec_json))
    for (i in seq_along(spec$steps)) {
      s <- spec$steps[[i]]
      input_refs <- if (!is.null(s$inputs))
        as.character(jsonlite::toJSON(s$inputs, auto_unbox = TRUE))
      else NA_character_
      DBI::dbExecute(db,
        "INSERT INTO steps (job_id, step_index, type, plane, runner, state, input_refs)
         VALUES (?, ?, ?, ?, ?, 'pending', ?)",
        params = list(job_id, i, s$type, s$plane,
                       s$runner %||% NA_character_, input_refs))
    }
    created_details <- list(total_steps = total_steps, owner = owner_id)
    if (!is.null(clone_owner)) created_details$clone_owner <- clone_owner
    .db_log_event(db, job_id, "created", created_details)
    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
}

#' @keywords internal
.store_get_job <- function(db, job_id) {
  row <- DBI::dbGetQuery(db, "SELECT * FROM jobs WHERE job_id = ?",
    params = list(job_id))
  if (nrow(row) == 0) return(NULL)
  as.list(row[1, ])
}

#' @keywords internal
.dshpc_invalid_durable_spec <- function() {
  condition <- structure(
    list(message = "Durable job specification is invalid.", call = NULL),
    class = c("dshpc_invalid_durable_spec", "error", "condition"))
  stop(condition)
}

#' @keywords internal
.store_get_spec <- function(db, job_id) {
  row <- DBI::dbGetQuery(db, "SELECT spec_json FROM jobs WHERE job_id = ?",
    params = list(job_id))
  if (nrow(row) == 0) return(NULL)
  spec <- tryCatch(
    jsonlite::fromJSON(row$spec_json[1], simplifyVector = FALSE),
    error = function(e) .dshpc_invalid_durable_spec())
  if (!is.list(spec)) .dshpc_invalid_durable_spec()
  spec
}

#' @keywords internal
.store_update_job <- function(db, job_id, ...) {
  updates <- list(...)
  if (length(updates) == 0) return(invisible(TRUE))
  set_clauses <- paste0(names(updates), " = ?")
  sql <- paste0("UPDATE jobs SET ", paste(set_clauses, collapse = ", "),
                " WHERE job_id = ?")
  DBI::dbExecute(db, sql, params = c(as.list(unname(updates)), list(job_id)))
}

#' @keywords internal
.store_update_step <- function(db, job_id, step_index, ...) {
  updates <- list(...)
  if (length(updates) == 0) return(invisible(TRUE))
  set_clauses <- paste0(names(updates), " = ?")
  sql <- paste0("UPDATE steps SET ", paste(set_clauses, collapse = ", "),
                " WHERE job_id = ? AND step_index = ?")
  DBI::dbExecute(db, sql,
    params = c(as.list(unname(updates)), list(job_id, step_index)))
}

#' @keywords internal
.store_list_jobs <- function(db, states = NULL, label = NULL,
                             owner_id = NULL, scope = NULL) {
  where_parts <- character(0)
  params <- list()

  if (!is.null(states)) {
    ph <- paste(rep("?", length(states)), collapse = ", ")
    where_parts <- c(where_parts, paste0("state IN (", ph, ")"))
    params <- c(params, as.list(states))
  }
  if (!is.null(label)) {
    where_parts <- c(where_parts, "label = ?")
    params <- c(params, list(label))
  }
  if (!is.null(scope)) {
    scope <- as.character(scope)[1]
    if (identical(scope, "mine")) {
      where_parts <- c(where_parts, "owner_id = ?")
      params <- c(params, list(owner_id %||% ""))
    } else if (identical(scope, "global")) {
      where_parts <- c(where_parts, "visibility = 'global'")
    } else if (identical(scope, "mine+global")) {
      where_parts <- c(where_parts, "(owner_id = ? OR visibility = 'global')")
      params <- c(params, list(owner_id %||% ""))
    }
  }
  sql <- paste(
    "SELECT job_id, owner_id, state, submitted_at, accepted_at, started_at,",
    "finished_at, step_index, total_steps, resource_class, priority, name, label,",
    "visibility, error_class, error_message, retry_count",
    "FROM jobs")
  if (length(where_parts) > 0)
    sql <- paste(sql, "WHERE", paste(where_parts, collapse = " AND "))
  sql <- paste(sql, "ORDER BY submitted_at DESC")
  if (length(params) > 0) DBI::dbGetQuery(db, sql, params = params)
  else DBI::dbGetQuery(db, sql)
}
