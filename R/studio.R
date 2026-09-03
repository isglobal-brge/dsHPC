# Module: dsHPC Studio observability API
# Trusted server-side metadata plus a retired legacy DataSHIELD wrapper.

#' Disabled Legacy DataSHIELD Studio Snapshot
#'
#' This compatibility symbol always reports that the method was retired,
#' including when it remains in a persisted DataSHIELD allowlist.
#'
#' @param scope Ignored.
#' @param label Ignored.
#' @param mode Ignored.
#' @return This function never returns successfully.
#' @export
hpcStudioDS <- function(scope = NULL, label = NULL, mode = "mine+global") {
  .legacy_ds_method_disabled("hpcStudioDS")
}

#' dsHPC Studio Snapshot for Trusted Server Code
#'
#' Return an operational snapshot of explicitly global jobs for one server.
#' This server-to-server API exposes control-plane metadata: job state, timings, step
#' state, DAG structure, output names/kinds, and sanitized scheduler health.
#' It never returns exact output sizes,
#' output values, artifact paths, raw logs, owner identifiers, worker PIDs, or
#' executable backend command paths.
#'
#' @param scope Retained for server API compatibility; ignored.
#' @param label Character or NULL; optional label filter.
#' @param mode Retained for server API compatibility; global scope is enforced.
#' @return Named operational snapshot for trusted server-side consumers.
#' @export
hpcStudioInternal <- function(scope = NULL, label = NULL,
                              mode = "mine+global") {
  .dshpc_require_trusted_server_caller()
  .dshpc_require_trusted_server_caller(label)
  mode <- match.arg(mode, c("mine", "mine+global", "global"))
  label <- .studio_label(label)

  db <- .db_connect()
  on.exit(.db_close(db))

  # Client-supplied owner scopes are not an authenticated Rock identity.
  # Analyst Studio therefore exposes explicitly global jobs only.
  mode <- "global"
  jobs <- .studio_jobs(db, owner_id = "", label = label, mode = mode)
  job_ids <- jobs$job_id
  specs <- .studio_specs(db, job_ids)
  steps <- .studio_steps(db, job_ids, specs)
  dag <- .studio_dag(steps, specs)

  list(
    server_time = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"),
    mode = mode,
    label = label,
    jobs = jobs,
    steps = steps,
    dag_nodes = dag$nodes,
    dag_edges = dag$edges,
    outputs = .studio_outputs(db, job_ids),
    events = .studio_events(db, job_ids),
    scheduler = .studio_scheduler(db)
  )
}

#' @keywords internal
.studio_label <- function(label) {
  if (is.null(label) || length(label) == 0) return(NULL)
  label <- as.character(label[1])
  if (is.na(label) || !nzchar(label)) NULL else label
}

#' @keywords internal
.studio_jobs <- function(db, owner_id, label = NULL, mode = "mine+global") {
  jobs <- .store_list_jobs(db, label = label, owner_id = owner_id, scope = mode)
  if (nrow(jobs) == 0) return(.studio_empty_jobs())

  jobs$step_index <- as.integer(jobs$step_index)
  jobs$total_steps <- as.integer(jobs$total_steps)
  jobs$retry_count <- as.integer(jobs$retry_count)
  jobs$progress <- paste0(jobs$step_index, "/", jobs$total_steps)
  jobs$progress_percent <- ifelse(jobs$total_steps > 0,
    round(100 * jobs$step_index / jobs$total_steps), 0L)
  jobs$scope <- ifelse(jobs$owner_id == owner_id, "mine", "global")
  jobs$error <- vapply(jobs$error_message, .studio_sanitize_error, character(1))
  jobs$elapsed_seconds <- mapply(.studio_elapsed_seconds,
    jobs$started_at, jobs$finished_at)
  jobs$queue_seconds <- mapply(.studio_elapsed_seconds,
    jobs$submitted_at, jobs$started_at)

  jobs[, c("job_id", "state", "scope", "visibility", "name", "label",
    "resource_class", "priority", "submitted_at", "accepted_at",
    "started_at", "finished_at", "elapsed_seconds", "queue_seconds",
    "step_index", "total_steps", "progress", "progress_percent",
    "retry_count", "error_class", "error"), drop = FALSE]
}

#' @keywords internal
.studio_empty_jobs <- function() {
  data.frame(
    job_id = character(0), state = character(0), scope = character(0),
    visibility = character(0), name = character(0), label = character(0),
    resource_class = character(0), priority = integer(0),
    submitted_at = character(0), accepted_at = character(0),
    started_at = character(0), finished_at = character(0),
    elapsed_seconds = numeric(0), queue_seconds = numeric(0),
    step_index = integer(0), total_steps = integer(0),
    progress = character(0), progress_percent = numeric(0),
    retry_count = integer(0), error_class = character(0),
    error = character(0), stringsAsFactors = FALSE)
}

#' @keywords internal
.studio_steps <- function(db, job_ids, specs) {
  if (length(job_ids) == 0) return(.studio_empty_steps())
  rows <- .studio_query_for_jobs(db, job_ids,
    paste(
      "SELECT job_id, step_index, type, plane, runner, state, input_refs,",
      "started_at, finished_at, exit_code, error_class, error_message,",
      "external_backend, external_status",
      "FROM steps WHERE job_id IN (%s)",
      "ORDER BY job_id, step_index"))
  if (nrow(rows) == 0) return(.studio_empty_steps())

  rows$step_index <- as.integer(rows$step_index)
  rows$node_id <- vapply(seq_len(nrow(rows)), function(i) {
    .studio_node_id(specs[[rows$job_id[i]]], rows$step_index[i])
  }, character(1))
  rows$runner[is.na(rows$runner)] <- ""
  rows$external_backend[is.na(rows$external_backend)] <- ""
  rows$external_status[is.na(rows$external_status)] <- ""
  rows$error <- vapply(rows$error_message, .studio_sanitize_error, character(1))
  rows$duration_seconds <- mapply(.studio_elapsed_seconds,
    rows$started_at, rows$finished_at)

  rows[, c("job_id", "step_index", "node_id", "type", "plane", "runner",
    "state", "started_at", "finished_at", "duration_seconds", "exit_code",
    "error_class", "error", "external_backend", "external_status"),
    drop = FALSE]
}

#' @keywords internal
.studio_empty_steps <- function() {
  data.frame(
    job_id = character(0), step_index = integer(0), node_id = character(0),
    type = character(0), plane = character(0), runner = character(0),
    state = character(0), started_at = character(0), finished_at = character(0),
    duration_seconds = numeric(0), exit_code = integer(0),
    error_class = character(0), error = character(0),
    external_backend = character(0), external_status = character(0),
    stringsAsFactors = FALSE)
}

#' @keywords internal
.studio_specs <- function(db, job_ids) {
  specs <- list()
  if (length(job_ids) == 0) return(specs)
  rows <- .studio_query_for_jobs(db, job_ids,
    "SELECT job_id, spec_json FROM jobs WHERE job_id IN (%s)")
  for (i in seq_len(nrow(rows))) {
    specs[[rows$job_id[i]]] <- tryCatch(
      jsonlite::fromJSON(rows$spec_json[i], simplifyVector = FALSE),
      error = function(e) NULL)
  }
  specs
}

#' @keywords internal
.studio_node_id <- function(spec, step_index) {
  if (is.null(spec) || is.null(spec$steps) ||
      length(spec$steps) < as.integer(step_index)) {
    return(paste0("step_", as.integer(step_index)))
  }
  step <- spec$steps[[as.integer(step_index)]] %||% NULL
  node_id <- step$node_id %||% NULL
  if (!is.null(node_id) && nzchar(node_id)) as.character(node_id)
  else paste0("step_", as.integer(step_index))
}

#' @keywords internal
.studio_dag <- function(steps, specs) {
  if (nrow(steps) == 0) {
    return(list(nodes = .studio_empty_dag_nodes(),
                edges = .studio_empty_dag_edges()))
  }

  nodes <- steps[, c("job_id", "step_index", "node_id", "type", "plane",
    "runner", "state"), drop = FALSE]
  edges <- list()

  for (job_id in unique(steps$job_id)) {
    spec <- specs[[job_id]]
    job_steps <- steps[steps$job_id == job_id, , drop = FALSE]
    by_index <- stats::setNames(job_steps$node_id, job_steps$step_index)
    has_explicit_edges <- FALSE

    for (i in seq_len(nrow(job_steps))) {
      step_index <- as.integer(job_steps$step_index[i])
      step_spec <- if (!is.null(spec) && !is.null(spec$steps) &&
          length(spec$steps) >= step_index) spec$steps[[step_index]] else list()
      inputs <- step_spec$inputs %||% NULL
      refs <- .studio_input_refs(inputs)
      if (length(refs) > 0) has_explicit_edges <- TRUE
      for (ref in refs) {
        from_step <- as.integer(ref$step %||% NA_integer_)
        if (is.na(from_step)) next
        from_node <- ref$ref %||% by_index[[as.character(from_step)]] %||%
          paste0("step_", from_step)
        edges[[length(edges) + 1L]] <- data.frame(
          job_id = job_id,
          from_step = from_step,
          to_step = step_index,
          from_node = as.character(from_node),
          to_node = job_steps$node_id[i],
          input_name = as.character(ref$name %||% ""),
          stringsAsFactors = FALSE)
      }
    }

    if (!has_explicit_edges && is.null(spec$dag) && nrow(job_steps) > 1) {
      for (i in 2:nrow(job_steps)) {
        edges[[length(edges) + 1L]] <- data.frame(
          job_id = job_id,
          from_step = as.integer(job_steps$step_index[i - 1L]),
          to_step = as.integer(job_steps$step_index[i]),
          from_node = job_steps$node_id[i - 1L],
          to_node = job_steps$node_id[i],
          input_name = "",
          stringsAsFactors = FALSE)
      }
    }
  }

  list(
    nodes = nodes,
    edges = if (length(edges) == 0) .studio_empty_dag_edges()
      else do.call(rbind, edges)
  )
}

#' @keywords internal
.studio_empty_dag_nodes <- function() {
  data.frame(job_id = character(0), step_index = integer(0),
    node_id = character(0), type = character(0), plane = character(0),
    runner = character(0), state = character(0), stringsAsFactors = FALSE)
}

#' @keywords internal
.studio_empty_dag_edges <- function() {
  data.frame(job_id = character(0), from_step = integer(0),
    to_step = integer(0), from_node = character(0), to_node = character(0),
    input_name = character(0), stringsAsFactors = FALSE)
}

#' @keywords internal
.studio_input_refs <- function(inputs) {
  if (is.null(inputs)) return(list())
  if (is.numeric(inputs)) {
    return(lapply(as.integer(inputs), function(step) {
      list(step = step, name = paste0("step_", step), ref = NULL)
    }))
  }
  if (is.character(inputs)) return(list())
  if (!is.list(inputs)) return(list())

  nm <- names(inputs) %||% rep("", length(inputs))
  out <- list()
  for (i in seq_along(inputs)) {
    item <- inputs[[i]]
    name <- nm[[i]]
    if (is.numeric(item) && length(item) == 1) {
      step <- as.integer(item)
      out[[length(out) + 1L]] <- list(step = step,
        name = if (nzchar(name)) name else paste0("step_", step), ref = NULL)
    } else if (is.list(item)) {
      step <- item$step %||% item$step_index
      if (!is.null(step)) {
        out[[length(out) + 1L]] <- list(
          step = as.integer(step),
          name = item$name %||% if (nzchar(name)) name else paste0("step_", step),
          ref = item$ref %||% item$node %||% item$id %||% NULL)
      }
    }
  }
  out
}

#' @keywords internal
.studio_outputs <- function(db, job_ids) {
  if (length(job_ids) == 0) return(.studio_empty_outputs())
  rows <- .studio_query_for_jobs(db, job_ids,
    paste(
      "SELECT job_id, step_index, name, kind, safe_for_client,",
      "created_at FROM outputs WHERE job_id IN (%s)",
      "ORDER BY job_id, id"))
  if (nrow(rows) == 0) return(.studio_empty_outputs())
  rows$step_index <- as.integer(rows$step_index)
  rows$safe_for_client <- as.logical(rows$safe_for_client)
  rows$size_bytes <- rep(NA_real_, nrow(rows))
  rows
}

#' @keywords internal
.studio_empty_outputs <- function() {
  data.frame(job_id = character(0), step_index = integer(0),
    name = character(0), kind = character(0), safe_for_client = logical(0),
    size_bytes = numeric(0), created_at = character(0),
    stringsAsFactors = FALSE)
}

#' @keywords internal
.studio_events <- function(db, job_ids) {
  if (length(job_ids) == 0) return(.studio_empty_events())
  rows <- .studio_query_for_jobs(db, job_ids,
    "SELECT job_id, event, timestamp FROM events WHERE job_id IN (%s) ORDER BY timestamp")
  if (nrow(rows) == 0) return(.studio_empty_events())
  rows
}

#' @keywords internal
.studio_empty_events <- function() {
  data.frame(job_id = character(0), event = character(0),
    timestamp = character(0), stringsAsFactors = FALSE)
}

#' @keywords internal
.studio_scheduler <- function(db) {
  status <- .scheduler_status(db)
  executor <- status$executor %||% list()
  cell <- status$cell %||% list()
  workers <- cell$workers %||% data.frame()

  list(
    mode = status$mode,
    executor = list(
      backend = executor$backend %||% NA_character_,
      available = isTRUE(executor$available),
      delegates_resources = isTRUE(executor$delegates_resources),
      reason = executor$reason %||% NA_character_,
      capabilities = executor$capabilities %||% list(),
      container = .studio_container_status(executor$container %||% list())
    ),
    cell = list(
      cell_id = cell$cell_id %||% NA_character_,
      node_id = cell$node_id %||% NA_character_,
      leader_active = !is.null(cell$leader),
      worker_count = if (is.data.frame(workers)) nrow(workers) else 0L,
      workers_by_state = .studio_counts(
        if (is.data.frame(workers) && "state" %in% names(workers))
          workers$state else character(0))
    ),
    node = .studio_resource_summary(status$node %||% list()),
    usage = .studio_resource_summary(status$usage %||% list()),
    throttles = status$throttles %||% data.frame(),
    cooldowns = status$cooldowns %||% data.frame()
  )
}

#' @keywords internal
.studio_container_status <- function(container) {
  list(runtime = container$runtime %||% NA_character_,
       available = isTRUE(container$available),
       pull = container$pull %||% NA_character_,
       network = container$network %||% NA_character_)
}

#' @keywords internal
.studio_resource_summary <- function(x) {
  keep <- c("memory_mb", "total_memory_mb", "memory_reserve_mb", "cpu_slots",
    "gpus", "gpu_memory_mb", "gpu_backend", "running_jobs")
  out <- x[intersect(names(x), keep)]
  if (length(out) == 0) list() else out
}

#' @keywords internal
.studio_counts <- function(x) {
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0) return(data.frame(value = character(0), n = integer(0)))
  tab <- table(x)
  data.frame(value = names(tab), n = as.integer(tab),
    stringsAsFactors = FALSE)
}

#' @keywords internal
.studio_query_for_jobs <- function(db, job_ids, sql_template) {
  job_ids <- unique(as.character(job_ids))
  ph <- paste(rep("?", length(job_ids)), collapse = ", ")
  DBI::dbGetQuery(db, sprintf(sql_template, ph), params = as.list(job_ids))
}

#' @keywords internal
.studio_sanitize_error <- function(x) {
  .safe_job_error(x)
}

#' @keywords internal
.studio_elapsed_seconds <- function(start, finish = NA_character_) {
  start <- .studio_parse_time(start)
  if (is.na(start)) return(NA_real_)
  finish <- .studio_parse_time(finish)
  if (is.na(finish)) finish <- Sys.time()
  as.numeric(difftime(finish, start, units = "secs"))
}

#' @keywords internal
.studio_parse_time <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[1]) || !nzchar(x[1])) {
    return(as.POSIXct(NA))
  }
  x <- as.character(x[1])
  out <- suppressWarnings(as.POSIXct(x, format = "%Y-%m-%dT%H:%M:%OSZ",
    tz = "UTC"))
  if (!is.na(out)) return(out)
  out <- suppressWarnings(as.POSIXct(x, format = "%Y-%m-%dT%H:%M:%S%z",
    tz = "UTC"))
  if (!is.na(out)) return(out)
  suppressWarnings(as.POSIXct(x, tz = "UTC"))
}
