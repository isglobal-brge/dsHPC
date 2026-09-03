# Module: Plugin Registry
# Generic extension points for publishers, dataset adapters, runners.
# Domain packages such as dsImaging register their hooks here.

#' Register a publisher plugin
#'
#' Called by domain packages such as dsImaging to register
#' publish logic. dsHPC calls the registered function when a
#' publish_asset or publish_dataset step completes. Registration records the
#' calling package and execution requires a matching job-label family.
#'
#' @param kind Character; publisher kind (e.g. "imaging_asset", "radiomics_dataset").
#' @param fn Function; publisher function(job_id, step, output_dir, db) -> list.
#' @param overwrite Logical; replace a publisher owned by the same registering
#'   package. Defaults to `FALSE`; cross-package replacement is denied.
#' @export
register_dshpc_publisher <- function(kind, fn, overwrite = FALSE) {
  .dshpc_require_trusted_server_caller()
  owner <- .dshpc_trusted_server_caller()
  kind <- .validate_identifier(kind, "Publisher kind")
  if (!is.function(fn)) stop("Publisher must be a function.", call. = FALSE)
  existing <- .dshpc_env$.publishers[[kind]]
  if (!is.null(existing)) {
    existing_owner <- if (is.list(existing) && is.function(existing$fn)) {
      existing$owner %||% "dsHPC"
    } else {
      # Pre-0.2.3 in-memory registrations were made inside the trusted node.
      "dsHPC"
    }
    if (!identical(existing_owner, owner)) {
      stop("Publisher is owned by another server package: ", kind,
        call. = FALSE)
    }
    existing_fn <- if (is.list(existing) && is.function(existing$fn)) {
      existing$fn
    } else existing
    if (!isTRUE(overwrite)) {
      if (identical(existing_fn, fn)) return(invisible(TRUE))
      stop("Publisher already exists: ", kind, call. = FALSE)
    }
  }
  .dshpc_env$.publishers[[kind]] <- list(fn = fn, owner = owner)
  invisible(TRUE)
}

#' Get a registered publisher
#' @keywords internal
.get_publisher <- function(kind) {
  entry <- .dshpc_env$.publishers[[kind]]
  if (is.list(entry) && is.function(entry$fn)) entry$fn else entry
}

#' Get the package owner of a registered publisher
#' @noRd
.get_publisher_owner <- function(kind) {
  entry <- .dshpc_env$.publishers[[kind]]
  if (is.list(entry) && is.function(entry$fn)) entry$owner %||% "dsHPC" else
    if (is.function(entry)) "dsHPC" else NULL
}

#' List registered publishers
#' @keywords internal
.list_publishers <- function() {
  names(.dshpc_env$.publishers)
}

#' Execute a publish step via plugin or fallback
#'
#' If a domain-specific publisher is registered for the step's
#' publish_kind, delegates to it. Otherwise uses generic filesystem copy.
#'
#' @keywords internal
.execute_publish <- function(job_id, step, output_dir, db) {
  output_dir <- .dshpc_validate_job_artifact_path(output_dir, job_id,
    check_tree = TRUE)
  publish_kind <- step$publish_kind %||% "generic"
  publisher <- .get_publisher(publish_kind)
  if (is.null(publisher)) {
    .load_publisher_packages(step)
    publisher <- .get_publisher(publish_kind)
  }

  if (!is.null(publisher)) {
    owner <- .get_publisher_owner(publish_kind)
    job <- .store_get_job(db, job_id)
    if (is.null(job) ||
        (!identical(owner, "dsHPC") &&
          !.dshpc_label_matches_package(job$label, owner))) {
      stop("Publisher is not registered for this job label.", call. = FALSE)
    }
    # Delegate to domain-specific publisher
    return(publisher(job_id, step, output_dir, db))
  }

  # Generic fallback: copy output to publish dir
  .publish_generic(job_id, step, output_dir, db)
}

#' Load publisher plugin packages requested by a job step
#' @keywords internal
.load_publisher_packages <- function(step) {
  cfg <- step$config %||% list()
  pkgs <- c(
    step$publisher_package,
    step$package,
    step$requires,
    cfg$publisher_package,
    .dshpc_option("publisher_packages", character(0)),
    .dshpc_option("plugin_packages", character(0))
  )
  pkgs <- unique(as.character(unlist(pkgs, use.names = FALSE)))
  pkgs <- pkgs[nzchar(pkgs)]
  if (length(pkgs) == 0) return(invisible(FALSE))
  for (pkg in pkgs) {
    tryCatch(requireNamespace(pkg, quietly = TRUE), error = function(e) FALSE)
  }
  invisible(TRUE)
}

#' Generic filesystem publisher
#' @keywords internal
.publish_generic <- function(job_id, step, output_dir, db) {
  dataset_id <- step$dataset_id
  asset_name <- step$asset_name
  if (is.null(dataset_id) || is.null(asset_name)) {
    return(list(status = "skipped", reason = "no dataset_id or asset_name"))
  }

  .validate_identifier(dataset_id, "dataset_id")
  .validate_identifier(asset_name, "asset_name")

  home <- .dshpc_home()
  output_dir <- .dshpc_validate_job_artifact_path(output_dir, job_id,
    check_tree = TRUE)
  lock_path <- .lock_acquire_dataset(dataset_id)
  tryCatch({
    publish_dir <- file.path(home, "publish", dataset_id, asset_name)
    .dshpc_with_private_umask(dir.create(dirname(publish_dir),
      recursive = TRUE, showWarnings = FALSE, mode = "0770"))
    tryCatch(Sys.chmod(dirname(publish_dir), "0770", use_umask = FALSE),
             error = function(e) NULL)
    if (dir.exists(publish_dir)) {
      backup <- paste0(publish_dir, ".bak.", format(Sys.time(), "%Y%m%d%H%M%S"))
      file.rename(publish_dir, backup)
    }
    # Copy output to publish location
    .copy_input_tree(output_dir, publish_dir,
      target_root = file.path(home, "publish"))

    .db_log_event(db, job_id, "published",
      list(dataset_id = dataset_id, asset_name = asset_name))
    .db_register_output(db, job_id, step$step_index %||% NA_integer_,
      asset_name, "published_asset", output_dir, safe_for_client = FALSE)

    list(status = "published", dataset_id = dataset_id, asset_name = asset_name,
         path = publish_dir)
  }, finally = .lock_release(lock_path))
}

# --- File locks (for publish only, not for DB operations) ---

#' @keywords internal
.lock_acquire_dataset <- function(dataset_id, timeout_secs = 60) {
  home <- .dshpc_home()
  lock_dir <- file.path(home, "locks")
  .dshpc_with_private_umask(dir.create(lock_dir, recursive = TRUE,
    showWarnings = FALSE, mode = "0770"))
  tryCatch(Sys.chmod(lock_dir, "0770", use_umask = FALSE),
           error = function(e) NULL)
  lock_path <- file.path(lock_dir, paste0("dataset.", dataset_id, ".lock"))
  .lock_acquire(lock_path, timeout_secs)
  lock_path
}

#' @keywords internal
.lock_acquire <- function(lock_path, timeout_secs = 60, stale_mins = 15) {
  deadline <- Sys.time() + timeout_secs
  repeat {
    if (!file.exists(lock_path)) {
      tryCatch({
        .dshpc_with_private_umask(local({
          con <- file(lock_path, open = "wx")
          on.exit(close(con))
          writeLines(as.character(Sys.getpid()), con)
        }))
        tryCatch(Sys.chmod(lock_path, "0660", use_umask = FALSE),
                 error = function(e) NULL)
        return(TRUE)
      }, error = function(e) {})
    }
    lock_age <- difftime(Sys.time(), file.info(lock_path)$mtime, units = "mins")
    if (!is.na(lock_age) && lock_age > stale_mins) { unlink(lock_path); next }
    if (Sys.time() > deadline) stop("Timeout acquiring lock: ", lock_path, call. = FALSE)
    Sys.sleep(1)
  }
}

#' @keywords internal
.lock_release <- function(lock_path) {
  if (file.exists(lock_path)) unlink(lock_path)
  invisible(TRUE)
}
