# Module: Persistent Runner Registry
# Imports admin-controlled YAML runner definitions into DSHPC_HOME/runners.

#' Register an allowlisted dsHPC runner
#'
#' Registers a runner definition by writing a validated YAML file into
#' `DSHPC_HOME/runners`. This is intended for domain packages and site
#' packages. Site administrators install hospital-local definitions through
#' the configured runner-registry paths, which dsHPC synchronizes internally.
#' The registering package is recorded and later matched to the job label.
#' Missing `allowed_params` means that no step parameters are permitted.
#'
#' @param config Named list containing at least `name`, `command`, and
#'   `args_template`.
#' @param name Optional runner name overriding `config$name`.
#' @param overwrite Logical; replace a runner owned by the same registering
#'   package. Defaults to `FALSE`. A package cannot replace another package's
#'   runner, and installed dsHPC runners cannot be shadowed.
#' @return Invisibly, the installed runner file path.
#' @export
register_dshpc_runner <- function(config, name = NULL, overwrite = FALSE) {
  .dshpc_require_trusted_server_caller()
  owner <- .dshpc_trusted_server_caller()
  config <- .validate_runner_config(config, name = name)
  builtin <- system.file("runners", paste0(config$name, ".yml"),
    package = "dsHPC")
  if (nzchar(builtin) && file.exists(builtin)) {
    stop("Installed dsHPC runner cannot be replaced: ", config$name,
      call. = FALSE)
  }
  home <- .dshpc_home(must_exist = FALSE)
  runners_dir <- file.path(home, "runners")
  locks_dir <- file.path(home, "locks")
  if (.dshpc_path_is_symlink(home) ||
      .dshpc_path_is_symlink(runners_dir) ||
      .dshpc_path_is_symlink(locks_dir)) {
    stop("Runner registry storage is unavailable.", call. = FALSE)
  }
  .dshpc_with_private_umask(dir.create(runners_dir, recursive = TRUE,
    showWarnings = FALSE, mode = "0770"))
  .dshpc_with_private_umask(dir.create(locks_dir, recursive = TRUE,
    showWarnings = FALSE, mode = "0770"))
  if (!dir.exists(runners_dir) || !dir.exists(locks_dir) ||
      .dshpc_path_is_symlink(runners_dir) ||
      .dshpc_path_is_symlink(locks_dir)) {
    stop("Runner registry storage is unavailable.", call. = FALSE)
  }
  lock_path <- file.path(locks_dir, paste0("runner.", config$name, ".lock"))
  .lock_acquire(lock_path)
  on.exit(.lock_release(lock_path), add = TRUE)
  dest <- file.path(runners_dir, paste0(config$name, ".yml"))
  if (.dshpc_path_is_symlink(dest)) {
    stop("Runner registry entry must not be a symbolic link.", call. = FALSE)
  }
  if (file.exists(dest)) {
    existing <- tryCatch(.validate_runner_config(yaml::read_yaml(dest)),
      error = function(e) NULL)
    if (!isTRUE(overwrite)) {
      stop("Runner already exists: ", config$name, call. = FALSE)
    }
    existing_owner <- if (is.null(existing)) NULL else
      existing$registered_by %||% NULL
    if (!is.null(existing_owner) && !identical(existing_owner, owner)) {
      stop("Runner is owned by another server package: ", config$name,
        call. = FALSE)
    }
    if (is.null(existing_owner)) {
      comparable <- existing
      if (!is.null(comparable)) comparable$registered_by <- NULL
      candidate <- config
      candidate$registered_by <- NULL
      if (is.null(existing) || !identical(comparable, candidate)) {
        stop("Unowned runner must be migrated by the site operator before replacement: ",
          config$name, call. = FALSE)
      }
    }
  }
  config$registered_by <- owner
  tmp <- tempfile(pattern = ".runner-", tmpdir = dirname(dest),
                  fileext = ".yml")
  on.exit(unlink(tmp, force = TRUE), add = TRUE)
  .dshpc_with_private_umask(yaml::write_yaml(config, tmp))
  tryCatch(Sys.chmod(tmp, "0660", use_umask = FALSE),
           error = function(e) NULL)
  if (!file.rename(tmp, dest)) {
    stop("Could not atomically register runner: ", config$name,
         call. = FALSE)
  }
  invisible(dest)
}

#' @keywords internal
.dshpc_sync_runner_registries <- function(force = FALSE, quiet = TRUE) {
  if (isTRUE(.dshpc_env$.runner_registry_syncing)) return(invisible(FALSE))
  enabled <- .dshpc_option("runner_registry_autosync", TRUE)
  if (identical(enabled, FALSE) || identical(enabled, "false")) {
    return(invisible(FALSE))
  }

  paths <- .dshpc_option("runner_registry_paths",
    Sys.getenv("DSHPC_RUNNER_REGISTRY_PATHS", unset = ""))
  paths <- .parse_registry_paths(paths)
  if (length(paths) == 0) return(invisible(FALSE))

  now <- Sys.time()
  last <- .dshpc_env$.runner_registry_synced_at
  ttl <- as.numeric(.dshpc_option("runner_registry_sync_secs", 30))
  if (!force && !is.null(last) &&
      as.numeric(difftime(now, last, units = "secs")) < ttl) {
    return(invisible(TRUE))
  }

  .dshpc_env$.runner_registry_syncing <- TRUE
  on.exit({ .dshpc_env$.runner_registry_syncing <- FALSE }, add = TRUE)
  installed <- character(0)
  for (path in paths) {
    installed <- c(installed, tryCatch(
      .install_runner_registry_path(path),
      error = function(e) {
        if (!quiet) warning(conditionMessage(e), call. = FALSE)
        character(0)
      }
    ))
  }
  .dshpc_env$.runner_registry_synced_at <- now
  .dshpc_env$.runner_registry_installed <- unique(installed)
  invisible(TRUE)
}

#' @keywords internal
.parse_registry_paths <- function(paths) {
  if (is.null(paths)) return(character(0))
  if (is.character(paths) && length(paths) == 1 && nzchar(paths)) {
    first <- substr(paths, 1, 1)
    if (first %in% c("[", "{")) {
      parsed <- tryCatch(jsonlite::fromJSON(paths, simplifyVector = FALSE),
        error = function(e) NULL)
      if (!is.null(parsed)) paths <- parsed
    }
  }
  paths <- as.character(unlist(paths, use.names = FALSE))
  paths <- unlist(strsplit(paths, .Platform$path.sep, fixed = TRUE),
    use.names = FALSE)
  paths <- trimws(paths)
  unique(paths[nzchar(paths)])
}

#' @keywords internal
.install_runner_registry_path <- function(path) {
  path <- path.expand(path)
  if (!file.exists(path)) stop("Runner registry path not found: ", path, call. = FALSE)
  files <- if (dir.exists(path)) {
    list.files(path, pattern = "\\.(ya?ml)$", full.names = TRUE,
      ignore.case = TRUE)
  } else {
    path
  }
  installed <- character(0)
  for (file in files) {
    registry <- yaml::read_yaml(file)
    runners <- .registry_extract_runners(registry, file)
    for (runner in runners) {
      installed <- c(installed, register_dshpc_runner(runner, overwrite = TRUE))
    }
  }
  installed
}

#' @keywords internal
.registry_extract_runners <- function(registry, source_file) {
  if (!is.list(registry)) {
    stop("Runner registry must be YAML mapping: ", source_file, call. = FALSE)
  }
  if (is.null(registry$runners)) {
    name <- registry$name %||% sub("\\.ya?ml$", "", basename(source_file),
      ignore.case = TRUE)
    return(list(.validate_runner_config(registry, name = name)))
  }

  runners <- registry$runners
  if (!is.list(runners) || length(runners) == 0) {
    stop("Runner registry 'runners' must be a non-empty list: ",
         source_file, call. = FALSE)
  }
  names_r <- names(runners) %||% rep("", length(runners))
  out <- vector("list", length(runners))
  for (i in seq_along(runners)) {
    name <- runners[[i]]$name %||% names_r[[i]]
    out[[i]] <- .validate_runner_config(runners[[i]], name = name)
  }
  out
}

#' @keywords internal
.validate_runner_config <- function(config, name = NULL) {
  if (!is.list(config)) stop("Runner config must be a list.", call. = FALSE)
  config$name <- name %||% config$name
  config$name <- .validate_identifier(config$name, "Runner name")
  if (!grepl("^[A-Za-z0-9_]+$", config$name)) {
    stop("Runner name must contain only letters, numbers and underscore.",
         call. = FALSE)
  }

  command <- config$command %||% "python"
  if (!is.character(command) || length(command) != 1 || !nzchar(command) ||
      grepl("[\r\n]", command)) {
    stop("Runner '", config$name, "' has invalid command.", call. = FALSE)
  }
  config$command <- command

  if (!is.null(config$args_template)) {
    if (is.list(config$args_template) && length(config$args_template) == 0L) {
      config$args_template <- character(0)
    } else if (!is.atomic(config$args_template)) {
      stop("Runner '", config$name, "' args_template must be a vector.",
           call. = FALSE)
    } else {
      config$args_template <- as.character(config$args_template)
    }
  }

  if (!is.null(config$allowed_params)) {
    params <- as.character(unlist(config$allowed_params, use.names = FALSE))
    bad <- params[!grepl("^[A-Za-z0-9_.-]+$", params)]
    if (length(bad) > 0) {
      stop("Runner '", config$name, "' has invalid allowed_params.",
           call. = FALSE)
    }
    config$allowed_params <- params
  } else {
    # Missing allowlists mean no client-configurable parameters, never all.
    config$allowed_params <- character(0)
  }

  if (!is.null(config$env)) {
    if (is.list(config$env) && length(config$env) == 0L) {
      config$env <- list()
    } else if (!is.list(config$env) || is.null(names(config$env)) ||
        any(!nzchar(names(config$env))) || anyDuplicated(names(config$env))) {
      stop("Runner '", config$name, "' env must be a uniquely named list.",
        call. = FALSE)
    }
    env_names <- names(config$env)
    bad_names <- !grepl("^[A-Za-z_][A-Za-z0-9_]*$", env_names)
    reserved <- toupper(env_names) %in% .BLOCKED_ENV_VARS |
      startsWith(toupper(env_names), "DSHPC_")
    if (any(bad_names) || any(reserved)) {
      stop("Runner '", config$name,
        "' env contains an invalid or reserved variable.", call. = FALSE)
    }
    valid_values <- vapply(config$env, function(value) {
      is.atomic(value) && length(value) == 1L && !is.na(value)
    }, logical(1))
    if (!all(valid_values)) {
      stop("Runner '", config$name, "' env values must be scalar.",
        call. = FALSE)
    }
  }
  if (!is.null(config$registered_by) &&
      (!is.character(config$registered_by) ||
        length(config$registered_by) != 1L || is.na(config$registered_by) ||
        !grepl("^[A-Za-z][A-Za-z0-9.]*$", config$registered_by))) {
    stop("Runner '", config$name, "' has invalid registration ownership.",
      call. = FALSE)
  }
  config
}
