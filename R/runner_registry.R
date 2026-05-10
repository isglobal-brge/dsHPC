# Module: Persistent Runner Registry
# Imports admin-controlled YAML runner definitions into DSHPC_HOME/runners.

#' Register an allowlisted dsHPC runner
#'
#' Registers a runner definition by writing a validated YAML file into
#' `DSHPC_HOME/runners`. This is intended for domain packages and site
#' administrators that need hospital-local task runners without modifying the
#' dsHPC package itself.
#'
#' @param config Named list containing at least `name`, `command`, and
#'   `args_template`.
#' @param name Optional runner name overriding `config$name`.
#' @param overwrite Logical; replace an existing runner file.
#' @return Invisibly, the installed runner file path.
#' @export
register_dshpc_runner <- function(config, name = NULL, overwrite = TRUE) {
  config <- .validate_runner_config(config, name = name)
  dest <- file.path(.dshpc_home(must_exist = FALSE), "runners",
    paste0(config$name, ".yml"))
  if (file.exists(dest) && !isTRUE(overwrite)) {
    stop("Runner already exists: ", config$name, call. = FALSE)
  }
  dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE, mode = "0777")
  yaml::write_yaml(config, dest)
  tryCatch(Sys.chmod(dest, "0666"), error = function(e) NULL)
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
    if (!is.atomic(config$args_template)) {
      stop("Runner '", config$name, "' args_template must be a vector.",
           call. = FALSE)
    }
    config$args_template <- as.character(config$args_template)
  }

  if (!is.null(config$allowed_params)) {
    params <- as.character(unlist(config$allowed_params, use.names = FALSE))
    bad <- params[!grepl("^[A-Za-z0-9_.-]+$", params)]
    if (length(bad) > 0) {
      stop("Runner '", config$name, "' has invalid allowed_params.",
           call. = FALSE)
    }
    config$allowed_params <- params
  }
  config
}
