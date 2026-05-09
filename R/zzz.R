# Module: Package Hooks + Core Utilities
# No daemon start here. Worker is external (systemd/Docker).

`%||%` <- function(x, y) if (is.null(x)) y else x

# Package-level environment (plugin registries, cached connections)
.dshpc_env <- new.env(parent = emptyenv())

# Publisher plugin registry
.dshpc_env$.publishers <- list()

#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # Ensure DSHPC_HOME exists with required subdirectories.
  # The configure script should create these during R CMD INSTALL,
  # but Opal/Rock API installs may skip configure scripts entirely.
  # This fallback creates the structure at package load time.
  home <- .dshpc_option("home", "/srv/dshpc")
  subdirs <- c("artifacts", "runners", "publish", "staging")
  for (d in c(home, file.path(home, subdirs))) {
    if (!dir.exists(d)) {
      tryCatch(
        dir.create(d, recursive = TRUE, showWarnings = FALSE, mode = "0777"),
        error = function(e) NULL
      )
    }
    tryCatch(Sys.chmod(d, "0777"), error = function(e) NULL)
  }
  tryCatch({
    db <- .db_connect()
    .db_close(db)
  }, error = function(e) NULL)
  if (.dshpc_should_autostart_worker()) {
    tryCatch(.dshpc_worker_start(), error = function(e) NULL)
  }
  invisible(NULL)
}

#' @keywords internal
.dshpc_should_autostart_worker <- function() {
  mode <- .dshpc_option("worker_autostart", "auto")
  if (isFALSE(mode)) return(FALSE)
  if (identical(Sys.getenv("DSHPC_WORKER", unset = ""), "1")) return(FALSE)
  if (identical(Sys.getenv("DSHPC_DISABLE_AUTOSTART", unset = ""), "1")) return(FALSE)
  if (nzchar(Sys.getenv("_R_CHECK_PACKAGE_NAME_", unset = ""))) return(FALSE)
  if (nzchar(Sys.getenv("TESTTHAT", unset = ""))) return(FALSE)
  home <- .dshpc_home(must_exist = FALSE)
  if (is.null(home) || !dir.exists(home) || file.access(home, mode = 2) != 0)
    return(FALSE)
  if (isTRUE(mode)) return(TRUE)
  if (is.character(mode) && identical(mode, "true")) return(TRUE)
  if (identical(Sys.getenv("DSHPC_FORCE_AUTOSTART", unset = ""), "1"))
    return(TRUE)
  cmdline <- paste(commandArgs(FALSE), collapse = " ")
  grepl("Rserve|rock", cmdline, ignore.case = TRUE)
}

# --- Core utilities ---

#' @keywords internal
.dshpc_home <- function(must_exist = TRUE) {
  home <- .dshpc_option("home", "/srv/dshpc")
  if (must_exist && !dir.exists(home)) {
    stop("DSHPC_HOME does not exist: ", home, call. = FALSE)
  }
  home
}

#' @keywords internal
.dshpc_option <- function(name, default = NULL) {
  value <- getOption(paste0("dshpc.", name), NULL)
  if (!is.null(value)) return(value)
  value <- getOption(paste0("default.dshpc.", name), NULL)
  if (!is.null(value)) return(value)

  env_name <- paste0("DSHPC_", toupper(gsub("[^A-Za-z0-9]+", "_", name)))
  env_value <- Sys.getenv(env_name, unset = NA_character_)
  if (!is.na(env_value) && nzchar(env_value)) return(env_value)

  default
}

#' Deserialize B64/JSON argument from Opal transport
#' @keywords internal
.ds_arg <- function(x) {
  if (is.character(x) && length(x) == 1) {
    if (startsWith(x, "B64:")) {
      b64 <- substring(x, 5)
      b64 <- gsub("-", "+", b64)
      b64 <- gsub("_", "/", b64)
      pad <- (4 - nchar(b64) %% 4) %% 4
      if (pad > 0) b64 <- paste0(b64, strrep("=", pad))
      json <- rawToChar(jsonlite::base64_dec(b64))
      return(jsonlite::fromJSON(json, simplifyVector = FALSE))
    }
    if (nchar(x) > 0 && substr(x, 1, 1) %in% c("{", "[")) {
      return(jsonlite::fromJSON(x, simplifyVector = FALSE))
    }
  }
  x
}

#' Get owner identity
#'
#' In Opal/Rock, the DataSHIELD session user identity is NOT available
#' in the R process environment (Rock authenticates with its own internal
#' user, not the Opal user). Therefore, the client must pass the owner_id
#' as part of the job spec (.owner field). This function provides fallbacks
#' for DSLite and local testing.
#'
#' @param spec_owner Character or NULL; owner from the job spec (.owner field).
#' @keywords internal
.get_owner_id <- function(spec_owner = NULL) {
  # Best: explicit owner from client (injected by dsHPCClient)
  if (!is.null(spec_owner) && nzchar(spec_owner)) return(spec_owner)
  # DSLite / local fallback
  owner <- Sys.getenv("USER", unset = "")
  if (nzchar(owner)) return(owner)
  "anonymous"
}

#' Validate path-safe identifier
#' @keywords internal
.validate_identifier <- function(x, field_name) {
  if (!is.character(x) || length(x) != 1 || !nzchar(x))
    stop(field_name, " must be a non-empty string.", call. = FALSE)
  if (grepl("\\.\\.", x))
    stop(field_name, " must not contain '..'.", call. = FALSE)
  if (!grepl("^[a-zA-Z0-9][a-zA-Z0-9_.-]*$", x))
    stop(field_name, " contains invalid characters.", call. = FALSE)
  x
}

#' Check DSHPC_HOME directories and permissions.
#' @keywords internal
.dshpc_home_health <- function() {
  home <- .dshpc_home(must_exist = FALSE)
  subdirs <- c("runners", "artifacts", "publish", "staging")
  paths <- c(home, file.path(home, subdirs))
  names(paths) <- c("home", subdirs)
  rows <- lapply(names(paths), function(nm) {
    path <- paths[[nm]]
    data.frame(
      name = nm,
      path = path,
      exists = !is.null(path) && dir.exists(path),
      writable = !is.null(path) && dir.exists(path) &&
        file.access(path, mode = 2) == 0,
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

#' Generate unique job ID (UUIDv4, 122 bits entropy)
#' @keywords internal
.generate_job_id <- function() {
  paste0("job_", uuid::UUIDgenerate())
}

#' Check if PID is alive
#'
#' Uses /proc filesystem (Linux) as primary check, falls back to
#' kill(pid, 0) signal. The /proc check is more reliable in containers
#' where tools::pskill can return success for zombie/dead PIDs.
#'
#' @keywords internal
.pid_is_alive <- function(pid) {
  if (is.null(pid) || is.na(pid)) return(FALSE)
  pid <- as.integer(pid)

  # Primary: check /proc/<pid>/status (Linux containers)
  proc_path <- paste0("/proc/", pid, "/status")
  if (file.exists(proc_path)) {
    # Process exists -- but check if zombie
    status_lines <- tryCatch(readLines(proc_path, warn = FALSE),
                              error = function(e) character(0))
    state_line <- grep("^State:", status_lines, value = TRUE)
    if (length(state_line) > 0 && grepl("Z \\(zombie\\)", state_line[1]))
      return(FALSE)
    return(TRUE)
  }

  # /proc not available or PID not there -- process is dead
  if (dir.exists("/proc")) return(FALSE)

  # Non-Linux fallback: use kill -0 when available. tools::pskill() returns
  # FALSE without raising on some platforms, so do not treat a clean return as
  # proof that the process exists.
  kill <- Sys.which("kill")
  if (nzchar(kill)) {
    status <- tryCatch(
      system2(kill, c("-0", as.character(pid)), stdout = FALSE, stderr = FALSE),
      error = function(e) 1L)
    return(identical(as.integer(status), 0L))
  }

  isTRUE(tryCatch(tools::pskill(pid, signal = 0L), error = function(e) FALSE))
}
