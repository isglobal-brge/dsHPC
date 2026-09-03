# Module: Disclosure Controls
# Standard DataSHIELD disclosure settings for dsHPC.

#' Read a dsHPC option with DataSHIELD double-fallback
#'
#' Option chain: dshpc.<name> -> default.dshpc.<name> -> default.
#' @noRd
.dshpc_disclosure_settings <- function() {
  raw_nfilter <- getOption("nfilter.subset",
    getOption("default.nfilter.subset", 3))
  nfilter_subset <- tryCatch(suppressWarnings(as.numeric(raw_nfilter)),
    error = function(e) numeric(0))
  if (length(nfilter_subset) != 1L || is.na(nfilter_subset) ||
      !is.finite(nfilter_subset)) {
    nfilter_subset <- 3
  }
  nfilter_subset <- max(3, ceiling(nfilter_subset))
  if (nfilter_subset > .Machine$integer.max) {
    nfilter_subset <- .Machine$integer.max
  }
  list(
    nfilter_subset = as.integer(nfilter_subset)
  )
}

#' Validate a mandatory domain label
#' @noRd
.dshpc_require_label_value <- function(value, message) {
  if (!is.character(value) || length(value) != 1L || is.na(value) ||
      !nzchar(value)) {
    stop(message, call. = FALSE)
  }
  value
}

#' Test whether a job label belongs to one server package namespace
#' @noRd
.dshpc_label_matches_package <- function(label, package) {
  is.character(label) && length(label) == 1L && !is.na(label) &&
    is.character(package) && length(package) == 1L && !is.na(package) &&
    (identical(label, package) ||
      startsWith(label, paste0(package, "_")) ||
      startsWith(label, paste0(package, ".")))
}

#' Require a side-effect-free DataSHIELD argument expression
#' @noRd
.dshpc_require_literal_or_symbol <- function(expr, argument) {
  if (is.symbol(expr) || is.atomic(expr) || is.null(expr)) {
    return(invisible(TRUE))
  }
  stop(argument, " must be a literal value or an assigned server symbol.",
    call. = FALSE)
}

#' Require an internal API call to originate in a server package namespace
#' @noRd
.dshpc_require_trusted_server_caller <- function(required_label = NULL) {
  deny <- function() stop(
    "This internal API is available only to trusted server packages.",
    call. = FALSE)
  n <- sys.nframe()
  # Exclude this guard and the protected API itself. A direct DataSHIELD
  # expression therefore has no package caller to authorize it.
  if (n <= 2L) deny()
  namespaces <- character(0)
  for (i in seq_len(n - 2L)) {
    fn <- tryCatch(sys.function(i), error = function(e) NULL)
    if (is.null(fn)) next
    env <- environment(fn)
    if (isNamespace(env)) {
      namespaces <- c(namespaces, getNamespaceName(env))
    }
  }
  namespaces <- unique(namespaces)
  # dsHPC may compose its own internal operations. External callers must be a
  # DataSHIELD domain package, conventionally named with the `ds` prefix.
  candidates <- namespaces[namespaces == "dsHPC" |
    (startsWith(namespaces, "ds") &
      !namespaces %in% c("dsHPC", "DSI", "DSLite"))]
  if (length(candidates) == 0L) deny()
  if (is.null(required_label)) return(invisible(TRUE))
  required_label <- .dshpc_require_label_value(required_label,
    "A domain label is required for this internal operation.")
  matches <- vapply(candidates, function(pkg) {
    identical(pkg, "dsHPC") ||
      .dshpc_label_matches_package(required_label, pkg)
  }, logical(1))
  if (!any(matches)) deny()
  invisible(TRUE)
}

#' Identify the nearest package caller outside a protected registration API
#' @noRd
.dshpc_trusted_server_caller <- function(registration_frame = sys.parent()) {
  if (registration_frame > 1L) {
    for (i in rev(seq_len(registration_frame - 1L))) {
      caller <- tryCatch(sys.function(i), error = function(e) NULL)
      if (is.null(caller) || !isNamespace(environment(caller))) next
      package <- getNamespaceName(environment(caller))
      if (identical(package, "dsHPC") ||
          (startsWith(package, "ds") &&
            !package %in% c("DSI", "DSLite"))) {
        return(unname(package))
      }
    }
  }
  stop("This internal API is available only to trusted server packages.",
    call. = FALSE)
}

#' Validate an object explicitly approved for the client result boundary
#' @noRd
.dshpc_client_safe_value <- function(value, kind) {
  n <- .output_object_cardinality(value)
  nfilter <- .dshpc_disclosure_settings()$nfilter_subset
  if (!is.na(n)) return(n >= nfilter)

  # The built-in summary is a small named map rather than a record vector.
  # Admit only its closed, count-only schema; arbitrary lists fail closed.
  if (!identical(as.character(kind)[1], "summary") || !is.list(value)) {
    return(FALSE)
  }
  if (length(value) == 0L) return(TRUE)
  if (is.null(names(value)) || anyDuplicated(names(value)) ||
      !all(names(value) %in% c("n_output_files", "n_samples"))) {
    return(FALSE)
  }
  all(vapply(value, function(x) {
    if (!(is.integer(x) || is.numeric(x)) || length(x) != 1L) return(FALSE)
    if (is.na(x)) return(!is.nan(x))
    if (!is.finite(x) || x < nfilter || x != floor(x)) return(FALSE)
    identical(as.numeric(x), as.numeric(nfilter)) ||
      abs(log2(x) - round(log2(x))) < .Machine$double.eps^0.5
  }, logical(1)))
}

#' Test whether an existing path is a symbolic link
#' @noRd
.dshpc_path_is_symlink <- function(path) {
  link <- Sys.readlink(path)
  !is.na(link) & nzchar(link)
}

#' Validate the existing parent chain before creating a storage target
#' @noRd
.dshpc_validate_storage_target <- function(path, root) {
  deny <- function() stop("Artifact copy target failed validation.",
    call. = FALSE)
  if (!is.character(path) || length(path) != 1L || is.na(path) ||
      !nzchar(path) || !is.character(root) || length(root) != 1L ||
      is.na(root) || !nzchar(root) ||
      grepl("(^|[/\\\\])\\.\\.([/\\\\]|$)", path)) deny()
  if (!dir.exists(root) || .dshpc_path_is_symlink(root)) deny()
  root_real <- tryCatch(normalizePath(root, winslash = "/", mustWork = TRUE),
    error = function(e) deny())

  cursor <- path
  repeat {
    if (.dshpc_path_is_symlink(cursor)) deny()
    if (file.exists(cursor) || dir.exists(cursor)) {
      cursor_real <- tryCatch(normalizePath(cursor, winslash = "/",
        mustWork = TRUE), error = function(e) deny())
      if (!identical(cursor_real, root_real) &&
          !startsWith(cursor_real, paste0(root_real, "/"))) deny()
      break
    }
    parent <- dirname(cursor)
    if (identical(parent, cursor)) deny()
    cursor <- parent
  }
  invisible(path)
}

#' Reject symbolic links anywhere in an artifact tree
#' @noRd
.dshpc_assert_symlink_free_tree <- function(path) {
  deny <- function() stop("Job artifact tree failed validation.", call. = FALSE)
  if (!is.character(path) || length(path) != 1L || is.na(path) ||
      !nzchar(path)) deny()
  if (.dshpc_path_is_symlink(path)) deny()
  if (!file.exists(path) && !dir.exists(path)) deny()
  if (!dir.exists(path)) return(invisible(path))

  pending <- path
  while (length(pending) > 0L) {
    current <- pending[[1L]]
    pending <- pending[-1L]
    children <- list.files(current, all.files = TRUE, no.. = TRUE,
      full.names = TRUE, recursive = FALSE)
    if (length(children) == 0L) next
    links <- Sys.readlink(children)
    if (any(!is.na(links) & nzchar(links))) deny()
    child_info <- file.info(children)
    child_dirs <- children[!is.na(child_info$isdir) & child_info$isdir]
    if (length(child_dirs) > 0L) pending <- c(pending, child_dirs)
  }
  invisible(path)
}

#' Validate one durable path against its job-owned artifact boundary
#' @noRd
.dshpc_validate_job_artifact_path <- function(path, job_id,
                                               check_tree = TRUE) {
  deny <- function() stop(
    "Job artifact path is invalid or outside its job boundary.",
    call. = FALSE)
  if (!is.character(path) || length(path) != 1L || is.na(path) ||
      !nzchar(path) ||
      grepl("(^|[/\\\\])\\.\\.([/\\\\]|$)", path)) deny()
  job_id <- tryCatch(.validate_identifier(job_id, "job_id"),
    error = function(e) deny())

  home <- .dshpc_home()
  artifacts_root <- file.path(home, "artifacts")
  job_root <- file.path(artifacts_root, job_id)
  if (!dir.exists(artifacts_root) || !dir.exists(job_root)) deny()

  is_absolute <- grepl("^(/|[A-Za-z]:[/\\\\]|\\\\\\\\)", path)
  candidate <- if (is_absolute) path else file.path(home, path)
  if (!file.exists(candidate) && !dir.exists(candidate)) deny()

  artifacts_real <- tryCatch(normalizePath(artifacts_root, winslash = "/",
    mustWork = TRUE), error = function(e) deny())
  job_real <- tryCatch(normalizePath(job_root, winslash = "/",
    mustWork = TRUE), error = function(e) deny())
  candidate_real <- tryCatch(normalizePath(candidate, winslash = "/",
    mustWork = TRUE), error = function(e) deny())
  within <- function(child, parent) {
    identical(child, parent) || startsWith(child, paste0(parent, "/"))
  }
  if (!within(job_real, artifacts_real) ||
      !within(candidate_real, job_real)) deny()

  # Check each lexical ancestor down to the configured artifacts root. This
  # catches a job/step directory that is itself a link even when normalizePath
  # resolves it to another location still inside the root.
  cursor <- candidate
  repeat {
    if (.dshpc_path_is_symlink(cursor)) deny()
    cursor_real <- tryCatch(normalizePath(cursor, winslash = "/",
      mustWork = TRUE), error = function(e) deny())
    if (identical(cursor_real, artifacts_real)) break
    parent <- dirname(cursor)
    if (identical(parent, cursor)) deny()
    cursor <- parent
  }

  if (isTRUE(check_tree)) .dshpc_assert_symlink_free_tree(candidate)
  candidate_real
}

#' Resolve a relative step output reference inside its owning job
#' @noRd
.dshpc_resolve_job_artifact_ref <- function(ref, job_id,
                                             check_tree = TRUE) {
  deny <- function() stop(
    "Job artifact reference is invalid or outside its job boundary.",
    call. = FALSE)
  if (!is.character(ref) || length(ref) != 1L || is.na(ref) || !nzchar(ref) ||
      grepl("^(/|[A-Za-z]:[/\\\\]|\\\\\\\\)", ref) ||
      grepl("(^|[/\\\\])\\.\\.([/\\\\]|$)", ref)) deny()
  normalized_ref <- gsub("\\\\", "/", ref)
  expected <- gsub("\\\\", "/", file.path("artifacts", job_id))
  if (!identical(normalized_ref, expected) &&
      !startsWith(normalized_ref, paste0(expected, "/"))) deny()
  .dshpc_validate_job_artifact_path(file.path(.dshpc_home(), ref), job_id,
    check_tree = check_tree)
}

#' @noRd
.sanitize_job_logs <- function(lines, last_n = 50L) {
  if (is.null(lines) || length(lines) == 0) return(character(0))
  last_n <- suppressWarnings(as.integer(last_n)[1])
  if (is.na(last_n) || last_n <= 0L) return(character(0))
  last_n <- min(last_n, 200L)
  if (length(lines) > last_n) lines <- utils::tail(lines, last_n)
  lines <- gsub("/[a-zA-Z0-9_./-]{3,}", "<path>", lines)
  lines <- gsub("[A-Z]:\\\\[a-zA-Z0-9_.\\\\ -]{3,}", "<path>", lines)
  lines <- gsub("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b", "<ip>", lines)
  lines <- gsub("<ip>:\\d+", "<ip>:<port>", lines)
  lines <- gsub("\\bpid[= ]+\\d+", "pid=<pid>", lines, ignore.case = TRUE)
  lines
}

#' Disclosure-safe count for job summaries
#' @noRd
.safe_summary_count <- function(n) {
  n <- suppressWarnings(as.integer(n)[1])
  if (is.na(n) || n < 0L) return(NA_integer_)
  nfilter <- .dshpc_disclosure_settings()$nfilter_subset
  if (n < nfilter) return(NA_integer_)
  if (n < 4L) return(n)
  max(nfilter, as.integer(2^floor(log2(n))))
}

#' Disclosure-safe job failure marker
#' @noRd
.safe_job_error <- function(error_message) {
  if (is.null(error_message) || length(error_message) == 0L ||
      is.na(error_message[1]) || !nzchar(error_message[1])) {
    return(NA_character_)
  }
  "Job execution failed."
}
