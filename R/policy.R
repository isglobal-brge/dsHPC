# Module: Disclosure Controls
# Standard DataSHIELD disclosure settings for dsHPC.

#' Read a dsHPC option with DataSHIELD double-fallback
#'
#' Option chain: dshpc.<name> -> default.dshpc.<name> -> default.
#' @noRd
.dshpc_disclosure_settings <- function() {
  list(
    nfilter_subset = as.numeric(
      getOption("nfilter.subset", getOption("default.nfilter.subset", 3))
    )
  )
}

#' Whether low-level output loads require a domain label
#'
#' Defaults to TRUE so production profiles that accidentally allowlist
#' hpcLoadOutputDS still reject unscoped payload loads. Development and
#' single-user deployments can opt out with dshpc.require_domain_label = FALSE.
#' @noRd
.dshpc_require_domain_label <- function() {
  value <- .dshpc_option("require_domain_label", TRUE)
  if (is.logical(value)) return(isTRUE(value))
  value <- tolower(as.character(value)[1])
  !value %in% c("false", "0", "no", "off")
}

#' @noRd
.sanitize_job_logs <- function(lines, last_n = 50L) {
  if (is.null(lines) || length(lines) == 0) return(character(0))
  last_n <- min(as.integer(last_n), 200L)
  if (length(lines) > last_n) lines <- utils::tail(lines, last_n)
  lines <- gsub("/[a-zA-Z0-9_./-]{3,}", "<path>", lines)
  lines <- gsub("[A-Z]:\\\\[a-zA-Z0-9_.\\\\ -]{3,}", "<path>", lines)
  lines <- gsub("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b", "<ip>", lines)
  lines <- gsub("<ip>:\\d+", "<ip>:<port>", lines)
  lines <- gsub("\\bpid[= ]+\\d+", "pid=<pid>", lines, ignore.case = TRUE)
  lines
}
