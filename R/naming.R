# Module: Job Naming Utilities
# Internal display-name template resolution.

#' @keywords internal
.job_name_resolve_template <- function(db, name, owner_id, label = NULL) {
  if (is.null(name) || length(name) == 0L || is.na(name[1])) return(name)
  name <- as.character(name[1])
  if (!.job_name_has_number_placeholder(name)) return(name)

  .job_name_template_parts(name)
  jobs <- .store_list_jobs(db, label = label, owner_id = owner_id,
    scope = "mine+global")
  number <- .job_name_next_number(jobs$name, name)
  .job_name_render_template(name, number)
}

#' @keywords internal
.job_name_next_number <- function(existing_names, template) {
  parts <- .job_name_template_parts(template)
  existing_names <- as.character(existing_names %||% character(0))
  existing_names <- existing_names[!is.na(existing_names)]
  pattern <- paste0("^", .job_name_regex_escape(parts$before), "([0-9]+)",
    .job_name_regex_escape(parts$after), "$")
  matched <- existing_names[grepl(pattern, existing_names)]
  numbers <- suppressWarnings(as.integer(sub(pattern, "\\1", matched)))
  numbers <- numbers[!is.na(numbers)]
  if (length(numbers) == 0L) 1L else max(numbers) + 1L
}

#' @keywords internal
.job_name_has_number_placeholder <- function(x) {
  grepl("{number}", x, fixed = TRUE)
}

#' @keywords internal
.job_name_template_parts <- function(template) {
  if (!is.character(template) || length(template) != 1L || is.na(template) ||
      !nzchar(trimws(template))) {
    stop("Job name template must be a non-empty string.", call. = FALSE)
  }
  if (grepl("[\r\n\t]", template)) {
    stop("Job name template must be a single-line string.", call. = FALSE)
  }
  loc <- gregexpr("{number}", template, fixed = TRUE)[[1]]
  count <- if (identical(loc[1], -1L)) 0L else length(loc)
  if (count > 1L) {
    stop("Job name template can contain at most one {number}.",
      call. = FALSE)
  }
  if (count == 0L) return(list(before = template, after = ""))
  before <- substring(template, 1L, loc[1] - 1L)
  after <- substring(template, loc[1] + nchar("{number}"))
  list(before = before, after = after)
}

#' @keywords internal
.job_name_render_template <- function(template, number) {
  gsub("{number}", as.character(as.integer(number)), template, fixed = TRUE)
}

#' @keywords internal
.job_name_regex_escape <- function(x) {
  gsub("([][{}()+*^$|\\\\?.])", "\\\\\\1", x)
}
