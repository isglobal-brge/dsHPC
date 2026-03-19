# Module: Opal Backend
# ALL control plane ops go through opalr with the USER's own credentials.
# No caller_id injection. Identity = Opal session user = filesystem ACL.

#' @keywords internal
.backend_opal <- function(conn) {
  opal <- conn@opal
  username <- opal$username %||% "anonymous"

  list(
    type = "opal",
    username = username,

    # --- Submit: write spec to user's inbox ---
    cp_submit = function(job_id, spec) {
      base <- .opal_ensure_home(opal, username)
      tmp <- tempfile(paste0(job_id, "_"), fileext = ".json")
      on.exit(unlink(tmp))
      writeLines(jsonlite::toJSON(spec, auto_unbox = TRUE,
        pretty = TRUE, null = "null"), tmp)
      opalr::opal.file_upload(opal, tmp, paste0(base, "/inbox"))
    },

    # --- Status: read from user's mirror ---
    cp_read_status = function(job_id) {
      .opal_download_json(opal, paste0(
        "/home/", username, "/.dsjobs/jobs/", job_id, "/state.json"))
    },

    # --- Result: read from user's mirror ---
    cp_read_result = function(job_id) {
      .opal_download_json(opal, paste0(
        "/home/", username, "/.dsjobs/jobs/", job_id, "/result.json"))
    },

    # --- List: read job dirs from user's mirror ---
    cp_list_jobs = function(label = NULL, mode = "mine+global") {
      jobs <- .empty_job_list()

      # Own jobs
      if (mode %in% c("mine", "mine+global")) {
        own <- .opal_list_mirror_jobs(opal, username, label)
        if (nrow(own) > 0) jobs <- rbind(jobs, own)
      }

      # Global jobs (from shared location if accessible)
      if (mode %in% c("global", "mine+global")) {
        global <- tryCatch(
          .opal_list_mirror_jobs(opal, ".dsjobs-global", label),
          error = function(e) .empty_job_list())
        if (nrow(global) > 0) {
          global$visibility <- "global"
          # Avoid duplicates
          new_ids <- setdiff(global$job_id, jobs$job_id)
          if (length(new_ids) > 0)
            jobs <- rbind(jobs, global[global$job_id %in% new_ids, , drop = FALSE])
        }
      }

      jobs
    },

    # --- Cancel: write cancel request to user's mirror ---
    cp_cancel = function(job_id) {
      job_dir <- paste0("/home/", username, "/.dsjobs/jobs/", job_id)
      tryCatch(opalr::opal.file_mkdir(opal, job_dir), error = function(e) NULL)
      tmp <- tempfile("cancel_", fileext = ".json")
      on.exit(unlink(tmp))
      writeLines(jsonlite::toJSON(list(action = "cancel",
        requested_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")),
        auto_unbox = TRUE), tmp)
      opalr::opal.file_upload(opal, tmp, job_dir)
    }
  )
}

# --- Helpers ---

#' @keywords internal
.opal_ensure_home <- function(opal, username) {
  base <- paste0("/home/", username, "/.dsjobs")
  for (d in c(base, paste0(base, "/inbox"), paste0(base, "/jobs")))
    tryCatch(opalr::opal.file_mkdir(opal, d), error = function(e) NULL)
  base
}

#' @keywords internal
.opal_download_json <- function(opal, path) {
  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp))
  tryCatch({
    opalr::opal.file_download(opal, path, tmp)
    jsonlite::fromJSON(readLines(tmp, warn = FALSE), simplifyVector = FALSE)
  }, error = function(e) NULL)
}

#' @keywords internal
.opal_list_mirror_jobs <- function(opal, username, label = NULL) {
  jobs_path <- paste0("/home/", username, "/.dsjobs/jobs")
  dirs <- tryCatch(opalr::opal.file_ls(opal, jobs_path),
    error = function(e) data.frame())
  if (nrow(dirs) == 0) return(.empty_job_list())

  rows <- lapply(seq_len(nrow(dirs)), function(i) {
    jid <- dirs$name[i]
    st <- .opal_download_json(opal, paste0(jobs_path, "/", jid, "/state.json"))
    if (is.null(st)) return(NULL)
    if (!is.null(label) && !identical(st$label, label)) return(NULL)
    data.frame(
      job_id = jid, state = st$state %||% "UNKNOWN",
      label = st$label %||% NA_character_,
      visibility = st$visibility %||% "private",
      owner_id = st$owner_id %||% username,
      submitted_at = st$submitted_at %||% "",
      progress = paste0(st$step_index %||% 0, "/", st$total_steps %||% 0),
      stringsAsFactors = FALSE)
  })
  rows <- Filter(Negate(is.null), rows)
  if (length(rows) == 0) return(.empty_job_list())
  do.call(rbind, rows)
}

#' @keywords internal
.empty_job_list <- function() {
  data.frame(job_id = character(0), state = character(0),
    label = character(0), visibility = character(0),
    owner_id = character(0), submitted_at = character(0),
    progress = character(0), stringsAsFactors = FALSE)
}
