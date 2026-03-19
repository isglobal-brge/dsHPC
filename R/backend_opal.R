# Module: Opal Backend (uses opalr)
# Control plane via Opal filesystem per-user home.

#' @keywords internal
.backend_opal <- function(conn) {
  opal <- conn@opal
  username <- opal$username %||% "anonymous"

  list(
    type = "opal",
    username = username,

    cp_submit = function(job_id, spec) {
      base <- .opal_ensure_home(opal, username)
      tmp <- tempfile(paste0(job_id, "_"), fileext = ".json")
      on.exit(unlink(tmp))
      writeLines(jsonlite::toJSON(spec, auto_unbox = TRUE,
                                   pretty = TRUE, null = "null"), tmp)
      opalr::opal.file_upload(opal, tmp, paste0(base, "/inbox"))
    },

    cp_read_status = function(job_id) {
      path <- paste0("/home/", username, "/.dsjobs/jobs/", job_id, "/state.json")
      .opal_download_json(opal, path)
    },

    cp_read_result = function(job_id) {
      path <- paste0("/home/", username, "/.dsjobs/jobs/", job_id, "/result.json")
      .opal_download_json(opal, path)
    },

    cp_list_jobs = function(label = NULL) {
      jobs_path <- paste0("/home/", username, "/.dsjobs/jobs")
      dirs <- tryCatch(opalr::opal.file_ls(opal, jobs_path),
                         error = function(e) data.frame())
      if (nrow(dirs) == 0) return(.empty_job_list())

      rows <- lapply(seq_len(nrow(dirs)), function(i) {
        jid <- dirs$name[i]
        st <- tryCatch({
          p <- paste0(jobs_path, "/", jid, "/state.json")
          .opal_download_json(opal, p)
        }, error = function(e) NULL)
        if (is.null(st)) return(NULL)
        if (!is.null(label) && !identical(st$label, label)) return(NULL)
        data.frame(job_id = jid, state = st$state %||% "UNKNOWN",
          label = st$label %||% NA_character_,
          visibility = st$visibility %||% "private",
          submitted_at = st$submitted_at %||% "",
          progress = paste0(st$step_index %||% 0, "/", st$total_steps %||% 0),
          stringsAsFactors = FALSE)
      })
      rows <- Filter(Negate(is.null), rows)
      if (length(rows) == 0) return(.empty_job_list())
      do.call(rbind, rows)
    },

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
.empty_job_list <- function() {
  data.frame(job_id = character(0), state = character(0),
    label = character(0), visibility = character(0),
    submitted_at = character(0), progress = character(0),
    stringsAsFactors = FALSE)
}
