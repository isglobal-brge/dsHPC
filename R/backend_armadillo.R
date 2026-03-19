# Module: Armadillo Backend (uses MolgenisArmadillo)
# Control plane via Armadillo project store per-user.
# Project naming: dsjobs__<username> for private, dsjobs__global for shared.
#
# Verified against DSMolgenisArmadillo source (github.com/molgenis/molgenis-r-datashield):
#   ArmadilloConnection S4 class: slots = name, handle, user, cookies, token
#   conn@user = authenticated username
#   conn@token = Bearer token (if OAuth) or "" (if basic auth)
#
# Verified against MolgenisArmadillo source (github.com/molgenis/molgenis-r-armadillo):
#   armadillo.create_project(project_name, users)
#   armadillo.upload_resource(project, folder, resource, name)
#     -> resource is an R object, uploaded as .rds
#   armadillo.list_resources(project) -> character vector of "folder/name"
#   armadillo.load_resource(project, folder, name) -> loads .rds from store
#   Auth: armadillo.login() (OAuth2 device flow) or armadillo.login_basic()

#' @keywords internal
.backend_armadillo <- function(conn) {
  username <- tryCatch(conn@user, error = function(e) "anonymous")

  list(
    type = "armadillo",
    username = username,

    cp_submit = function(job_id, spec) {
      project <- paste0("dsjobs__", username)
      .armadillo_ensure_project(project)
      # armadillo.upload_resource takes an R object (serialized as .rds)
      MolgenisArmadillo::armadillo.upload_resource(
        project = project, folder = "inbox",
        resource = spec, name = job_id)
    },

    cp_read_status = function(job_id) {
      project <- paste0("dsjobs__", username)
      tryCatch(
        MolgenisArmadillo::armadillo.load_resource(
          project = project, folder = "jobs",
          name = paste0(job_id, "_state")),
        error = function(e) NULL)
    },

    cp_read_result = function(job_id) {
      project <- paste0("dsjobs__", username)
      tryCatch(
        MolgenisArmadillo::armadillo.load_resource(
          project = project, folder = "jobs",
          name = paste0(job_id, "_result")),
        error = function(e) NULL)
    },

    cp_list_jobs = function(label = NULL) {
      project <- paste0("dsjobs__", username)
      tryCatch({
        resources <- MolgenisArmadillo::armadillo.list_resources(project = project)
        # Resources are "folder/name" strings. Filter to inbox entries.
        inbox_items <- resources[grepl("^inbox/", resources)]
        if (length(inbox_items) == 0) return(.empty_job_list())

        # Read each state from jobs folder
        rows <- lapply(inbox_items, function(item) {
          jid <- sub("^inbox/", "", item)
          st <- tryCatch(
            MolgenisArmadillo::armadillo.load_resource(
              project = project, folder = "jobs",
              name = paste0(jid, "_state")),
            error = function(e) NULL)
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
      }, error = function(e) .empty_job_list())
    },

    cp_cancel = function(job_id) {
      project <- paste0("dsjobs__", username)
      tryCatch(
        MolgenisArmadillo::armadillo.upload_resource(
          project = project, folder = "jobs",
          resource = list(action = "cancel",
            requested_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")),
          name = paste0(job_id, "_cancel")),
        error = function(e) NULL)
    }
  )
}

#' @keywords internal
.armadillo_ensure_project <- function(project) {
  tryCatch(
    MolgenisArmadillo::armadillo.create_project(project),
    error = function(e) NULL)
}
