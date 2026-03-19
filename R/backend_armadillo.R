# Module: Armadillo Backend (uses MolgenisArmadillo)
# Control plane via Armadillo project store per-user.
# Project naming: dsjobs__<username> for private, dsjobs__global for shared.

#' @keywords internal
.backend_armadillo <- function(conn) {
  # ArmadilloConnection stores username in @user slot
  username <- tryCatch(conn@user, error = function(e) "anonymous")

  list(
    type = "armadillo",
    username = username,

    cp_submit = function(job_id, spec) {
      project <- paste0("dsjobs__", username)
      .armadillo_ensure_project(project)
      spec_json <- jsonlite::toJSON(spec, auto_unbox = TRUE, null = "null")
      # Upload spec as a resource in the project
      tmp <- tempfile(paste0(job_id, "_"), fileext = ".json")
      on.exit(unlink(tmp))
      writeLines(spec_json, tmp)
      MolgenisArmadillo::armadillo.upload_resource(
        project = project, folder = "inbox", name = job_id, filepath = tmp)
    },

    cp_read_status = function(job_id) {
      project <- paste0("dsjobs__", username)
      tryCatch({
        tmp <- tempfile(fileext = ".json")
        on.exit(unlink(tmp))
        MolgenisArmadillo::armadillo.load_resource(
          project = project, folder = "jobs", name = paste0(job_id, "_state"),
          destination = tmp)
        jsonlite::fromJSON(readLines(tmp, warn = FALSE), simplifyVector = FALSE)
      }, error = function(e) NULL)
    },

    cp_read_result = function(job_id) {
      project <- paste0("dsjobs__", username)
      tryCatch({
        tmp <- tempfile(fileext = ".json")
        on.exit(unlink(tmp))
        MolgenisArmadillo::armadillo.load_resource(
          project = project, folder = "jobs", name = paste0(job_id, "_result"),
          destination = tmp)
        jsonlite::fromJSON(readLines(tmp, warn = FALSE), simplifyVector = FALSE)
      }, error = function(e) NULL)
    },

    cp_list_jobs = function(label = NULL) {
      project <- paste0("dsjobs__", username)
      tryCatch({
        resources <- MolgenisArmadillo::armadillo.list_resources(project = project)
        # Filter to job state resources in the "jobs" folder
        job_resources <- resources[grepl("^jobs/.*_state$", resources)]
        if (length(job_resources) == 0) return(.empty_job_list())
        # Would need to download each state -- simplified for now
        .empty_job_list()
      }, error = function(e) .empty_job_list())
    },

    cp_cancel = function(job_id) {
      project <- paste0("dsjobs__", username)
      tmp <- tempfile("cancel_", fileext = ".json")
      on.exit(unlink(tmp))
      writeLines(jsonlite::toJSON(list(action = "cancel"),
        auto_unbox = TRUE), tmp)
      tryCatch(
        MolgenisArmadillo::armadillo.upload_resource(
          project = project, folder = "jobs",
          name = paste0(job_id, "_cancel"), filepath = tmp),
        error = function(e) NULL)
    }
  )
}

#' @keywords internal
.armadillo_ensure_project <- function(project) {
  tryCatch(
    MolgenisArmadillo::armadillo.create_project(project),
    error = function(e) NULL)  # Already exists
}
