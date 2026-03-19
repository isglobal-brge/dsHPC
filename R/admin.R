# Module: Admin Functions

#' List ALL jobs (admin only)
#'
#' Shows all jobs from all users. Requires admin_key configured on server.
#'
#' @param conns DSI connections object.
#' @param admin_key Character; admin key matching dsjobs.admin_key on server.
#' @param label Character or NULL; filter by label.
#' @return A dsjobs_result with per-site data.frames.
#' @export
ds.jobs.admin.list <- function(conns, admin_key, label = NULL) {
  results <- .ds_safe_aggregate(conns,
    expr = call("jobAdminListDS", admin_key, label))
  dsjobs_result(per_site = results)
}

#' Cancel any job (admin only)
#'
#' @param conns DSI connections object.
#' @param job_id Character; job ID to cancel.
#' @param admin_key Character; admin key.
#' @export
ds.jobs.admin.cancel <- function(conns, job_id, admin_key) {
  for (srv in names(conns)) {
    tryCatch(
      DSI::datashield.assign.expr(conns[srv], symbol = job_id,
        expr = call("jobAdminCancelDS", job_id, admin_key)),
      error = function(e) NULL)
  }
  invisible(NULL)
}
