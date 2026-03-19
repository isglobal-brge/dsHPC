# Module: Admin Functions
# Disabled by default. Enabled when dsjobs.admin_key is set on the server.
# Key is B64-encoded for transport (Opal's R parser can't handle special chars).

#' List ALL jobs from ALL users (admin only)
#'
#' @param conns DSI connections object.
#' @param admin_key Character; the admin key matching the server config.
#' @param label Character or NULL; filter by label.
#' @return A dsjobs_result with per-site data.frames.
#' @export
ds.jobs.admin.list <- function(conns, admin_key, label = NULL) {
  key_enc <- .ds_encode(list(.admin_key = admin_key))
  results <- .ds_safe_aggregate(conns,
    expr = call("jobAdminListDS", key_enc, label))
  dsjobs_result(per_site = results)
}

#' Cancel any job (admin only)
#'
#' @param conns DSI connections object.
#' @param job_id Character; job ID to cancel.
#' @param admin_key Character; the admin key.
#' @export
ds.jobs.admin.cancel <- function(conns, job_id, admin_key) {
  key_enc <- .ds_encode(list(.admin_key = admin_key))
  for (srv in names(conns)) {
    tryCatch(
      DSI::datashield.assign.expr(conns[srv], symbol = job_id,
        expr = call("jobAdminCancelDS", job_id, key_enc)),
      error = function(e) NULL)
  }
  invisible(NULL)
}
