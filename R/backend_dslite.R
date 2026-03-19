# Module: DSLite Backend (fallback)
# Uses DataSHIELD methods directly for local testing with DSLite.

#' @keywords internal
.backend_dslite <- function(conn) {
  list(
    type = "dslite",
    username = Sys.getenv("USER", "anonymous"),

    cp_submit = function(job_id, spec) {
      # DSLite: use DS method directly
      NULL  # Handled in submit.R via ds_method path
    },

    cp_read_status = function(job_id) {
      NULL  # Handled in status.R via ds_method path
    },

    cp_read_result = function(job_id) {
      NULL  # Handled in result.R via ds_method path
    },

    cp_list_jobs = function(label = NULL) {
      .empty_job_list()
    },

    cp_cancel = function(job_id) {
      NULL
    }
  )
}
