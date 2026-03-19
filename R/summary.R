# Module: Job Summary View

#' Print a formatted summary of all jobs
#'
#' @param conns DSI connections object.
#' @param label Character or NULL; filter by label.
#' @export
ds.jobs.summary <- function(conns, label = NULL) {
  jl <- ds.jobs.list(conns, label = label)

  for (srv in names(jl$per_site)) {
    df <- jl$per_site[[srv]]
    cat("-- ", srv, " ", paste(rep("-", 50), collapse = ""), "\n", sep = "")

    if (!is.data.frame(df) || nrow(df) == 0) {
      cat("  (no jobs)\n\n")
      next
    }

    # Count by state
    states <- table(df$state)
    cat("  Total:", nrow(df), "jobs")
    for (s in names(states)) cat(" |", s, ":", states[s])
    cat("\n\n")

    # List each job
    for (i in seq_len(nrow(df))) {
      vis <- if (identical(df$visibility[i], "global")) "[GLOBAL]" else "[private]"
      lab <- if (!is.na(df$label[i])) paste0(" <", df$label[i], ">") else ""
      cat("  ", df$state[i], " ", vis, lab, "\n", sep = "")
      cat("    ID: ", df$job_id[i], "\n")
      cat("    Progress: ", df$progress[i],
          "  Submitted: ", df$submitted_at[i], "\n\n")
    }
  }
  invisible(jl)
}
