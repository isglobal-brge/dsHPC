# Module: Blocking Wait
# Polls job status until completion or timeout.

#' Wait for a job to complete on all nodes
#'
#' Blocking poll with progress messages.
#'
#' @param conns DSI connections object.
#' @param symbol Character; job handle symbol.
#' @param timeout Numeric; max seconds to wait (default 3600).
#' @param poll_interval Numeric; seconds between polls (default 5).
#' @return A \code{dsjobs_result} with final per-site status.
#' @export
ds.jobs.wait <- function(conns, symbol, timeout = 3600,
                          poll_interval = 5) {
  deadline <- Sys.time() + timeout
  srv_names <- names(conns)
  done <- stats::setNames(rep(FALSE, length(srv_names)), srv_names)
  terminal_states <- c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED")
  last_progress <- list()

  message("Waiting for job '", symbol, "' to complete...")

  while (Sys.time() < deadline) {
    pending <- srv_names[!done]
    if (length(pending) == 0) break

    statuses <- .ds_safe_aggregate(
      conns[pending],
      expr = call("jobStatusDS", symbol)
    )

    for (srv in pending) {
      st <- statuses[[srv]]
      if (is.null(st)) next

      # Print progress if changed
      progress_key <- paste0(st$step_index, "/", st$total_steps, ":", st$state)
      if (!identical(last_progress[[srv]], progress_key)) {
        message("  ", srv, ": ", st$state,
                " [", st$step_index, "/", st$total_steps, "]")
        last_progress[[srv]] <- progress_key
      }

      if (st$state %in% terminal_states) {
        done[[srv]] <- TRUE
        if (identical(st$state, "FAILED")) {
          message("  ", srv, ": FAILED - ", st$error %||% "unknown error")
        }
      }
    }

    if (all(done)) break
    Sys.sleep(poll_interval)
  }

  if (!all(done)) {
    failed <- srv_names[!done]
    warning("Timeout waiting for job on: ", paste(failed, collapse = ", "),
            call. = FALSE)
  }

  # Return final status
  final <- .ds_safe_aggregate(conns, expr = call("jobStatusDS", symbol))

  dsjobs_result(
    per_site = final,
    meta = list(scope = "per_site")
  )
}
