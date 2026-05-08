# Module: Resource Accounting

#' @keywords internal
.check_quotas <- function(db, owner_id) {
  settings <- .dsjobs_settings()
  if (is.finite(settings$max_jobs_per_user)) {
    owner_n <- DBI::dbGetQuery(db,
      "SELECT COUNT(*) AS n FROM jobs
       WHERE owner_id = ? AND state IN ('PENDING','RUNNING')",
      params = list(owner_id))$n
    if (owner_n >= settings$max_jobs_per_user)
      stop("Per-user quota exceeded.", call. = FALSE)
  }

  global_n <- DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM jobs WHERE state IN ('PENDING','RUNNING')")$n
  if (global_n >= settings$max_queued_jobs_global)
    stop("Global job quota exceeded.", call. = FALSE)
}

#' @keywords internal
.dsjobs_settings <- function() {
  list(
    max_jobs_global = as.integer(.dsj_option("max_jobs_global", 1000000L)),
    max_jobs_per_user = as.numeric(.dsj_option("max_jobs_per_user", Inf)),
    max_queued_jobs_global = as.integer(.dsj_option("max_queued_jobs_global",
      .dsj_option("max_jobs_queued_global", 1000000L))),
    max_steps_per_job = as.integer(.dsj_option("max_steps_per_job", 50L)),
    max_spec_bytes = as.integer(.dsj_option("max_spec_bytes", 10485760L)),
    default_timeout_secs = as.integer(.dsj_option("default_timeout_secs", 86400L)),
    max_retries = as.integer(.dsj_option("max_retries", 3L)),
    pending_timeout_hours = as.numeric(.dsj_option("pending_timeout_hours", 168)),
    job_expiry_hours = as.numeric(.dsj_option("job_expiry_hours", 720)),
    worker_poll_secs = as.numeric(.dsj_option("worker_poll_secs", 2)),
    worker_leader_ttl_secs = as.numeric(.dsj_option("worker_leader_ttl_secs", 30)),
    scheduler = as.character(.dsj_option("scheduler", "adaptive")),
    cell_id = as.character(.dsj_option("cell_id", "auto")),
    node_id = as.character(.dsj_option("node_id", "auto")),
    worker_autostart = .dsj_option("worker_autostart", "auto"),
    node_memory_mb = .dsj_option("node_memory_mb", "auto"),
    memory_reserve_mb = as.integer(.dsj_option("memory_reserve_mb", 2048L)),
    cpu_slots = .dsj_option("cpu_slots", "auto"),
    gpu_count = .dsj_option("gpu_count", "auto"),
    gpu_memory_reserve_mb = as.integer(.dsj_option("gpu_memory_reserve_mb", 512L)),
    scheduler_scan_limit = as.integer(.dsj_option("scheduler_scan_limit", 100L)),
    default_runner_memory_mb = as.integer(.dsj_option("default_runner_memory_mb", 1024L)),
    default_runner_cpu_slots = as.integer(.dsj_option("default_runner_cpu_slots", 1L)),
    runner_overrides = .dsj_option("runner_overrides", list()))
}
