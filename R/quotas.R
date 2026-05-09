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
    executor_backend = as.character(.dsj_option("executor_backend", "embedded")),
    external_enforce_local_resources = isTRUE(.dsj_option("external_enforce_local_resources", FALSE)),
    external_enforce_runner_concurrency = isTRUE(.dsj_option("external_enforce_runner_concurrency", FALSE)),
    cell_id = as.character(.dsj_option("cell_id", "auto")),
    node_id = as.character(.dsj_option("node_id", "auto")),
    worker_autostart = .dsj_option("worker_autostart", "auto"),
    node_memory_mb = .dsj_option("node_memory_mb", "auto"),
    memory_reserve_mb = as.integer(.dsj_option("memory_reserve_mb", 2048L)),
    cpu_slots = .dsj_option("cpu_slots", "auto"),
    gpu_count = .dsj_option("gpu_count", "auto"),
    gpu_memory_reserve_mb = as.integer(.dsj_option("gpu_memory_reserve_mb", 512L)),
    oom_throttle_hours = as.numeric(.dsj_option("oom_throttle_hours", 24)),
    oom_throttle_max_concurrent = as.integer(.dsj_option("oom_throttle_max_concurrent", 1L)),
    scheduler_scan_limit = as.integer(.dsj_option("scheduler_scan_limit", 100L)),
    default_runner_memory_mb = as.integer(.dsj_option("default_runner_memory_mb", 1024L)),
    default_runner_cpu_slots = as.integer(.dsj_option("default_runner_cpu_slots", 1L)),
    slurm_sbatch = .dsj_option("slurm_sbatch", Sys.getenv("DSJOBS_SLURM_SBATCH", unset = "")),
    slurm_squeue = .dsj_option("slurm_squeue", Sys.getenv("DSJOBS_SLURM_SQUEUE", unset = "")),
    slurm_sacct = .dsj_option("slurm_sacct", Sys.getenv("DSJOBS_SLURM_SACCT", unset = "")),
    slurm_scancel = .dsj_option("slurm_scancel", Sys.getenv("DSJOBS_SLURM_SCANCEL", unset = "")),
    slurm_sinfo = .dsj_option("slurm_sinfo", Sys.getenv("DSJOBS_SLURM_SINFO", unset = "")),
    slurm_partition = .dsj_option("slurm_partition", Sys.getenv("DSJOBS_SLURM_PARTITION", unset = "")),
    slurm_account = .dsj_option("slurm_account", Sys.getenv("DSJOBS_SLURM_ACCOUNT", unset = "")),
    slurm_qos = .dsj_option("slurm_qos", Sys.getenv("DSJOBS_SLURM_QOS", unset = "")),
    slurm_time = .dsj_option("slurm_time", Sys.getenv("DSJOBS_SLURM_TIME", unset = "")),
    slurm_extra_args = .dsj_option("slurm_extra_args", character(0)),
    slurm_request_optional_gpus = isTRUE(.dsj_option("slurm_request_optional_gpus", FALSE)),
    backend_gpu_count = .dsj_option("backend_gpu_count",
      Sys.getenv("DSJOBS_BACKEND_GPU_COUNT", unset = "auto")),
    backend_request_optional_gpus = .dsj_option("backend_request_optional_gpus",
      Sys.getenv("DSJOBS_BACKEND_REQUEST_OPTIONAL_GPUS", unset = "auto")),
    backend_capabilities_cmd = .dsj_option("backend_capabilities_cmd",
      Sys.getenv("DSJOBS_BACKEND_CAPABILITIES_CMD", unset = "")),
    backend_capabilities_ttl_secs = as.numeric(.dsj_option("backend_capabilities_ttl_secs",
      Sys.getenv("DSJOBS_BACKEND_CAPABILITIES_TTL_SECS", unset = "30"))),
    container_runtime = .dsj_option("container_runtime",
      Sys.getenv("DSJOBS_CONTAINER_RUNTIME", unset = "auto")),
    container_pull = .dsj_option("container_pull",
      Sys.getenv("DSJOBS_CONTAINER_PULL", unset = "missing")),
    container_network = .dsj_option("container_network",
      Sys.getenv("DSJOBS_CONTAINER_NETWORK", unset = "none")),
    container_extra_args = .dsj_option("container_extra_args", character(0)),
    container_run_as_current_user = isTRUE(.dsj_option("container_run_as_current_user", FALSE)),
    external_submit_cmd = .dsj_option("external_submit_cmd", Sys.getenv("DSJOBS_EXTERNAL_SUBMIT_CMD", unset = "")),
    external_status_cmd = .dsj_option("external_status_cmd", Sys.getenv("DSJOBS_EXTERNAL_STATUS_CMD", unset = "")),
    external_cancel_cmd = .dsj_option("external_cancel_cmd", Sys.getenv("DSJOBS_EXTERNAL_CANCEL_CMD", unset = "")),
    backend_path_mappings = .dsj_option("backend_path_mappings",
      .dsj_option("path_mappings", Sys.getenv("DSJOBS_BACKEND_PATH_MAPPINGS", unset = ""))),
    runner_overrides = .dsj_option("runner_overrides", list()))
}
