# Module: Resource Accounting

#' @keywords internal
.check_quotas <- function(db, owner_id) {
  settings <- .dshpc_settings()
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
.dshpc_settings <- function() {
  list(
    max_jobs_global = as.integer(.dshpc_option("max_jobs_global", 1000000L)),
    max_jobs_per_user = as.numeric(.dshpc_option("max_jobs_per_user", Inf)),
    max_queued_jobs_global = as.integer(.dshpc_option("max_queued_jobs_global",
      .dshpc_option("max_jobs_queued_global", 1000000L))),
    max_steps_per_job = as.integer(.dshpc_option("max_steps_per_job", 50L)),
    max_spec_bytes = as.integer(.dshpc_option("max_spec_bytes", 10485760L)),
    default_timeout_secs = as.integer(.dshpc_option("default_timeout_secs", 86400L)),
    max_retries = as.integer(.dshpc_option("max_retries", 3L)),
    pending_timeout_hours = as.numeric(.dshpc_option("pending_timeout_hours", 168)),
    job_expiry_hours = as.numeric(.dshpc_option("job_expiry_hours", 720)),
    worker_poll_secs = as.numeric(.dshpc_option("worker_poll_secs", 2)),
    worker_leader_ttl_secs = as.numeric(.dshpc_option("worker_leader_ttl_secs", 30)),
    scheduler = as.character(.dshpc_option("scheduler", "adaptive")),
    executor_backend = as.character(.dshpc_option("executor_backend", "embedded")),
    external_enforce_local_resources = isTRUE(.dshpc_option("external_enforce_local_resources", FALSE)),
    external_enforce_runner_concurrency = isTRUE(.dshpc_option("external_enforce_runner_concurrency", FALSE)),
    cell_id = as.character(.dshpc_option("cell_id", "auto")),
    node_id = as.character(.dshpc_option("node_id", "auto")),
    worker_autostart = .dshpc_option("worker_autostart", "auto"),
    node_memory_mb = .dshpc_option("node_memory_mb", "auto"),
    memory_reserve_mb = as.integer(.dshpc_option("memory_reserve_mb", 2048L)),
    cpu_slots = .dshpc_option("cpu_slots", "auto"),
    gpu_count = .dshpc_option("gpu_count", "auto"),
    gpu_memory_reserve_mb = as.integer(.dshpc_option("gpu_memory_reserve_mb", 512L)),
    oom_throttle_hours = as.numeric(.dshpc_option("oom_throttle_hours", 24)),
    oom_throttle_max_concurrent = as.integer(.dshpc_option("oom_throttle_max_concurrent", 1L)),
    scheduler_scan_limit = as.integer(.dshpc_option("scheduler_scan_limit", 100L)),
    default_runner_memory_mb = as.integer(.dshpc_option("default_runner_memory_mb", 1024L)),
    default_runner_cpu_slots = as.integer(.dshpc_option("default_runner_cpu_slots", 1L)),
    slurm_sbatch = .dshpc_option("slurm_sbatch", Sys.getenv("DSHPC_SLURM_SBATCH", unset = "")),
    slurm_squeue = .dshpc_option("slurm_squeue", Sys.getenv("DSHPC_SLURM_SQUEUE", unset = "")),
    slurm_sacct = .dshpc_option("slurm_sacct", Sys.getenv("DSHPC_SLURM_SACCT", unset = "")),
    slurm_scancel = .dshpc_option("slurm_scancel", Sys.getenv("DSHPC_SLURM_SCANCEL", unset = "")),
    slurm_sinfo = .dshpc_option("slurm_sinfo", Sys.getenv("DSHPC_SLURM_SINFO", unset = "")),
    slurm_partition = .dshpc_option("slurm_partition", Sys.getenv("DSHPC_SLURM_PARTITION", unset = "")),
    slurm_account = .dshpc_option("slurm_account", Sys.getenv("DSHPC_SLURM_ACCOUNT", unset = "")),
    slurm_qos = .dshpc_option("slurm_qos", Sys.getenv("DSHPC_SLURM_QOS", unset = "")),
    slurm_time = .dshpc_option("slurm_time", Sys.getenv("DSHPC_SLURM_TIME", unset = "")),
    slurm_extra_args = .dshpc_option("slurm_extra_args", character(0)),
    slurm_request_optional_gpus = isTRUE(.dshpc_option("slurm_request_optional_gpus", FALSE)),
    backend_gpu_count = .dshpc_option("backend_gpu_count",
      Sys.getenv("DSHPC_BACKEND_GPU_COUNT", unset = "auto")),
    backend_request_optional_gpus = .dshpc_option("backend_request_optional_gpus",
      Sys.getenv("DSHPC_BACKEND_REQUEST_OPTIONAL_GPUS", unset = "auto")),
    backend_capabilities_cmd = .dshpc_option("backend_capabilities_cmd",
      Sys.getenv("DSHPC_BACKEND_CAPABILITIES_CMD", unset = "")),
    backend_capabilities_ttl_secs = as.numeric(.dshpc_option("backend_capabilities_ttl_secs",
      Sys.getenv("DSHPC_BACKEND_CAPABILITIES_TTL_SECS", unset = "30"))),
    container_runtime = .dshpc_option("container_runtime",
      Sys.getenv("DSHPC_CONTAINER_RUNTIME", unset = "auto")),
    container_pull = .dshpc_option("container_pull",
      Sys.getenv("DSHPC_CONTAINER_PULL", unset = "missing")),
    container_network = .dshpc_option("container_network",
      Sys.getenv("DSHPC_CONTAINER_NETWORK", unset = "none")),
    container_extra_args = .dshpc_option("container_extra_args", character(0)),
    container_run_as_current_user = isTRUE(.dshpc_option("container_run_as_current_user", FALSE)),
    external_submit_cmd = .dshpc_option("external_submit_cmd", Sys.getenv("DSHPC_EXTERNAL_SUBMIT_CMD", unset = "")),
    external_status_cmd = .dshpc_option("external_status_cmd", Sys.getenv("DSHPC_EXTERNAL_STATUS_CMD", unset = "")),
    external_cancel_cmd = .dshpc_option("external_cancel_cmd", Sys.getenv("DSHPC_EXTERNAL_CANCEL_CMD", unset = "")),
    backend_path_mappings = .dshpc_option("backend_path_mappings",
      .dshpc_option("path_mappings", Sys.getenv("DSHPC_BACKEND_PATH_MAPPINGS", unset = ""))),
    runner_overrides = .dshpc_option("runner_overrides", list()))
}
