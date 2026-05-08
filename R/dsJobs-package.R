#' dsJobs: DataSHIELD Durable Federated Job Runtime
#'
#' dsJobs provides a server-side DataSHIELD runtime for durable jobs that may
#' outlive an interactive session. It stores job state in SQLite, executes
#' short session-plane work inline, dispatches artifact-plane work through a
#' resource-aware scheduler, and exposes disclosure-safe status and result
#' methods to clients.
#'
#' Domain packages register allowlisted runners and publishers. dsJobs owns the
#' queue, worker lifecycle, resource accounting, retries, logs, artifact paths,
#' and optional execution delegation to Slurm or site-specific external HPC
#' wrappers.
#'
#' @keywords internal
"_PACKAGE"

#' dsJobs Server Options
#'
#' @description
#' dsJobs is configured through R options on the DataSHIELD server. Site admins
#' can set either `dsjobs.<name>` or `default.dsjobs.<name>`; the package reads
#' the specific option first, then the default option. Environment variables are
#' also supported for backend commands where noted below.
#'
#' @section Core queue options:
#' - `dsjobs.home`: dsJobs state directory. Defaults to `/srv/dsjobs`.
#' - `dsjobs.scheduler`: scheduler mode. Defaults to `"adaptive"`.
#' - `dsjobs.max_jobs_global`: maximum simultaneously running jobs.
#' - `dsjobs.max_jobs_per_user`: per-user pending/running job quota.
#' - `dsjobs.max_queued_jobs_global`: global pending/running queue quota.
#' - `dsjobs.max_steps_per_job`: maximum steps accepted in one job spec.
#' - `dsjobs.max_retries`: retry count for failed artifact steps.
#'
#' @section Embedded resource scheduling:
#' - `dsjobs.node_memory_mb`: node memory budget, or `"auto"` for cgroup/host
#'   detection.
#' - `dsjobs.memory_reserve_mb`: memory reserved for Rock/Rserve and OS work.
#' - `dsjobs.cpu_slots`: schedulable CPU slots, or `"auto"`.
#' - `dsjobs.gpu_count`: local GPU count, or `"auto"` for `nvidia-smi`,
#'   `NVIDIA_VISIBLE_DEVICES`, or `CUDA_VISIBLE_DEVICES` detection.
#' - `dsjobs.gpu_memory_reserve_mb`: GPU memory reserve.
#' - `dsjobs.runner_overrides`: named list of per-runner resource overrides.
#'
#' @section Executor backends:
#' - `dsjobs.executor_backend`: one of `"embedded"`, `"slurm"`, `"external"`,
#'   or `"kubernetes"` (preflight only).
#' - `dsjobs.external_enforce_local_resources`: when `TRUE`, keep local
#'   CPU/RAM/GPU budget checks even for delegated backends.
#' - `dsjobs.external_enforce_runner_concurrency`: when `TRUE`, keep local
#'   per-runner and concurrency-group limits for delegated backends.
#' - `dsjobs.backend_path_mappings`: named character vector or equivalent JSON
#'   mapping Rock-local paths to backend-visible paths.
#'
#' @section Slurm backend:
#' - `dsjobs.slurm_sbatch`, `dsjobs.slurm_squeue`, `dsjobs.slurm_sacct`,
#'   `dsjobs.slurm_scancel`, `dsjobs.slurm_sinfo`: command paths or names.
#'   Environment fallbacks use `DSJOBS_SLURM_*`.
#' - `dsjobs.slurm_partition`, `dsjobs.slurm_account`, `dsjobs.slurm_qos`,
#'   `dsjobs.slurm_time`, `dsjobs.slurm_extra_args`: optional submit settings.
#'
#' @section External backend:
#' - `dsjobs.external_submit_cmd`: admin-provided submit wrapper command.
#' - `dsjobs.external_status_cmd`: admin-provided status wrapper command.
#' - `dsjobs.external_cancel_cmd`: optional cancel wrapper command.
#' - `dsjobs.backend_capabilities_cmd`: optional command that reports backend
#'   resources as a JSON object with a `gpus` field or as key-value lines such
#'   as `gpus=2`.
#' - `dsjobs.backend_capabilities_ttl_secs`: cache duration for capability
#'   probes.
#'
#' @section Container runners:
#' - `dsjobs.container_runtime`: `"auto"`, `"docker"`, `"podman"`,
#'   `"apptainer"`, `"singularity"`, or `"none"`.
#' - `dsjobs.container_pull`: `"missing"`, `"always"`, or `"never"`.
#' - `dsjobs.container_network`: network mode for Docker/Podman runners.
#' - `dsjobs.container_extra_args`: extra runtime arguments controlled by the
#'   site admin.
#' - `dsjobs.container_run_as_current_user`: run Docker/Podman containers with
#'   the current UID/GID.
#'
#' @section Backend GPU handling:
#' Backend GPU decisions are independent of Rock-local GPU detection. Set
#' `dsjobs.backend_gpu_count` to a number, or to `"auto"` to use Slurm `sinfo`
#' or `dsjobs.backend_capabilities_cmd`. With
#' `dsjobs.backend_request_optional_gpus = "auto"`, runners declaring
#' `optional_gpus` request GPU only when the backend reports GPU capacity.
#'
#' @seealso [jobCapabilitiesDS()], [jobSchedulerStatusDS()]
#' @name dsjobs-options
NULL
