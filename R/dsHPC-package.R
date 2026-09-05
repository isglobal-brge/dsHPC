#' dsHPC: DataSHIELD Durable Federated Job Runtime
#'
#' dsHPC provides a server-side DataSHIELD runtime for durable jobs that may
#' outlive an interactive session. It stores job state in SQLite, executes
#' short session-plane work inline, dispatches artifact-plane work through a
#' resource-aware scheduler, and exposes disclosure-safe status and result
#' methods to clients.
#'
#' Domain packages register allowlisted runners and publishers. dsHPC owns the
#' queue, worker lifecycle, resource accounting, retries, logs, artifact paths,
#' and optional execution delegation to Slurm or site-specific external HPC
#' wrappers.
#'
#' @keywords internal
"_PACKAGE"

#' dsHPC Server Options
#'
#' @description
#' dsHPC is configured through R options on the DataSHIELD server. Site admins
#' can set either `dshpc.<name>` or `default.dshpc.<name>`; the package reads
#' the specific option first, then the default option. Environment variables are
#' also supported for backend commands where noted below.
#'
#' @section Core queue options:
#' - `dshpc.home`: dsHPC state directory. Defaults to `/srv/dshpc`.
#' - `dshpc.scheduler`: scheduler mode. Defaults to `"adaptive"`.
#' - `dshpc.runtime_revision`: administrator-set lowercase SHA-256 identifying
#'   the exact executable/container/lockfile and model-weight bundle. Canonical
#'   active/completed and step-cache reuse is disabled when absent.
#' - `dshpc.queue_visibility`: `"shared"` (default) exposes only logical
#'   tracking roots and approved outputs through the analyst API; `"scoped"`
#'   disables that surface and retains capability-only access.
#' - `dshpc.max_jobs_global`: maximum simultaneously running jobs.
#' - `dshpc.max_jobs_per_user`: per-owner pending/running job quota. Its default
#'   is `Inf` for compatibility; production sites should configure a finite
#'   value and domain packages must supply a stable authorized owner.
#' - `dshpc.max_queued_jobs_global`: global pending/running queue quota.
#' - `dshpc.max_steps_per_job`: maximum steps accepted in one job spec.
#' - `dshpc.max_retries`: retry count for failed artifact steps.
#' - `dshpc.default_timeout_secs`: maximum runtime for one artifact attempt.
#'   Defaults to 86400 seconds; non-positive values disable the worker deadline.
#' - `dshpc.step_cache`: enable content-addressed reuse and single-flight
#'   coalescing of deterministic artifact steps across jobs. Defaults to
#'   `TRUE`, but historical reuse is admitted only when the trusted domain job
#'   supplies a lowercase SHA-256 `reuse_fingerprint` binding its immutable
#'   external inputs/runtime contract. Set a step's `cache = FALSE` or
#'   `cacheable = FALSE` to opt out for a non-deterministic or effectful runner.
#'
#' @section Embedded resource scheduling:
#' Admission conservatively leases the maximum requirement of any step for the
#' whole job; leases are not resized at DAG step transitions.
#' - `dshpc.node_memory_mb`: node memory budget, or `"auto"` for cgroup/host
#'   detection.
#' - `dshpc.memory_reserve_mb`: memory reserved for Rock/Rserve and OS work.
#' - `dshpc.cpu_slots`: schedulable CPU slots, or `"auto"`.
#' - `dshpc.gpu_count`: local GPU count, or `"auto"` for `nvidia-smi`,
#'   `NVIDIA_VISIBLE_DEVICES`, or `CUDA_VISIBLE_DEVICES` detection.
#' - `dshpc.gpu_memory_reserve_mb`: GPU memory reserve.
#' - `dshpc.oom_throttle_hours`: hours to keep a runner at reduced
#'   concurrency after an OOM-like exit.
#' - `dshpc.oom_throttle_max_concurrent`: effective runner concurrency while
#'   OOM throttling is active. Defaults to 1.
#' - `dshpc.runner_overrides`: named list of per-runner resource overrides.
#' - `dshpc.runner_registry_paths`: path-separated YAML files/directories with
#'   admin-controlled runner definitions to sync into `DSHPC_HOME/runners`.
#' - `dshpc.runner_registry_autosync`: enable registry sync on package load and
#'   runner lookup. Defaults to `TRUE`.
#' - `dshpc.runner_registry_sync_secs`: minimum seconds between automatic
#'   registry sync attempts. Defaults to 30.
#' - `dshpc.units_file`: administrator-owned YAML catalogue for selectable
#'   execution units. Production Rock and worker processes should receive the
#'   same path through `DSHPC_UNITS_FILE`. Unit Resources contain no executor
#'   credentials; SSH keys and cloud identities stay in worker mounts, agents,
#'   or workload identity.
#' - `dshpc.site_default_pool_id`: physical scheduler-pool identifier for the
#'   site default. Set the same `resource_pool_id` on a selectable alias that
#'   reaches that physical executor. Defaults to `"site-default"`.
#'
#' @section Executor backends:
#' - `dshpc.executor_backend`: one of `"embedded"`, `"slurm"`, `"external"`,
#'   or `"kubernetes"`.
#' - `dshpc.external_enforce_local_resources`: when `TRUE`, keep local
#'   CPU/RAM/GPU budget checks even for delegated backends.
#' - `dshpc.external_enforce_runner_concurrency`: when `TRUE`, keep local
#'   per-runner and concurrency-group limits for delegated backends.
#' - `dshpc.backend_path_mappings`: named character vector or equivalent JSON
#'   mapping Rock-local paths to backend-visible paths.
#'
#' @section Slurm backend:
#' - `dshpc.slurm_sbatch`, `dshpc.slurm_squeue`, `dshpc.slurm_sacct`,
#'   `dshpc.slurm_scancel`, `dshpc.slurm_sinfo`: command paths or names.
#'   Environment fallbacks use `DSHPC_SLURM_*`.
#' - `dshpc.slurm_partition`, `dshpc.slurm_account`, `dshpc.slurm_qos`, and
#'   `dshpc.slurm_time`: optional submit settings. If
#'   `dshpc.slurm_time` is empty, it is derived from `default_timeout_secs`.
#' - `dshpc.slurm_extra_args` is retained only for pre-0.2.5 jobs without a
#'   unit snapshot. New durable submissions reject it; put fixed site flags in
#'   an administrator-controlled `sbatch` wrapper instead.
#'
#' @section Kubernetes backend:
#' - `dshpc.kubernetes_kubectl`: `kubectl` path/name. Environment fallback:
#'   `DSHPC_KUBERNETES_KUBECTL`.
#' - `dshpc.kubernetes_context`, `dshpc.kubernetes_namespace`: optional
#'   kubectl target. Namespace defaults to `"default"`.
#' - `dshpc.kubernetes_pvc`: shared PersistentVolumeClaim mounted into runner
#'   pods. Required for Kubernetes dispatch.
#' - `dshpc.kubernetes_mount_path`: mount path for that PVC in pods. Defaults
#'   to the backend-visible `dshpc.home`; use `dshpc.backend_path_mappings`
#'   when the Rock-local dsHPC path differs from the pod mount path.
#' - `dshpc.kubernetes_image`: default runner image when the runner YAML does
#'   not declare a container image.
#' - `dshpc.kubernetes_service_account`,
#'   `dshpc.kubernetes_image_pull_policy`, `dshpc.kubernetes_backoff_limit`,
#'   and `dshpc.kubernetes_ttl_seconds_after_finished`: optional Job settings.
#'   The backend submits `batch/v1` Jobs, polls them with
#'   `kubectl get job -o json`, and cancels with `kubectl delete job`.
#'
#' @section External backend:
#' - `dshpc.external_submit_cmd`: admin-provided submit wrapper command.
#' - `dshpc.external_status_cmd`: admin-provided status wrapper command.
#' - `dshpc.external_cancel_cmd`: optional cancel wrapper command.
#' - `dshpc.backend_capabilities_cmd`: optional command that reports backend
#'   resources as a JSON object with a `gpus` field or as key-value lines such
#'   as `gpus=2`.
#' - `dshpc.backend_capabilities_ttl_secs`: cache duration for capability
#'   probes.
#'
#' @section Container runners:
#' - `dshpc.container_runtime`: `"auto"`, `"docker"`, `"podman"`,
#'   `"apptainer"`, `"singularity"`, or `"none"`.
#' - `dshpc.container_pull`: `"missing"`, `"always"`, or `"never"`.
#' - `dshpc.container_network`: network mode for Docker/Podman runners.
#' - `dshpc.container_extra_args` is retained only for pre-0.2.5 jobs without a
#'   unit snapshot. New durable submissions reject it; put fixed runtime flags
#'   in an administrator-controlled runner definition instead.
#' - `dshpc.container_run_as_current_user`: run Docker/Podman containers with
#'   the current UID/GID.
#'
#' @section Backend GPU handling:
#' Backend GPU decisions are independent of Rock-local GPU detection. Set
#' `dshpc.backend_gpu_count` to a number, or to `"auto"` to use Slurm `sinfo`
#' or `dshpc.backend_capabilities_cmd`. With
#' `dshpc.backend_request_optional_gpus = "auto"`, runners declaring
#' `optional_gpus` request GPU only when the backend reports GPU capacity.
#'
#' @seealso [hpcCapabilitiesDS()], [hpcSchedulerStatusInternal()]
#' @name dshpc-options
NULL
