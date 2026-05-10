# dsHPC

dsHPC is the durable job runtime for DataSHIELD server packages. It lets a
package submit allowlisted work, keep state outside the interactive session,
throttle resource-heavy jobs, collect outputs, and publish derived artifacts
without exposing raw data to the client.

The package is intended to be installed on each Rock/DataSHIELD server. Domain
packages such as `dsImaging` register their runners and publishers; dsHPC
owns scheduling, retries, worker state, logs, result metadata, and optional
delegation to HPC backends.

## Execution model

dsHPC splits work into two planes:

- `session`: short DataSHIELD work that runs inline in the server session.
- `artifact`: heavier allowlisted runner work that is queued, isolated, and
  executed by the dsHPC worker or by a delegated backend.

Jobs are persisted in SQLite under `dshpc.home` and survive session restarts.
The default scheduler is adaptive: it reads cgroup/host CPU and memory, detects
local GPU visibility where available, leases resources while jobs run, and puts
heavy runners into cooldown after OOM-like exits.

Jobs may be submitted as classic ordered steps or as a declarative DAG pipeline.
The DAG form uses named nodes and explicit input dependencies; dsHPC validates
the graph, rejects cycles, topologically compiles it to the durable step model,
and stages multiple upstream artifact inputs under a per-step input manifest
without returning those artifacts to the client.

The same control plane supports three deployment modes:

- **Local cell:** each Rock owns its own `dshpc.home` and embedded worker.
- **Shared cell:** several Rocks/sessions point at the same writable
  `dshpc.home`; SQLite locks and worker heartbeats elect one scheduler leader
  for the shared queue.
- **HPC unit:** the Rock keeps the durable DataSHIELD control plane, while
  artifact steps are delegated to Slurm or an admin-provided external wrapper.

## Installation

Install the package on the DataSHIELD server and publish the DataSHIELD methods
as usual for the deployment:

```r
install.packages("dsHPC_0.1.0.tar.gz", repos = NULL, type = "source")
```

On load, dsHPC creates the default state tree if needed:

```text
/srv/dshpc/
  artifacts/
  locks/
  publish/
  runners/
  staging/
```

The configure/on-load path is intentionally defensive because Opal/Rock package
installs may not always run source configure scripts in the same way as a local
`R CMD INSTALL`.

## Basic options

Configure dsHPC with R options on the server. Site-wide defaults can use either
`dshpc.<name>` or `default.dshpc.<name>`.

```r
options(
  dshpc.home = "/srv/dshpc",
  dshpc.scheduler = "adaptive",
  dshpc.node_memory_mb = "auto",
  dshpc.memory_reserve_mb = 2048,
  dshpc.cpu_slots = "auto",
  dshpc.gpu_count = "auto",
  dshpc.oom_throttle_hours = 24,
  dshpc.oom_throttle_max_concurrent = 1,
  dshpc.max_jobs_global = 1000000,
  dshpc.max_jobs_per_user = Inf
)
```

Hospital/site-specific runners can be registered without editing dsHPC by
pointing `dshpc.runner_registry_paths` at YAML files or directories:

```r
options(
  dshpc.runner_registry_paths = "/etc/dshpc/runners",
  dshpc.runner_registry_autosync = TRUE
)
```

Each YAML runner remains allowlisted, resource-declared, and validated before it
can run.

Multiple Rock R sessions sharing the same `dshpc.home` participate in the same
cell. Leader election and SQLite state keep queue ownership singleton-like for
that shared cell, while allowing more than one Rock/session to see status.

For an explicit shared-cell identity, set:

```r
options(
  dshpc.home = "/shared/dshpc",
  dshpc.cell_id = "site-imaging-cell",
  dshpc.node_id = "rock-a"
)
```

If `cell_id = "auto"`, dsHPC derives the cell id from `dshpc.home`. That is
enough when the path is truly shared. For independent Rocks that happen to use
the same container path, set distinct `cell_id` values if you want observability
to make the separation explicit.

## Recovery guarantees

dsHPC treats the database and artifact directory as the source of truth. A
worker can die, an R session can disconnect, or an HPC status command can be
temporarily unavailable without losing submitted jobs.

Guardrails:

- Job specs, job state, steps, resource leases, outputs, logs, worker nodes, and
  cooldowns are persisted under `dshpc.home`.
- Worker start records the real daemon PID after heartbeat, not the transient
  launcher PID.
- Worker stop/cancel uses OS signals and clears stale scheduler locks.
- Admin-only cancellation is protected by `dshpc.admin_key` or the container
  environment variable `DSHPC_ADMIN_KEY`; package-level helpers such as
  `cancel_jobs_by_tag()` use the same gate as `hpcAdminCancelDS()`.
- Embedded artifact steps write `child.pid` and an atomic `exit_code`; missing
  `exit_code` is treated as interrupted and requeued, not as success.
- Successful step completion is committed before advancing the next step; if a
  crash happens between those phases, the next worker resumes the advance.
- Slurm/external submissions write `external_backend.json` before updating the
  DB so a new worker can recover the backend job id and continue polling.
- Transient external status failures return `STATUS_UNKNOWN` and keep the job
  running instead of creating duplicate retries.
- OOM-like exits (`-9`, `137`) put the runner/concurrency group into cooldown
  before retrying. After cooldown, recent OOMs also throttle that runner to
  `dshpc.oom_throttle_max_concurrent` for `dshpc.oom_throttle_hours`, so the
  scheduler does not repeat the same unsafe concurrency pattern.

These guarantees apply to local cell, shared cell, Slurm, and external-HPC
execution. The client API is unchanged across modes.

## Backends

### Embedded

Embedded is the default. Artifact runners execute as local child processes,
with dsHPC enforcing local CPU, memory, GPU, runner concurrency, retries, and
cooldowns:

```r
options(dshpc.executor_backend = "embedded")
```

This is the batteries-included mode for ordinary DataSHIELD deployments.

### Slurm

Slurm mode keeps dsHPC as the DataSHIELD control plane, but delegates
CPU/RAM/GPU scheduling to Slurm:

```r
options(
  dshpc.executor_backend = "slurm",
  dshpc.slurm_sbatch = "sbatch",
  dshpc.slurm_squeue = "squeue",
  dshpc.slurm_sacct = "sacct",
  dshpc.slurm_scancel = "scancel",
  dshpc.slurm_partition = "gpu"
)
```

Runner resource declarations become `sbatch` flags such as `--mem`,
`--cpus-per-task`, and, when GPU is requested, `--gres=gpu:N`.

### External

External mode is for site-specific HPC gateways. dsHPC prepares the step script
and passes paths/resources to admin-controlled wrappers:

```r
options(
  dshpc.executor_backend = "external",
  dshpc.external_submit_cmd = "/usr/local/bin/dshpc-submit",
  dshpc.external_status_cmd = "/usr/local/bin/dshpc-status",
  dshpc.external_cancel_cmd = "/usr/local/bin/dshpc-cancel"
)
```

The submit wrapper receives environment variables including:

```text
DSHPC_JOB_ID
DSHPC_STEP_INDEX
DSHPC_RUNNER
DSHPC_STEP_SCRIPT
DSHPC_OUTPUT_DIR
DSHPC_LOCAL_STEP_SCRIPT
DSHPC_LOCAL_OUTPUT_DIR
DSHPC_MEMORY_MB
DSHPC_CPU_SLOTS
DSHPC_GPUS_REQUESTED
```

The wrapper should return a backend job id on stdout. The status wrapper should
return one of `RUNNING`, `PENDING`, `SUCCEEDED`, `FAILED`, or `CANCELLED`,
optionally followed by an exit code.

## Path mappings

If the Rock path differs from the backend path, configure a mapping:

```r
options(
  dshpc.backend_path_mappings = c(
    "/srv/dshpc" = "/mnt/hpc/dshpc"
  )
)
```

Generated backend scripts use the backend-visible path, while dsHPC keeps local
paths for result registration and disclosure checks.

## Containerized runners

Runner YAML can declare a container image. This is the preferred mode for
outsourced radiomics/imaging work because the HPC unit only needs a container
runtime and mounted/staged artifacts; it does not need R packages, Python
virtualenvs, PyRadiomics, Torch, LungMask, or model code preinstalled.

```yaml
name: pyradiomics_extract
plane: artifact
resource_class: cpu_heavy
command: python
args_template:
  - /srv/dsimaging/python/dsimaging_extract.py
  - --input
  - "{input_dir}"
  - --output
  - "{output_dir}"
container:
  image: ghcr.io/isglobal-brge/dsimaging-runner@sha256:...
  runtime: auto
  pull: missing
  command: python
  args_template:
    - -m
    - dsimaging_extract
    - --input
    - "{input_dir}"
    - --output
    - "{output_dir}"
resources:
  memory_mb: 8192
  cpu_slots: 2
```

Container runtime options:

```r
options(
  dshpc.container_runtime = "auto",
  dshpc.container_pull = "missing",
  dshpc.container_network = "none"
)
```

Supported runtimes are Docker, Podman, Apptainer, and Singularity.

## Backend GPU detection

Backend GPU handling is independent of Rock-local GPU detection. A Rock may have
no GPU while the HPC backend does.

For explicit backend capacity:

```r
options(
  dshpc.backend_gpu_count = 1,
  dshpc.backend_request_optional_gpus = "auto"
)
```

For Slurm auto-detection:

```r
options(
  dshpc.executor_backend = "slurm",
  dshpc.slurm_sinfo = "sinfo",
  dshpc.backend_gpu_count = "auto",
  dshpc.backend_request_optional_gpus = "auto"
)
```

If a runner declares `optional_gpus: 1` and Slurm reports GPU capacity, dsHPC
adds `--gres=gpu:1`.

For external backends, expose a lightweight capabilities command:

```r
options(
  dshpc.executor_backend = "external",
  dshpc.backend_capabilities_cmd = "/usr/local/bin/dshpc-capabilities",
  dshpc.backend_gpu_count = "auto",
  dshpc.backend_request_optional_gpus = "auto"
)
```

The command may print JSON:

```json
{"gpus": 2}
```

or key-value lines:

```text
gpus=2
gpu_memory_mb=81920
```

The generated external/container environment includes:

```text
DSHPC_GPUS_REQUIRED
DSHPC_GPUS_OPTIONAL
DSHPC_GPUS_REQUESTED
DSHPC_BACKEND_GPU_COUNT
DSHPC_BACKEND_GPU_SOURCE
```

Docker/Podman scripts use `--gpus` when GPU is required, and for optional GPU
only when a GPU appears available on the backend host or the site wrapper sets
`DSHPC_FORCE_CONTAINER_GPU=1`. Apptainer/Singularity scripts use `--nv`.

## DataSHIELD methods

Aggregate methods:

- `hpcStatusDS(job_id_or_symbol)`
- `hpcResultDS(job_id_or_symbol)`
- `hpcLogsDS(job_id_or_symbol, last_n = 50)`
- `hpcListDS(label = NULL)`
- `hpcOutputsDS(job_id_or_symbol)`
- `hpcCapabilitiesDS()`
- `hpcSchedulerStatusDS()`

Assign method:

- `hpcSubmitDS(spec_encoded)`

## Client commands

Researchers and domain packages use `dsHPCClient` against the same control
plane regardless of whether execution is embedded, cell-shared, Slurm-backed, or
delegated to an external HPC wrapper:

```r
dsHPCClient::ds.hpc.list(conns, label = "dsImaging")
dsHPCClient::ds.hpc.summary(conns, label = "dsImaging")
dsHPCClient::ds.hpc.status(conns, job_id)
dsHPCClient::ds.hpc.wait(conns, job_id, timeout = 3600, poll_interval = 10)
dsHPCClient::ds.hpc.logs(conns, job_id, last_n = 100)
dsHPCClient::ds.hpc.outputs(conns, job_id)
dsHPCClient::ds.hpc.result(conns, job_id)
dsHPCClient::ds.hpc.load_output(conns, job_id, "features", symbol = "rad")
dsHPCClient::ds.hpc.capabilities(conns)
dsHPCClient::ds.hpc.scheduler_status(conns)

# Admin-only, enabled by setting dshpc.admin_key on the server or
# DSHPC_ADMIN_KEY in the Rock/HPC environment.
dsHPCClient::ds.hpc.admin.list(conns, admin_key, label = "dsImaging")
dsHPCClient::ds.hpc.admin.cancel(conns, job_id, admin_key)
```

Domain clients can wrap these for domain-specific labels or generation state.
For example, `dsImagingClient::ds.imaging.jobs(conns)` lists imaging jobs, while
`dsImagingClient::ds.imaging.radiomics.collection_status(conns, generation_id)`
reports collection-level progress for a fire-and-forget imaging generation.

Server-side package API:

- `register_dshpc_publisher(kind, fn)`
- `register_dshpc_runner(config, name = NULL, overwrite = TRUE)`
- `query_jobs_by_tag(tag_pattern, states = NULL)`
- `query_failed_jobs(tag_pattern)`
- `get_job_output_ref(job_id_or_symbol, output_name, required_label = NULL)`
- `count_active_jobs(tag_pattern)`
- `get_owner_id()`

## dsImaging integration

`dsImaging` registers runners into `DSHPC_HOME/runners` on load. It can keep
using embedded local Python environments, or it can declare container images
through options such as:

```r
options(
  dsimaging.container_images = list(
    pyradiomics_extract = "ghcr.io/isglobal-brge/dsimaging-runner@sha256:...",
    lungmask_infer = "ghcr.io/isglobal-brge/dsimaging-lungmask@sha256:..."
  ),
  dsimaging.container_runtime = "auto",
  dsimaging.container_pull = "missing"
)
```

dsImaging does not need to know whether the job runs embedded, through Slurm,
or through an external HPC gateway. It declares the runner contract and resource
needs; dsHPC handles orchestration.

## Validation

Run package tests:

```sh
R --quiet -e 'pkgload::load_all("dsHPC", quiet=TRUE); testthat::test_dir("dsHPC/tests/testthat", reporter="summary")'
```

Run a package check:

```sh
R CMD check --no-manual --no-build-vignettes dsHPC
```

Optional Docker integration tests require a local `alpine:latest` image and:

```sh
DSHPC_RUN_DOCKER_TESTS=1 R --quiet -e 'pkgload::load_all("dsHPC", quiet=TRUE); testthat::test_dir("dsHPC/tests/testthat", reporter="summary")'
```
