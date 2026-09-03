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
- `artifact`: heavier allowlisted runner work that is queued and executed in a
  minimized process environment by the dsHPC worker or a delegated backend.

Jobs are persisted in SQLite under `dshpc.home` and survive session restarts.
The default scheduler is adaptive: it reads cgroup/host CPU and memory, detects
local GPU visibility where available, leases resources while jobs run, and puts
heavy runners into cooldown after OOM-like exits.

Artifact steps are also content-addressed. For each deterministic runner step,
dsHPC hashes the resolved input contents, the canonical step definition and the
registered runner definition. If another job already contains an identical
completed step, dsHPC copies the cached output into the current job and records
a `step_cached` event instead of rerunning the runner. If an identical step is
already running, the duplicate job releases its resource lease, records a
`step_cache_wait` event and waits for the first step to finish before copying
the output. Whole-job deduplication by `spec_hash` remains in place for fully
identical submissions. Domain packages can opt out per invocation with
`cache = FALSE` or `cacheable = FALSE` for non-deterministic or effectful
runners.

Resource admission is conservative: a job leases the maximum CPU, memory, and
GPU requirement declared by any of its steps until the job finishes. Resources
are not currently resized between DAG steps. This can reduce utilization for
heterogeneous pipelines, but it avoids under-accounting at step transitions.

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
install.packages("dsHPC_0.2.4.tar.gz", repos = NULL, type = "source")
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
  dshpc.max_jobs_global = 8,
  dshpc.max_queued_jobs_global = 100,
  dshpc.max_jobs_per_user = 10,
  dshpc.default_timeout_secs = 86400
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

`allowed_params` is fail-closed: omitting it is equivalent to
`allowed_params: []`. Managed registration through `register_dshpc_runner()`
records the registering package, defaults to `overwrite = FALSE`, denies
cross-package replacement, and binds use to that package's job-label family.
Installed dsHPC runners cannot be shadowed by a same-named writable YAML file.
Site/domain packages that write YAML directly bypass managed ownership; those
files are operator-trusted configuration and their directory must not be
writable by an untrusted runner identity. Current dsImaging versions use the
distinct runner name `dsimaging_image_preprocess`, avoiding the installed
dsHPC `image_preprocess` runner.

Artifact commands receive only explicit runtime variables, runner-declared
non-reserved `env` entries, and dsHPC step variables. `HOME`, `TMPDIR`, `TMP`,
and `TEMP` point inside the current step directory. This is a breaking change
for runners that depended on arbitrary Rock/worker environment variables: add
each required value explicitly to the runner YAML `env` mapping. The minimized
environment is not an OS sandbox. A runner using the same UID and mounts can
still open sibling artifacts or the SQLite control database; production sites
must use a separate UID/container and expose only the required step mounts when
that code is not fully trusted.

The default per-owner quota is retained for compatibility, but production sites
should set a finite `dshpc.max_jobs_per_user`. dsHPC does not authenticate an
analyst identity itself: the domain package must authorize the request and pass
a stable owner value instead of forwarding an arbitrary client-controlled
owner.

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
- Interrupted whole-job deduplication clones remain in `CLONING` until a worker
  rebuilds their independently owned output tree from a completed job with the
  exact same global label and specification.
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
dsHPC also submits with `--export=NONE`; only the generated step contract is
reintroduced before the runner starts.

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

These options are server-side administrative configuration. For SSH gateways,
point them at an admin-controlled external wrapper; mount the SSH key and
`known_hosts` as server secrets, never as R options or job-spec values. The
wrapper must share or synchronize `DSHPC_STEP_SCRIPT` and `DSHPC_OUTPUT_DIR`
with the remote host, and submission must be idempotent for each
`DSHPC_JOB_ID` + `DSHPC_STEP_INDEX` pair.

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

The submit wrapper must return exactly one stdout line containing a 1--256 byte
backend job id matching `[A-Za-z0-9][A-Za-z0-9._:-]*`. The status wrapper must
return exactly one stdout line containing one of `RUNNING`, `PENDING`,
`SUCCEEDED`, `FAILED`, or `CANCELLED`, optionally followed by an exit code.
Wrapper diagnostics belong on stderr.

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

- `hpcJobReferenceDS(job_bearer_or_symbol)` (the explicit operation that
  exports a portable bearer)
- `hpcStatusDS(job_bearer_or_symbol)`
- `hpcResultDS(job_bearer_or_symbol)`
- `hpcLogsDS(job_bearer_or_symbol, last_n = 50)` (authorizes the job but does
  not return runner stdout/stderr to analysts)
- `hpcOutputsDS(job_bearer_or_symbol)`
- `hpcCapabilitiesDS()` (minimal public contract; no topology or load data)

Low-level server API (not registered for direct DataSHIELD calls):

- `hpcSubmitInternal(spec_encoded)`; the decoded spec must include a non-empty
  `label` identifying the server-side domain package that submitted the job.
- `hpcLoadOutputInternal(...)`; domain packages may load an output after
  applying their own domain contract. Descriptor mode may contain an absolute
  node path for server-side consumers and must never be relayed to a client.
- `hpcListInternal(...)`, `hpcStudioInternal(...)`, and
  `hpcSchedulerStatusInternal()` provide operational data to trusted
  server-side callers.

These functions must be called by a server-side DataSHIELD domain package.
dsHPC verifies that the caller namespace matches the domain label (including
domain-owned suffixes such as `dsImaging_image`); direct evaluation from a
DataSHIELD session or the global workspace fails closed.

The retired names `hpcSubmitDS`, `hpcLoadOutputDS`, `hpcListDS`, `hpcStudioDS`,
and `hpcSchedulerStatusDS` remain exported as compatibility stubs. They always
report that the method was retired, including when an older server allowlist
still names them. Domain methods such as those in `dsImaging` submit fixed
workflows on the server. The resulting server-session handle contains a
per-job capability; only `hpcJobReferenceDS()` returns a portable B64 bearer,
while a raw job ID is never a credential. Public status and result payloads do
not contain that bearer. Status contains only the coarse state, terminal flag,
and a generic failure marker; exact steps, retries, labels, and timestamps stay
on the node. A result value must additionally have defensible cardinality at or
above `nfilter.subset`; arbitrary lists fail closed except for the built-in
closed, count-only summary schema.

For upgrades from dsHPC 0.2.2 or earlier, resynchronise the DataSHIELD package
methods and inspect the effective server allowlist. Remove the historical
aggregate aliases `c=base::c` and `list=base::list`; unlike dsHPC's retired
function names, direct base-R aliases cannot be made fail-closed by a package
compatibility wrapper. The upgraded deployment is not disclosure-safe while
either persisted alias remains enabled.

Persisted jobs created before dsHPC 0.2.3 lack an `access_token_hash` and
therefore fail closed: they cannot be accessed through the analyst job APIs
after the upgrade. Operators must drain those jobs before upgrading or
resubmit them after upgrading.

The reviewed disclosure boundary and its residual assumptions are recorded in
[`DISCLOSURE_CONTROL.md`](DISCLOSURE_CONTROL.md).

## Client commands

Researchers and domain packages use `dsHPCClient` against the same control
plane regardless of whether execution is embedded, cell-shared, Slurm-backed, or
delegated to an external HPC wrapper:

```r
dsHPCClient::ds.hpc.status(conns, job_id)
dsHPCClient::ds.hpc.wait(conns, job_id, timeout = 3600, poll_interval = 10)
dsHPCClient::ds.hpc.logs(conns, job_id, last_n = 100)
dsHPCClient::ds.hpc.outputs(conns, job_id)
dsHPCClient::ds.hpc.result(conns, job_id)
dsHPCClient::ds.hpc.capabilities(conns)

# Admin-only, enabled by setting dshpc.admin_key on the server or
# DSHPC_ADMIN_KEY in the Rock/HPC environment.
dsHPCClient::ds.hpc.admin.list(conns, admin_key, label = "dsImaging")
dsHPCClient::ds.hpc.admin.cancel(conns, job_id, admin_key)
```

Domain clients wrap these for domain-specific state. For example,
`dsImagingClient` polls the opaque assigned workflow symbol rather than a raw
generation or job identifier.

Server-side package API:

- `hpcSubmitInternal(spec_encoded)`
- `hpcLoadOutputInternal(job_id_or_symbol, output_name,
  required_label = "domain-package")`
- `hpcListInternal(label = NULL)`
- `hpcStudioInternal(label = NULL)`
- `hpcSchedulerStatusInternal()`
- `register_dshpc_publisher(kind, fn, overwrite = FALSE)`
- `register_dshpc_runner(config, name = NULL, overwrite = FALSE)`
- `query_jobs_by_tag(tag_pattern, states = NULL)`
- `query_failed_jobs(tag_pattern)`
- `get_job_output_ref(job_id_or_symbol, output_name, required_label)`
- `count_active_jobs(tag_pattern)`
- `get_owner_id()`

Domain packages may compose specs as raw lists, or use the internal builder
helpers `dsHPC:::ds_job()`, `dsHPC:::ds_pipeline()`,
`dsHPC:::ds_pipeline_node()`, and `dsHPC:::ds_step_*()` from server-side R code.
All submitted jobs must carry a non-empty domain `label`.

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
