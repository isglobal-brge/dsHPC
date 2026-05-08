# dsJobs

dsJobs is the durable job runtime for DataSHIELD server packages. It lets a
package submit allowlisted work, keep state outside the interactive session,
throttle resource-heavy jobs, collect outputs, and publish derived artifacts
without exposing raw data to the client.

The package is intended to be installed on each Rock/DataSHIELD server. Domain
packages such as `dsRadiomics` register their runners and publishers; dsJobs
owns scheduling, retries, worker state, logs, result metadata, and optional
delegation to HPC backends.

## Execution model

dsJobs splits work into two planes:

- `session`: short DataSHIELD work that runs inline in the server session.
- `artifact`: heavier allowlisted runner work that is queued, isolated, and
  executed by the dsJobs worker or by a delegated backend.

Jobs are persisted in SQLite under `dsjobs.home` and survive session restarts.
The default scheduler is adaptive: it reads cgroup/host CPU and memory, detects
local GPU visibility where available, leases resources while jobs run, and puts
heavy runners into cooldown after OOM-like exits.

## Installation

Install the package on the DataSHIELD server and publish the DataSHIELD methods
as usual for the deployment:

```r
install.packages("dsJobs_0.1.0.tar.gz", repos = NULL, type = "source")
```

On load, dsJobs creates the default state tree if needed:

```text
/srv/dsjobs/
  artifacts/
  publish/
  runners/
  staging/
```

The configure/on-load path is intentionally defensive because Opal/Rock package
installs may not always run source configure scripts in the same way as a local
`R CMD INSTALL`.

## Basic options

Configure dsJobs with R options on the server. Site-wide defaults can use either
`dsjobs.<name>` or `default.dsjobs.<name>`.

```r
options(
  dsjobs.home = "/srv/dsjobs",
  dsjobs.scheduler = "adaptive",
  dsjobs.node_memory_mb = "auto",
  dsjobs.memory_reserve_mb = 2048,
  dsjobs.cpu_slots = "auto",
  dsjobs.gpu_count = "auto",
  dsjobs.max_jobs_global = 1000000,
  dsjobs.max_jobs_per_user = Inf
)
```

Multiple Rock R sessions sharing the same `dsjobs.home` participate in the same
cell. Leader election and SQLite state keep queue ownership singleton-like for
that shared cell, while allowing more than one Rock/session to see status.

## Backends

### Embedded

Embedded is the default. Artifact runners execute as local child processes,
with dsJobs enforcing local CPU, memory, GPU, runner concurrency, retries, and
cooldowns:

```r
options(dsjobs.executor_backend = "embedded")
```

This is the batteries-included mode for ordinary DataSHIELD deployments.

### Slurm

Slurm mode keeps dsJobs as the DataSHIELD control plane, but delegates
CPU/RAM/GPU scheduling to Slurm:

```r
options(
  dsjobs.executor_backend = "slurm",
  dsjobs.slurm_sbatch = "sbatch",
  dsjobs.slurm_squeue = "squeue",
  dsjobs.slurm_sacct = "sacct",
  dsjobs.slurm_scancel = "scancel",
  dsjobs.slurm_partition = "gpu"
)
```

Runner resource declarations become `sbatch` flags such as `--mem`,
`--cpus-per-task`, and, when GPU is requested, `--gres=gpu:N`.

### External

External mode is for site-specific HPC gateways. dsJobs prepares the step script
and passes paths/resources to admin-controlled wrappers:

```r
options(
  dsjobs.executor_backend = "external",
  dsjobs.external_submit_cmd = "/usr/local/bin/dsjobs-submit",
  dsjobs.external_status_cmd = "/usr/local/bin/dsjobs-status",
  dsjobs.external_cancel_cmd = "/usr/local/bin/dsjobs-cancel"
)
```

The submit wrapper receives environment variables including:

```text
DSJOBS_JOB_ID
DSJOBS_STEP_INDEX
DSJOBS_RUNNER
DSJOBS_STEP_SCRIPT
DSJOBS_OUTPUT_DIR
DSJOBS_LOCAL_STEP_SCRIPT
DSJOBS_LOCAL_OUTPUT_DIR
DSJOBS_MEMORY_MB
DSJOBS_CPU_SLOTS
DSJOBS_GPUS_REQUESTED
```

The wrapper should return a backend job id on stdout. The status wrapper should
return one of `RUNNING`, `PENDING`, `SUCCEEDED`, `FAILED`, or `CANCELLED`,
optionally followed by an exit code.

## Path mappings

If the Rock path differs from the backend path, configure a mapping:

```r
options(
  dsjobs.backend_path_mappings = c(
    "/srv/dsjobs" = "/mnt/hpc/dsjobs"
  )
)
```

Generated backend scripts use the backend-visible path, while dsJobs keeps local
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
  - /srv/dsradiomics/python/dsradiomics_extract.py
  - --input
  - "{input_dir}"
  - --output
  - "{output_dir}"
container:
  image: ghcr.io/isglobal-brge/dsradiomics-runner@sha256:...
  runtime: auto
  pull: missing
  command: python
  args_template:
    - -m
    - dsradiomics_extract
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
  dsjobs.container_runtime = "auto",
  dsjobs.container_pull = "missing",
  dsjobs.container_network = "none"
)
```

Supported runtimes are Docker, Podman, Apptainer, and Singularity.

## Backend GPU detection

Backend GPU handling is independent of Rock-local GPU detection. A Rock may have
no GPU while the HPC backend does.

For explicit backend capacity:

```r
options(
  dsjobs.backend_gpu_count = 1,
  dsjobs.backend_request_optional_gpus = "auto"
)
```

For Slurm auto-detection:

```r
options(
  dsjobs.executor_backend = "slurm",
  dsjobs.slurm_sinfo = "sinfo",
  dsjobs.backend_gpu_count = "auto",
  dsjobs.backend_request_optional_gpus = "auto"
)
```

If a runner declares `optional_gpus: 1` and Slurm reports GPU capacity, dsJobs
adds `--gres=gpu:1`.

For external backends, expose a lightweight capabilities command:

```r
options(
  dsjobs.executor_backend = "external",
  dsjobs.backend_capabilities_cmd = "/usr/local/bin/dsjobs-capabilities",
  dsjobs.backend_gpu_count = "auto",
  dsjobs.backend_request_optional_gpus = "auto"
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
DSJOBS_GPUS_REQUIRED
DSJOBS_GPUS_OPTIONAL
DSJOBS_GPUS_REQUESTED
DSJOBS_BACKEND_GPU_COUNT
DSJOBS_BACKEND_GPU_SOURCE
```

Docker/Podman scripts use `--gpus` when GPU is required, and for optional GPU
only when a GPU appears available on the backend host or the site wrapper sets
`DSJOBS_FORCE_CONTAINER_GPU=1`. Apptainer/Singularity scripts use `--nv`.

## DataSHIELD methods

Aggregate methods:

- `jobStatusDS(job_id_or_symbol)`
- `jobResultDS(job_id_or_symbol)`
- `jobLogsDS(job_id_or_symbol, last_n = 50)`
- `jobListDS(label = NULL)`
- `jobOutputsDS(job_id_or_symbol)`
- `jobCapabilitiesDS()`
- `jobSchedulerStatusDS()`

Assign method:

- `jobSubmitDS(spec_encoded)`

Server-side package API:

- `register_dsjobs_publisher(kind, fn)`
- `query_jobs_by_tag(tag_pattern, states = NULL)`
- `query_failed_jobs(tag_pattern)`
- `get_job_output_ref(job_id_or_symbol, output_name, required_label = NULL)`
- `count_active_jobs(tag_pattern)`
- `get_owner_id()`

## dsRadiomics integration

`dsRadiomics` registers runners into `DSJOBS_HOME/runners` on load. It can keep
using embedded local Python environments, or it can declare container images
through options such as:

```r
options(
  dsradiomics.container_images = list(
    pyradiomics_extract = "ghcr.io/isglobal-brge/dsradiomics-runner@sha256:...",
    lungmask_infer = "ghcr.io/isglobal-brge/dsradiomics-lungmask@sha256:..."
  ),
  dsradiomics.container_runtime = "auto",
  dsradiomics.container_pull = "missing"
)
```

dsRadiomics does not need to know whether the job runs embedded, through Slurm,
or through an external HPC gateway. It declares the runner contract and resource
needs; dsJobs handles orchestration.

## Validation

Run package tests:

```sh
R --quiet -e 'pkgload::load_all("dsJobs", quiet=TRUE); testthat::test_dir("dsJobs/tests/testthat", reporter="summary")'
```

Run a package check:

```sh
R CMD check --no-manual --no-build-vignettes dsJobs
```

Optional Docker integration tests require a local `alpine:latest` image and:

```sh
DSJOBS_RUN_DOCKER_TESTS=1 R --quiet -e 'pkgload::load_all("dsJobs", quiet=TRUE); testthat::test_dir("dsJobs/tests/testthat", reporter="summary")'
```
