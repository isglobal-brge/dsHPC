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

Artifact steps are content-addressed only under an explicit immutable reuse
contract. For each eligible deterministic runner step, dsHPC hashes the trusted
domain's lowercase SHA-256 `reuse_fingerprint`, resolved input contents,
canonical step definition, execution unit, and registered runner definition.
An identical completed step can then be copied into the current job and records
a `step_cached` event; an identical running step is coalesced through
`step_cache_wait`. Whole-job deduplication similarly attaches eligible active
or completed shared submissions to one execution only when both the domain
`reuse_fingerprint` and an administrator-set `dshpc.runtime_revision` are
present. The fingerprint binds immutable external inputs; the runtime revision
must identify the exact container or lockfile and model-weight bundle behind
the selected execution unit. Package versions, runner definitions, unit
configuration, and this revision are sealed into the job and revalidated before
every step. Session operations with caller-visible effects
(`assign_*`, `aggregate`, and `publish_*`) are never whole-job reused because
those effects must run in every session. In the `scoped` compatibility policy,
eligible completed global work is copied into an independent job and bearer,
again only with this immutable fingerprint.
Domain packages can opt out per invocation with
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
install.packages("dsHPC_0.3.0.tar.gz", repos = NULL, type = "source")
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
  dshpc.queue_visibility = "shared",
  dshpc.scheduler = "adaptive",
  dshpc.runtime_revision = Sys.getenv("DSHPC_RUNTIME_REVISION"),
  dshpc.site_default_pool_id = "site-default",
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

`dshpc.runtime_revision` must be a lowercase 64-hex SHA-256 supplied by the
operator. Change it whenever the executable environment, container content, or
model weights change. Without it jobs still execute, but active/completed and
step-cache reuse is disabled; domain packages such as dsImaging that promise
reusable derivations fail closed until it is configured. A selectable external
unit may instead declare its own `runtime_revision` in the unit catalogue.

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

## Selectable execution units

No Resource is required for a normal installation: jobs use the configured
site default, which is embedded/local unless the operator selects another
backend. The effective default is sealed into every new job so later option
changes do not change work already queued.

To offer more than one unit, the server operator creates an admin-owned YAML
catalogue and sets `dshpc.units_file` (or `DSHPC_UNITS_FILE`). Start from
`inst/examples/units.yml`:

```yaml
schema_version: 1
units:
  cluster_a:
    type: external
    enabled: true
    resource_pool_id: shared_cluster
    allowed_labels: [dsImaging]
    allowed_runners: []
    config:
      external_submit_cmd: /usr/local/libexec/dshpc/cluster-a-submit
      external_status_cmd: /usr/local/libexec/dshpc/cluster-a-status
      external_cancel_cmd: /usr/local/libexec/dshpc/cluster-a-cancel
```

`resource_pool_id` makes aliases for the same physical cluster share one local
scheduler budget. Omit it when the unit has its own capacity. Catalogue command
paths must be absolute executable files. Keep the catalogue and wrappers
non-writable by analysts and runners.

The Resource is only an authorized selector. It never contains SSH keys,
tokens, cloud credentials, or executable arguments. Put credentials in a
server-managed wrapper, SSH agent, secret mount, or workload identity. Generic
`slurm_extra_args` and `container_extra_args` are deliberately not accepted in
durable unit snapshots; fixed site flags belong in the wrapper or the
admin-owned runner definition.

Use the Resource authorization supplied by the DataSHIELD deployment so only
operators can create or modify unit selectors and analysts can resolve only
the selectors granted to their project. This applies equally to Opal and
Armadillo. dsHPC validates the selected catalogue entry, label and runner set;
it does not treat a guessable Resource name as authorization. Do not expose the
physical site-default executor again as a selectable alias unless both paths
use the same pool identifier: set `dshpc.site_default_pool_id` and the alias's
`resource_pool_id` to the same value so they share one scheduler budget.

### Opal registration

When dsHPC is installed, `inst/resources/resource.js` supplies Opal's Resource
UI for Slurm, external and Kubernetes selectors. An external unit with ID
`cluster_a` produces this non-secret descriptor:

```text
dshpc+unit://external/cluster_a
```

### Armadillo registration

Armadillo stores Resources under `project/folder/name` and current servers
rewrite their backing URL while adding a short-lived access JWT. Therefore the
unit locator is carried in `format`, and the backing object is an inert marker:

```r
marker <- data.frame(selector = TRUE)
MolgenisArmadillo::armadillo.upload_table(
  project = "hpcunits", folder = "markers", table = marker,
  name = "unit_alpha_marker")

unit_alpha <- resourcer::newResource(
  name = "unit_alpha",
  url = paste0(
    armadillo_url,
    "/storage/projects/hpcunits/objects/",
    "markers%2Funit_alpha_marker.parquet"
  ),
  format = "dshpc-unit:external/unit_alpha"
)
MolgenisArmadillo::armadillo.upload_resource(
  project = "hpcunits", folder = "resources",
  resource = unit_alpha, name = "unit_alpha")
```

The marker contents and URL are never opened by dsHPC. The resolver derives
the selection only from the strict format locator, discards any transport JWT,
and retains a canonical credential-free Resource. `dsHPCClient` also removes
and verifies Armadillo's transient `R` and `rds` symbols. The resulting DSI
Resource name is `hpcunits/resources/unit_alpha`; Opal and Armadillo names are
both treated as opaque client-side identifiers.

Current Armadillo servers parse the backing marker URL more narrowly than the
storage API itself. Use only letters, digits, and underscores for the marker's
project, folder, and object name, as in the example above. This restriction
does not apply to the dsHPC `unit_id`, which travels in `format` and may also
contain dots and hyphens.

Changing, disabling, or removing a catalogue entry prevents its saved
selection from starting another job or pipeline step. Already launched remote
work retains the old non-secret snapshot so status and cancellation can still
be reconciled.

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
- Interrupted pre-0.3 whole-job deduplication clones remain recoverable from a
  completed job with the exact same global label and specification. New shared
  submissions attach to one canonical execution instead of creating clones.
- Slurm/external submissions write `external_backend.json` before updating the
  DB so a new worker can recover the backend job id and continue polling.
- Transient external status failures return `STATUS_UNKNOWN` and keep the job
  running instead of creating duplicate retries.
- Unreadable persisted specifications and invalid execution-unit snapshots are
  moved to `FAILED` transactionally and release stale scheduler leases instead
  of remaining indefinitely `PENDING` or `RUNNING`; transient database read
  errors are retried.
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

- `hpcTrackingListDS(limit = 100, cursor = NULL)` returns a bounded `root_v1`
  page containing one neutral row per logical workflow.
- `hpcTrackingStatusDS(tracking_id)` looks up a shared root from any session.
- `hpcTrackingResultDS(tracking_id)` returns only closed-schema,
  disclosure-validated client-safe results.
- `hpcTrackingOutputsDS(tracking_id)` lists at most the fixed aliases
  `output_001` (one reusable server object) and `output_002` (one closed
  count-only summary), without internal names, values, paths, sizes, child jobs,
  or providers.
- `hpcJobReferenceDS(job_bearer_or_symbol)` (the explicit operation that
  exports a portable bearer)
- `hpcStatusDS(job_bearer_or_symbol)`
- `hpcResultDS(job_bearer_or_symbol)`
- `hpcLogsDS(job_bearer_or_symbol, last_n = 50)` (authorizes the job but does
  not return runner stdout/stderr to analysts)
- `hpcOutputsDS(job_bearer_or_symbol)`
- `hpcCapabilitiesDS()` (minimal public contract; no topology or load data)

Assign methods:

- `hpcTrackingAssignOutputDS(tracking_id, output_name)` assigns an opaque
  `dshpc_output_reference` in the server R session. It never materializes or
  serializes the underlying artifact to the client.

Low-level server API (not registered for direct DataSHIELD calls):

- `hpcSubmitInternal(spec_encoded)`; the decoded spec must include a non-empty
  `label` identifying the server-side domain package that submitted the job.
  Active and completed reuse require a lowercase SHA-256 `reuse_fingerprint`
  plus an administrator-sealed runtime revision; an explicit
  `tracking_role = "primary"` always requires it.
- `hpcTrackingCreateInternal(reuse_key, kind = "analysis")` creates or joins
  one logical root; trusted imaging workflows use the neutral `"imaging"`
  category. Private fan-out jobs use
  `hpcSubmitInternal(..., tracking_id = root)` and a
  direct idempotent execution additionally uses `tracking_role = "primary"`.
- `hpcTrackingPublishOutputInternal()` and
  `hpcTrackingPublishReferenceInternal()` explicitly publish a neutral logical
  output on a root. The first attached role fixes a root as a direct primary or
  a child collection. A terminal primary with a reusable output seals safely on
  lookup; collections stay open between batches and are sealed explicitly with
  `hpcTrackingFinishInternal()` or a domain recovery method.
- `hpcTrackingResolveOutputInternal()` resolves an assigned opaque reference
  only inside trusted server code. No generic cardinality test is applied at
  this server-only boundary; any later client result is checked again by the
  consuming DataSHIELD package.
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
workflows on the server. Shared workflows use a public `trk_` tracking id as a
knowledge address; it is sufficient only for neutral status, already-approved
client results, and assignment of opaque reusable references. It cannot expose
private artifacts, runner logs, paths, child jobs, scheduler topology, or
control/cancel a job. Private and compatibility job reads remain capability
protected: only `hpcJobReferenceDS()` exports that B64 bearer, and a raw
`job_` id is never a private-data credential.

One tracking root represents one logical workflow even when a collection fans
out into thousands of per-image jobs. Children and retries never enter the
analyst list. Root states are only `queued`, `running`, and `terminal`; exact
progress, outcomes, labels, owners, retries, and timestamps remain internal.
Set `dshpc.queue_visibility = "scoped"` to disable all shared tracking methods;
completed global deduplication then retains its pre-0.3 copy-and-capability
semantics.

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
dsHPCClient::ds.hpc.list(conns, limit = 100) # page size; follows all pages
dsHPCClient::ds.hpc.studio(conns)
dsHPCClient::ds.hpc.status(conns, "trk_...")
dsHPCClient::ds.hpc.result(conns, "trk_...")
dsHPCClient::ds.hpc.load_output(
  conns, "trk_...", "output_001", symbol = "features_ref")

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
- `hpcTrackingCreateInternal(reuse_key = NULL, kind = "analysis")`
- `hpcTrackingFinishInternal(tracking_id, success = TRUE)`
- `hpcTrackingPublishOutputInternal(...)`
- `hpcTrackingPublishReferenceInternal(...)`
- `hpcTrackingResolveOutputInternal(...)`
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
