# dsHPC 0.2.5

* Added backend-neutral DataSHIELD Resource selection for administrator-managed
  Slurm, external, and Kubernetes execution units. New jobs persist a sealed,
  credential-free unit snapshot; the local/site default is sealed as well.
* Added Resource contracts for both Opal and Armadillo. Armadillo transport JWTs
  are accepted only as transient platform metadata and are discarded before
  the Resource client or durable job retains the selector.
* Unit changes and revocation are checked before every new submission or step,
  while old snapshots remain usable for status and cancellation of work already
  launched. Command integrity is now independent of current file availability.
* Added explicit physical resource pools for unit aliases and included unit
  identity in scheduler and step-cache isolation.
* Durable unit snapshots reject generic Slurm/container extra arguments, which
  could otherwise persist credentials or privileged flags. Such configuration
  belongs in admin-controlled wrappers or runner definitions.
* Added a trusted, exact-label `hpcStatusInternal()` API for domain packages;
  it is not registered as an analyst-callable DataSHIELD method.
* Delegated scripts, runner admission, failure accounting, and scheduler
  budgets now consistently use the unit settings sealed into each job. Legacy
  and current site-default jobs also share one scheduler pool after upgrade.
* Selected units are revalidated against each domain label, and revocation is
  covered across queued and continuing jobs while completed status remains
  readable.
* Added `dshpc.site_default_pool_id` so a selectable alias of the physical
  default executor can share the same scheduler budget without double-counting
  capacity.
* Unit initialization now accepts only a platform-resolved Resource client;
  an in-session raw descriptor cannot bypass Opal or Armadillo Resource
  authorization.
* Workers now terminalize unreadable durable job specifications and invalid
  execution-unit snapshots instead of leaving jobs permanently pending or
  running. The transition is transactional, releases stale scheduler leases,
  and records a detail-free audit event; transient database failures remain
  retryable.

# dsHPC 0.2.4

* Queue admission now checks per-owner and global quotas inside the same
  immediate SQLite transaction that creates the job, preventing concurrent
  submissions from exceeding configured limits.
* Artifact retries remove and verify the exact failed attempt directory before
  resetting durable state, so stale outputs and scheduler markers cannot be
  mistaken for a new attempt.
* Remote cancellation and timeout are now two-phase operations. Jobs retain
  their lease and remain running until Slurm, Kubernetes, or an external
  wrapper confirms a terminal state; rejected cancellation never creates a
  duplicate retry.
* Slurm and external wrapper responses have strict, stderr-separated contracts;
  backend identifiers are bounded ASCII values, cancelled Slurm jobs cannot be
  interpreted as successful, and the default step timeout is enforced locally
  and translated to a Slurm time limit.
* Shared-cell leader liveness uses the persisted heartbeat for another node
  instead of testing a process id from the wrong PID namespace.
* Generated artifact scripts use portable POSIX `sh`, including minimal Alpine
  executor images. The Docker HPC-unit and complete package checks cover the
  resulting submit, poll, output, retry, cancellation, and recovery paths.

# dsHPC 0.2.3

* Retired generic DataSHIELD submission, loading, enumeration, studio, and
  scheduler methods. Persisted entries for those dsHPC functions now fail
  closed; trusted domain packages use non-registered `*Internal` functions
  instead. Upgrades must also remove the historical `c=base::c` and
  `list=base::list` aggregate aliases from the effective server allowlist,
  because direct base-R aliases cannot be retired by dsHPC code.
* Added per-job capabilities containing 244 random bits; only their SHA-256
  digests are stored. Analyst status, result, output, and log calls reject raw
  job identifiers and expose only capability-authorized, disclosure-controlled
  data. Persisted pre-0.2.3 jobs lack a capability hash and therefore fail
  closed after upgrade; operators must drain them before upgrading or resubmit
  them afterwards. Job-symbol resolution no longer consults the process-global
  workspace, preventing a private handle there from crossing DataSHIELD
  sessions in a shared R process.
* Added the explicit `hpcJobReferenceDS()` endpoint for exporting a portable
  bearer. Routine status and result payloads no longer contain that credential,
  and result-reconstruction failures are mapped to a path-free public error.
* Reduced public status to coarse state/completion, removed runner logs and
  exact operational progress, and rebuilt results exclusively from outputs
  explicitly marked client-safe with an allowed kind. Marked-safe values now
  also pass the same configured cardinality floor used by internal loading;
  arbitrary list summaries fail closed, while the built-in count-only summary
  has a closed schema.
* Runner parameters now fail closed when `allowed_params` is absent. Managed
  runner and publisher registrations default to no overwrite, record their
  registering package, reject cross-package replacement, and enforce that
  package against the job label. Installed dsHPC runners take precedence over
  writable registry files, preventing built-in name shadowing. Deployments
  using dsImaging's directly written `image_preprocess` definition must rename
  that domain runner (and its workflow references) because dsHPC already ships
  a built-in with that name.
* **Runner migration:** artifact subprocesses no longer inherit the Rock,
  worker, or scheduler environment. They receive a minimal explicit
  environment with per-step `HOME`/temporary directories; Slurm uses
  `--export=NONE`. Runner definitions that relied on inherited variables must
  declare the required non-reserved values in their YAML `env` mapping.
* Disabled private cross-job cache/deduplication, enforced output cardinality
  before internal assignment, parsed CSV records rather than counting physical
  lines, rejected non-tabular references with unknown cardinality, and
  suppressed all counts below the configured DataSHIELD subset threshold,
  including zero.
* Added a runtime boundary for DataSHIELD evaluators that validate only the
  outer call: public methods accept literal/symbol arguments without evaluating
  nested calls, while server-only APIs require a matching domain package
  namespace in the call stack.
* Confined output references to their exact job artifact tree, rejected
  symbolic links throughout runner outputs, cache inputs, deduplicated jobs,
  and publisher inputs, and copied reused outputs into a job-owned tree.
* Workers now recover interrupted whole-job clones after restart, rebuilding
  their independently owned output trees from an exact-label, exact-spec
  completed source and failing closed when no valid source remains.
* Hardened upgrades of existing storage: package load performs a symlink-safe,
  once-per-version permission pass (`0770` directories, `0660` files), and the
  supplied systemd unit now creates worker logs under `UMask=0007`.
* Removed dsHPC's cross-package call to `dsImaging` generation cleanup. Imaging
  generation expiry must account for domain lifecycle state and no longer runs
  from the generic job garbage collector.
* Preserved artifact sizes above 2 GiB as validated SQLite integers/R doubles
  (exact through 2^53) while continuing to suppress sizes from public payloads.
* Resource leases remain conservatively scoped to the maximum requirement of
  any step for the whole job. Sites must configure finite quotas and domain
  packages must provide a stable authorized owner; per-step leasing and a
  runtime-authenticated analyst identity remain deployment/policy concerns.

# dsHPC 0.2.2

* Corrected the `hpcLoadOutputDS` documentation to describe its actual
  semantics: it is a guarded DataSHIELD `AssignMethods` entry. Domain
  server-side packages are the primary intended callers; deployments may
  allowlist it for direct analyst use, with the same guards in either case
  (mandatory `required_label` ownership check, terminal FINISHED/PUBLISHED
  state check, `nfilter.subset` disclosure floor on tabular outputs). The
  previous roxygen block contained two merged doc blocks with a stale
  "not directly callable by users" claim and duplicated parameter entries.
* Added contract-test coverage for slurm submission flags
  `--partition`/`--account`/`--qos` (options `dshpc.slurm_partition`/
  `dshpc.slurm_account`/`dshpc.slurm_qos`) and for slurm status reaping
  when `sacct` is missing: after an empty `squeue` probe, state is taken
  from the step directory's local `exit_code` file, or reported as
  unknown/still running when that file is absent.
* No code behavior changes.
