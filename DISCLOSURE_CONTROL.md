# dsHPC disclosure-control contract

Reviewed: 2026-09-03. This document describes the analyst-facing DataSHIELD
boundary. It is a release invariant, not a claim that every package calling the
trusted server API is automatically disclosure-safe.

## Public analyst surface

`DESCRIPTION` registers only capability-protected status/result/output/log
methods, a minimal capability probe, and explicitly authenticated administrator
operations. There is no generic DataSHIELD assign method for submitting an
arbitrary job or loading an arbitrary output.

The legacy names `hpcSubmitDS`, `hpcLoadOutputDS`, `hpcListDS`, `hpcStudioDS`,
and `hpcSchedulerStatusDS` remain exported solely so an old persisted server
allowlist fails closed after upgrade. Their implementations never perform the
legacy operation.

Upgrading the package does not itself rewrite an allowlist persisted by
Opal/Rock. Before dsHPC 0.2.3, its package metadata also registered
`c=base::c` and `list=base::list` as aggregate methods. Those aliases cannot be
disabled by a dsHPC compatibility stub because they resolve directly to base R
functions and can return an assigned object unchanged. Operators **must remove
both aggregate entries** (and resynchronise the package methods) during the
upgrade. A deployment is not compliant with this contract until its effective
allowlist has been checked and contains neither alias.

Persisted jobs created before dsHPC 0.2.3 lack an `access_token_hash`. The
upgrade intentionally does not mint capabilities for them: they fail closed
and cannot be accessed through the analyst job APIs after the upgrade.
Operators must drain those jobs before upgrading or resubmit them afterwards.

## Enforced invariants

1. A job identifier is not an authorization credential. Every job receives an
   independently generated capability containing 244 random bits; only its
   SHA-256 digest is stored in the database. All analyst job reads require the
   capability. A symbol is resolved only from the active call/session stack;
   the process-global workspace is never consulted.
   Public methods also reject compound argument expressions before forcing
   them, so an evaluator that checks only the outer allowlisted call cannot use
   a nested call to invoke a server-only API as a side effect.
2. Only the explicit `hpcJobReferenceDS` operation exports a portable bearer.
   Routine status and result payloads do not contain it. `hpcStatusDS` returns
   only coarse state, terminal flag, and a generic error marker. Step number,
   retry count, labels, timestamps, worker identity, backend state, and paths
   remain server-side.
3. Runner stdout/stderr never crosses the analyst API. `hpcLogsDS` authorizes
   the job and returns an empty vector.
4. Result values cross the boundary only when the output is explicitly marked
   `safe_for_client`, has an allowed aggregate/summary kind, and has defensible
   cardinality at or above `nfilter.subset`. Arbitrary list/map values fail
   closed; the built-in count-only summary is admitted through a closed field
   schema. Output names for all other artifacts are not discoverable.
5. Counts below `nfilter.subset`, including zero, are suppressed. Larger
   operational counts produced by the built-in summary step are bucketed.
6. Private jobs do not reuse another job's cache or deduplicated outputs.
   Cross-job reuse is limited to jobs that are both explicitly `global` and
   carry the exact same domain label. Reused outputs are copied into the new
   job's own artifact tree rather than retaining paths into the source job.
7. Low-level submission, output loading, enumeration, studio, and scheduler
   APIs are server-to-server functions and are absent from DataSHIELD method
   registration. They additionally require a caller in a DataSHIELD domain
   package namespace matching the job label (for example, `dsImaging` may use
   `dsImaging` and `dsImaging_image`). Domain packages must authorize a
   dataset/workflow and build a fixed job specification before using them.
8. Every registered output and step reference must resolve under
   `dshpc.home/artifacts/<its-job-id>/`. Artifact trees containing symbolic
   links are rejected at runner finalization and revalidated before cache,
   deduplication, loading, result construction, or publication.
9. Managed runner/publisher registrations record a package owner, deny
   cross-package replacement, and may execute only for that package's label
   family. A missing runner `allowed_params` list permits no parameters, and
   installed runners take precedence over the writable registry.

## Trust boundary and residual signals

- The B64 job bearer is intentionally transferable: possession grants access
  to that one job's already controlled public surface. It is returned only by
  an explicit reference-export call. Clients must treat it as a secret and
  avoid logs or shared notebooks.
- Polling necessarily reveals whether a known job is pending, running, or in a
  terminal state. Exact progress and timestamps are withheld, but a caller can
  still observe elapsed wall time and success/failure. Domain APIs must use
  generic errors, bounded workflows, and submission quotas so this cannot be
  turned into an unrestricted data oracle.
- `*Internal` functions, the local database, artifacts, scheduler state, and
  administrator methods belong to the trusted node/operator boundary. A domain
  package that exposes their raw return values would violate this contract.
  In particular, an internal Parquet descriptor contains a node-local absolute
  path needed by its server consumer and must not be returned to an analyst.
- The worker and Rock process must run inside the trusted node boundary. If an
  artifact runner shares their Unix identity and can directly rewrite the
  SQLite database, package-level checks cannot defend that database; production
  runners should be isolated from the control database and given only their
  step input/output mounts. dsHPC clears inherited process/scheduler variables
  and confines `HOME`/temporary directories to the step, but this is not UID or
  mount isolation.
- YAML written directly into `dshpc.home/runners` is operator-managed rather
  than ownership-attested by the registration API. Protect that directory from
  runner writes. Managed definitions default to no overwrite; legacy unowned
  definitions can be claimed only by an identical managed registration or
  migrated explicitly by the operator.
- Resource leases use the maximum demand across all steps for the job lifetime,
  rather than changing per DAG step. Per-owner quotas default to compatibility
  settings, and dsHPC has no independently authenticated analyst identity.
  Deployments must configure finite quotas and domain packages must supply a
  stable, authorized owner; do not accept a client-selected `.owner` blindly.
- Administrator methods are disabled until an operator configures a key and
  should be reachable only through the operator-controlled DataSHIELD service.
