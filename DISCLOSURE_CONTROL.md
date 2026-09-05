# dsHPC disclosure-control contract

Reviewed: 2026-09-05. This document describes the analyst-facing DataSHIELD
boundary in dsHPC 0.3.0. It is a release invariant, not a claim that every
domain package consuming a server-side object is automatically disclosure-safe.

## Shared knowledge model

The normal node policy is `dshpc.queue_visibility = "shared"`. A public
`trk_` identifier addresses one logical workflow in the node's shared
knowledge catalogue. It is intentionally reusable by another authenticated
analyst and from a later DataSHIELD session.

Tracking and data are separate channels:

- Tracking exposes one neutral root row with only `tracking_id`, coarse state
  (`queued`, `running`, or `terminal`), terminal flag, and a kind from the
  closed neutral vocabulary `analysis` / `imaging`. Kind is a broad workflow
  category, never a caller-supplied collection or cohort label. Tracking omits
  names, labels, owners, exact progress, counts, timestamps, outcomes, retries,
  errors, events, scheduler topology, and execution identifiers.
- A logical collection root may own any number of private child jobs. Children,
  retries, and drip-feed batches are never enumerated, so the number of queue
  rows is not the number of images or patients.
- Client result values are returned only from the single optional public
  `output_002` summary. It must be classified `client_safe`, retain
  `safe_for_client = TRUE`, use kind `summary`, and pass the closed count-only
  schema and power-of-two/`nfilter.subset` validation.
- `server_reusable` outputs can be assigned only as an opaque
  `dshpc_output_reference`. The assign method contains no value, artifact path,
  provider reference, execution job id, or credential. A trusted server package
  may resolve it without a generic cardinality test because the value remains
  inside the server. Any later value crossing to a client must pass the
  consuming DataSHIELD package's disclosure control again.
- `internal_only` outputs, runner logs, filesystem paths, credentials, and
  secret-like output kinds are neither discoverable nor reusable through the
  shared analyst API.

Set `dshpc.queue_visibility = "scoped"` to disable the complete shared
tracking/result/assignment surface. Private and compatibility jobs continue to
use per-job capabilities in either policy. In `scoped`, completed global
deduplication retains its pre-0.3 copy-on-dedup semantics when an immutable
reuse fingerprint is present, so every reused job has an independent artifact
tree and capability.

## Public analyst surface

The shared API consists of:

- `hpcTrackingListDS(limit, cursor)`, returning a bounded, deterministic
  `root_v1` page without a total count or timestamp-bearing cursor;
- `hpcTrackingStatusDS(tracking_id)`;
- `hpcTrackingResultDS(tracking_id)`;
- `hpcTrackingOutputsDS(tracking_id)`; and
- assign method `hpcTrackingAssignOutputDS(tracking_id, output_name)`.

The capability-compatible API remains `hpcJobReferenceDS`, `hpcStatusDS`,
`hpcResultDS`, `hpcOutputsDS`, and `hpcLogsDS`. Possession of a capability
authorizes only that job's already-controlled surface. A raw `job_` id still
does not authorize status, data, logs, outputs, cancellation, or reference
export. A public tracking id does not authorize private job control either.

Legacy names `hpcSubmitDS`, `hpcLoadOutputDS`, `hpcListDS`, `hpcStudioDS`, and
`hpcSchedulerStatusDS` remain permanent fail-closed compatibility stubs. The
new Studio/client implementation must call the `hpcTracking*DS` protocol and
must never revive those names.

Upgrading a package does not itself rewrite an allowlist persisted by
Opal/Rock. Before dsHPC 0.2.3, metadata registered `c=base::c` and
`list=base::list` as aggregate methods. Operators **must remove both aliases**
and resynchronise package methods. A deployment is not compliant while either
alias remains in the effective allowlist.

Persisted jobs created before 0.2.3 have no `access_token_hash` and continue to
fail closed on the capability API. Existing global jobs are not backfilled as
tracking roots during the 0.3.0 migration: automatic backfill could turn a
historical per-image execution fan-out into a cardinality channel. New domain
submissions create roots explicitly or automatically.

## Enforced invariants

1. Tracking roots and execution jobs use separate tables with an explicit
   many-to-many relationship. A global direct submission automatically creates
   one implicit root. A domain collection creates one explicit root and
   attaches its execution jobs as hidden children.
2. Eligible, canonically identical global direct submissions in the same
   domain and execution-unit context can attach to the same active or completed
   execution only when the fixed specification carries a validated lowercase
   SHA-256 `reuse_fingerprint` and the selected runtime carries an
   administrator-set immutable `runtime_revision`. The former attests the
   external inputs; the latter must identify the exact container/lockfile and
   model-weight bundle. dsHPC also seals provider/package versions, resolved
   runner definitions, and the execution-unit snapshot. Without either seal, a
   direct submission creates a fresh root and execution. dsHPC does not guess
   whether an arbitrary string is a path or read outside admitted job inputs.
3. Explicit roots use a domain-provided reuse key that is hashed before durable
   storage. An open root and a successful sealed root with a published output
   are reusable. Failed roots and terminal roots without knowledge outputs are
   not reused, allowing a clean retry. The first attached execution fixes the
   root as either a single-primary workflow or a child collection; the roles
   cannot be mixed. Explicit primary submissions require the same immutable
   `reuse_fingerprint`. A single-primary root is sealed only after its primary is
   terminal and a reusable output exists. A collection remains open across
   batches until its domain explicitly seals or safely recovers it.
   A failed/incomplete primary remains open during the configured retention
   window so the domain can retry it; GC then seals it as failed before removing
   the expired private execution rows. Collection roots are never inferred
   complete merely because the current batch has no active children.
4. Root visibility is deployment policy, not client input. Child job
   visibility is independent: a shared root may contain entirely private
   executions. Changing a child label, name, tag, or visibility cannot make it
   appear as another root.
5. Output classification is closed to `internal_only`, `server_reusable`, and
   `client_safe`. Existing outputs migrate to `internal_only`; no legacy output
   is promoted automatically. Only trusted domain code can publish a chosen
   child output or opaque domain reference on a root. The public projection
   suppresses all internal names and cardinality: it exposes at most one
   `output_001`/`server_object` and one `output_002`/`summary` row.
6. Client-safe values are rebuilt from the current output registry. The
   `safe_for_client` flag alone is insufficient: the kind and value must pass
   the current allowlist and `nfilter.subset` checks. Arbitrary lists fail
   closed except for the built-in count-only summary schema. Reconstruction
   errors become the generic `Job result is unavailable.`
7. Server reuse never serializes the source value through a DataSHIELD
   aggregate. The assigned object is a safe locator; resolution revalidates the
   root, terminal success, classification, registry entry, and confined
   artifact path. Cross-package resolution is allowed for trusted `ds*`
   packages because shared knowledge is a node property, not user ownership.
8. Every registered output and step reference must resolve under
   `dshpc.home/artifacts/<job-id>/`. Symlinked paths or trees fail closed before
   caching, loading, result construction, reuse, or publication.
9. Shared tracking exposes no cancel method. Cancellation, raw logs, events,
   scheduler details, backend ids, PIDs, command paths, and the legacy
   operational Studio snapshot remain operator-only. Analyst dsHPC Studio is
   rebuilt solely from the safe root-tracking/result APIs. The old `hpcLogsDS`
   continues to return an empty vector after capability authorization.
10. Tracked knowledge is durable across worker and R-session restarts. Garbage
    collection may remove unreferenced execution children and failed/cancelled
    implicit execution rows after retention, while preserving their terminal
    root metadata. It retains successful implicit primaries and any job output
    referenced by a published root. An opaque domain reference can outlive all
    of its execution children. A stale hidden `CREATING` root with no attached
    job or output expires after the normal retention interval. Publication and
    database collection share the SQLite writer lock, so a newly published
    output cannot be deleted from a stale protection snapshot.

11. Whole-job reuse is disabled whenever a step declares `cache = FALSE` or
    `cacheable = FALSE`, and for session operations whose caller-visible effects
    must be reconstructed (`assign_*`, `aggregate`, and `publish_*`). Historical
    artifact step reuse is likewise limited to admitted global deterministic
    jobs carrying `reuse_fingerprint` and a sealed `runtime_revision`; both are
    part of the reuse contract alongside resolved input contents, runner
    definition, and execution unit.

## Platform identity feasibility

dsHPC does not treat `.owner` as an authenticated principal. Standard Opal
knows the principal and profile in its Java control plane, but Rock receives a
technical connection and profile options rather than a verifiable analyst
identity. Armadillo similarly uses its Spring principal for control-plane
auditing/workspaces without injecting a verifiable principal or permission set
into arbitrary R package methods.

Consequently, `mine` cannot be a package-level security boundary. Opal may
restrict the profile that contains the shared methods as an additional
deployment gate; the R implementation cannot inspect or reproduce that check,
and Armadillo profiles do not provide an equivalent portable boundary. Shared
versus scoped is therefore an operator policy for the whole dsHPC node.

## Trust boundary and residual signals

- A shared queue necessarily reveals the number of logical workflows visible
  in each page and lets a poller observe root appearance and coarse state
  transitions. It does not reveal execution fan-out. Deployments where even
  logical workflow existence is sensitive must use `scoped`.
- Internal output names are not public metadata. The shared channel maps them
  to fixed ordinal aliases and exposes no more than one reusable server object
  and one closed summary.
- A trusted domain package resolving a server-reusable patient-level object
  must preserve its provenance and expose only allowlisted disclosure-safe
  operations over it. dsHPC attaches provenance metadata, but package code can
  deliberately strip an R attribute; the installed server package/allowlist
  remains part of the trusted computing base.
- The B64 compatibility bearer remains intentionally transferable and must be
  treated as a secret. Shared tracking normally removes the need to export it.
- The worker and Rock process must stay inside the trusted node boundary.
  Package-level path checks do not provide UID or mount isolation from a runner
  that can directly rewrite SQLite or sibling artifacts. Production runners
  should receive only their step mounts and a separate identity/container.
- Runner and unit configuration are operator-managed. Canonical reuse is sound
  only if `runtime_revision` is changed whenever executable or model content
  changes; absent a valid revision, reuse fails closed. Protect writable
  registries and external wrappers from analyst and runner modification; keep
  SSH keys, API tokens, and cloud credentials in server-managed agents, secret
  mounts, or workload identity rather than Resource descriptors or job specs.
- Per-owner quotas are advisory unless a domain/platform supplies a stable
  authorized owner. Global queue/resource quotas remain enforceable without a
  per-user identity.
