# Concurrent durable DAG execution (0.4.0)

Design recorded before implementation, 2026-09-23.

## Scope and execution mechanism

Compiled DAG jobs use a ready set; ordinary `steps` jobs retain their existing
cursor and sequential execution. Dependencies are the compiled explicit input
references. A root has no implicit previous-step input. A node becomes ready only
when every referenced step is durably `done`. Session operations remain brief,
synchronous operations in the owning R process; independent artifact runners and
containers overlap. `dshpc.max_parallel_steps` is an administrator-only positive
integer, default 2; 1 gives strictly sequential DAG dispatch in topological order.

Retain the asynchronous artifact launcher and worker polling model. DAGs use
processx handles for detached Unix wrappers with durable exit files; handles
are only an optimization, never the source of truth. Linear launch is unchanged.
Creating another worker pool or a process per scheduler would duplicate the
existing leader and recovery contracts. Each poll reconciles all running DAG
steps, then fills free ready-set slots. Completion of a step can immediately
unlock downstream work without waiting for unrelated branches. Reentrant
session/cache completion cannot dispatch a node twice.

## Durable state and migration

SQLite `steps` is authoritative for readiness, running attempts, completion,
per-step retries, process identity, and external backend identity. Add per-step
PID and attempt/retry bookkeeping and `steps.resource_json` allocations in
schema version 3. Existing `resource_leases` rows project their per-job sum.
The additive migration from the v0.3.0 schema (user_version 2) copies the old
active job PID/retry state to its current step before changing DAG progress.
For DAGs, `jobs.step_index` is a count of successful nodes (not an active node
address); `total_steps` remains the fixed node count. Legacy linear jobs retain
the old cursor semantics and job PID for compatibility. No spec, capability,
tracking root, output classification, or runtime identity is rewritten.

Claims and resource reservations use SQLite's writer lock. The worker leader
lease remains the dispatch authority; check it before claims and launches
so an expired leader cannot continue dispatching behind its replacement.
Per-step launch markers and a launch handshake close the PID-persistence gap:
a runner starts only after its PID is durably recorded, and a replacement worker
can adopt or retire an interrupted launch. Do not erase an attempt's files or
redispatch while its wrapper or child is still alive. A processx stdin pipe gates
the runner until the step PID commits; worker death closes an unacknowledged
gate. A worker SIGKILL leaves
runners discoverable from durable state. Missing processes are re-dispatched
within their individual retry allowance; completed branches are never rerun.
Recovery is at-least-once, as in the existing durable artifact contract; runners
must make external side effects idempotent. External submission retains its
existing unavoidable submit/acknowledgement ambiguity and durable backend marker.

A permanent failure records the first observed error durably, stops fresh
ready-node dispatch, and leaves the job `RUNNING` until all already in-flight
branches reach terminal states. Interrupted branches still reconcile/retry as
needed to settle their attempts. Only then does the job become `FAILED` and
release remaining leases. Retryable failures consume a per-step retry budget;
`jobs.retry_count` remains an operator aggregate. Cancellation and timeouts
operate on every applicable running step and preserve remote cancellation
acknowledgement. Resume/restart derives work from step rows, not the progress
counter. Final output registration and step completion commit together; result
construction is recoverable if the worker dies after the last step commits.

## Resource and backend contracts

DAG admission counts one job against the job quota. Each ready artifact step
claims its own CPU, memory, runner/group concurrency, and GPU budget; active DAG
steps are added to the existing reservations for linear jobs. Lease release is
per step. GPU assignments are passed only to the owning step. This avoids both
oversubscription and multiplying the resource demand of a sequential chain.

Embedded local and container steps overlap. Slurm and Kubernetes accept multiple
independent step submissions and reconcile each backend id. Generic external
units also use the existing per-step submit/status/cancel contract (job id plus
step index and isolated step directory); submissions are issued serially by the
leader but the accepted remote work overlaps. Wrappers must return promptly and
provide an independent external id per step. A wrapper/unit that cannot meet
that contract must be configured with administrator step parallelism 1 (and its
existing runner-concurrency enforcement where the restriction spans jobs).

## Cache, output, and disclosure invariants

Cache eligibility, hashes, visibility/domain restrictions, copying, capability
checks, runtime seals, output confinement, and publication classifications stay
unchanged. Cache waiting becomes a per-step condition for DAGs: it must not
PENDING the entire job or release sibling reservations. Cache application and
output registration finish before dependants become ready.

No new shared fields or rows are added. Shared tracking continues to expose
exactly one neutral logical root, `queued`/`running`/`terminal`, the terminal
flag, and the closed kind. Step counts, parallelism, retries, process/backend
identities, timings and intermediate errors remain private/operator state.
The existing residual coarse-transition timing signal remains documented in
DISCLOSURE_CONTROL.md; concurrency does not add a timing or progress API.

## Apptainer/Singularity network default

For `container_network = "none"`, build `--containall --net --network none`.
Preserve the admitted runner environment explicitly through containment. Network
isolation requires Linux namespace support and either permitted unprivileged
user namespaces or an administrator-approved setuid runtime configuration.
The generated command must fail closed on missing support, refused flags, or a
runtime that silently ignores isolation. Verify at the execution host, before
executing runner code, that the container has a distinct network namespace and
only loopback interfaces; refuse conflicting extra arguments. Never fall back
to an unisolated invocation. Explicit operator network opt-out remains possible.
Docker/Podman continue to emit `--network none` by default.

## Verification and delivery

Add real sleeping-runner overlap and sequential timing tests; fork/join staging;
failure draining; two-running-step SIGKILL and fresh-connection recovery;
per-step retry/resource/GPU/cache behavior; upgraded v0.3.0 database recovery;
and unchanged shared tracking/capability assertions. Exercise external contracts
and container command builders, including fail-closed namespace verification.
Run existing and new server tests and the unchanged client suite from clean
checkouts, plus R CMD check. Record exact tested commits, platform/R version,
per-block test counts, timing evidence, skips and check outcomes under
inst/validation/v0.4.0/. No historical demonstration is rerun. Bump only dsHPC
to 0.4.0, commit locally, and neither tag nor push.
