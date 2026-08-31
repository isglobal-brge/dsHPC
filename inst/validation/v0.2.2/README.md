# Executed validation evidence, dsHPC v0.2.2

- `transcript_lifecycle_fix.txt` — verbatim recorded execution of the full
  analyst-facing job lifecycle over a DSLite site on the embedded backend:
  submit, PENDING→RUNNING→FINISHED transitions, byte-identical resubmission
  deduplicated against the finished job (durable `deduplicated` event),
  worker SIGKILL followed by leader takeover and step requeue (retries=1),
  durability across logout, output listing/loading, log retrieval and the
  symbol→job-id resolution helper.
- `transcript_lifecycle.txt` — the corresponding execution recorded at
  v0.2.1 with the pre-0.3.2 client.
- `tests_dsHPC-022_docker.csv` — per-block results of the full test suite
  with the Docker-gated external-HPC-unit smoke enabled (351 expectations,
  0 failures): a real container acting as the external unit, including
  submit/status/cancel scripts, backend-vs-local path mapping and
  capability-driven GPU request propagation. Slurm and Kubernetes
  delegation are covered by contract tests (fake sbatch/kubectl executing
  the real step scripts, asserting partition/account/qos flags, manifest
  structure, reaping and cancel semantics, and the sacct-missing
  exit-code-file fallback).
