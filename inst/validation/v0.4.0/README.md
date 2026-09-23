# Executed validation evidence, dsHPC v0.4.0

Executed 2026-09-23 on macOS Sequoia 15.4.1, Darwin 24.4.0, arm64.
R version 4.5.2 (2025-10-31), platform `aarch64-apple-darwin20`.

Exact tested source commits:

- dsHPC 0.4.0: `41f5e4a2fee8d9a5c16dd3b1b545c7fc3bb6f32e`.
- dsHPCClient 0.4.0, unchanged: `f2e195b9c1615ccbe7f12ee9bb5e2a2dd68771ef`.
- The subsequent receipt commit adds validation evidence only; executable code,
  package metadata and tests are those of the tested server commit.

Both repositories were checked out into clean detached worktrees and verified
with `git status --porcelain`. Packages were built and installed into an isolated
library inside the server clone. The standalone suites used those installed
packages and the clean worktree tests. Final source tarballs were built from
`git archive` exports of the same exact commits, excluding worktree `.git`
metadata; full `R CMD check` then installed and tested each tarball independently.
No historical demonstration or thesis material was rerun or edited.

## Results

| Validation | Test blocks | Passed expectations | Failures | Errors | Warnings | Skipped blocks |
|---|---:|---:|---:|---:|---:|---:|
| Clean dsHPC suite | 250 | 1402 | 0 | 0 | 0 | 2 |
| Clean dsHPCClient suite | 77 | 325 | 0 | 0 | 0 | 0 |

`tests_dsHPC-040.csv` and `tests_dsHPCClient-040.csv` contain the per-block
results in the v0.2.2 receipt format. The server CSV has 1404 recorded
expectations including the two skip conditions; the client CSV has 325.

Both **full `R CMD check` runs passed: 0 errors, 0 warnings, 0 notes**. PDF manuals
were included and passed. The check-session test summaries are recorded
separately: server `PASS 1400 / FAIL 0 / WARN 0 / SKIP 2`; client
`PASS 325 / FAIL 0 / WARN 0 / SKIP 0`. The complete check logs and test transcripts
are included; counts above describe the separately recorded CSV suite runs.

The timing regression runs two independent shell runners sleeping 2 seconds
each. In the clean suite they took **2.104 seconds concurrently** and
**4.125 seconds sequentially**. The parallel assertion is below 3.5 seconds and
below 80% of the sequential duration; the sequential assertion is at least
4 seconds. The independent check run measured 2.100 and 4.158 seconds. See
`concurrency_timings.csv`.

New tests also verify named A/B-to-C input staging, immediate dependency
unlocking, first-error retention while siblings drain, per-step retry and GPU
admission, cache waiting/reuse, actual worker SIGKILL and lease takeover with
two live steps, two killed attempts without rerunning a completed sibling,
the pre-PID-commit launch crash, expired live daemon retirement with PID birth
checks, cancellation/submission races, v0.3.0 schema upgrade, and the unchanged
closed shared-tracking projection. All existing disclosure tests passed.

## Runtime coverage and upgrade constraints

Docker/Podman/Apptainer/Singularity command builders and executable isolation
guards are covered. External units and embedded container overlap are tested
with executable contract fixtures. Apptainer/Singularity use
`--containall --net --network none`; the runner executes only after a distinct
loopback-only namespace is verified. Refused or ignored isolation fails closed.

Two existing opt-in Docker integration tests were skipped: the Docker CLI is
present but its daemon is unavailable. Apptainer and Singularity are absent on
this macOS host, so native Linux namespace/setuid integration was not exercised.
Those deployment checks remain outstanding; no live runtime success is claimed.

The migration preserves existing runtime seals, including package versions.
Drain sealed v0.3.0 jobs before upgrading; starting a remaining sealed old node
under v0.4.0 correctly fails the unchanged runtime contract and requires
resubmission. Embedded durable DAG artifact execution requires Unix. External
submit/acknowledgement recovery retains its existing at-least-once contract.

## Reproduction

With clean checkouts at the commits above, create isolated library, temporary,
build and check directories inside a clone. Set `R_LIBS`, `TMPDIR` and
`DSHPC_HOME` to those directories, export each commit with `git archive`, then:

```sh
R CMD build source-server
R CMD build source-client
R CMD INSTALL --library="$R_LIBS" dsHPC_0.4.0.tar.gz
R CMD INSTALL --library="$R_LIBS" dsHPCClient_0.4.0.tar.gz
Rscript --vanilla run-suite.R clean-server tests_dsHPC-040.csv "$R_LIBS"
Rscript --vanilla run-suite.R clean-client tests_dsHPCClient-040.csv "$R_LIBS"
R CMD check dsHPC_0.4.0.tar.gz
R CMD check dsHPCClient_0.4.0.tar.gz
```

Final checked source tarball SHA-256 values:

- dsHPC: `5100816fed57215d1c8226846368090b59eac6a3cc450b31bc9d7a238fe850ee`.
- dsHPCClient: `ff6a210c67af7763f6ea250b6cdb1ec28e1452def3a5373a9241d3262e42fcfd`.
