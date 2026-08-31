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
