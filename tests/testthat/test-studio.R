test_that("Studio snapshot exposes global jobs only", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  dag_spec <- dsHPC:::.validate_job_spec(list(
    name = "Radiomics cohort A",
    label = "dsImaging",
    visibility = "private",
    dag = list(nodes = list(
      resolve = list(type = "emit", plane = "session", output_name = "raw",
        value = 1),
      features = list(type = "emit", plane = "session",
        output_name = "features", value = 2, inputs = "resolve"),
      summary = list(type = "safe_summary",
        inputs = list(features = "features"))
    ))
  ))
  dsHPC:::.store_create_job(db, "job_private_a", "alice", dag_spec, 3L)
  dsHPC:::.store_update_job(db, "job_private_a", state = "RUNNING",
    step_index = 2L, started_at = format(Sys.time() - 60,
      "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
  dsHPC:::.store_update_step(db, "job_private_a", 1L, state = "done",
    started_at = format(Sys.time() - 60, "%Y-%m-%dT%H:%M:%OS3Z",
      tz = "UTC"),
    finished_at = format(Sys.time() - 30, "%Y-%m-%dT%H:%M:%OS3Z",
      tz = "UTC"))
  dsHPC:::.store_update_step(db, "job_private_a", 2L, state = "running",
    started_at = format(Sys.time() - 30, "%Y-%m-%dT%H:%M:%OS3Z",
      tz = "UTC"),
    external_backend = "slurm", external_status = "RUNNING")
  private_artifacts <- file.path(home, "artifacts", "job_private_a")
  dir.create(private_artifacts, recursive = TRUE)
  saveRDS(list(status = "safe"), file.path(private_artifacts, "summary.rds"))
  dsHPC:::.db_register_output(db, "job_private_a", 1L, "summary",
    "summary", file.path(home, "artifacts", "job_private_a", "summary.rds"),
    size_bytes = 128L, safe_for_client = TRUE)

  other_spec <- dsHPC:::.validate_job_spec(list(
    name = "Other private",
    label = "dsOther",
    visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "out", value = 1))
  ))
  dsHPC:::.store_create_job(db, "job_private_b", "bob", other_spec, 1L)

  global_spec <- dsHPC:::.validate_job_spec(list(
    name = "Shared queue",
    label = "dsShared",
    visibility = "global",
    steps = list(list(type = "emit", plane = "session",
      output_name = "out", value = 1))
  ))
  dsHPC:::.store_create_job(db, "job_global", "bob", global_spec, 1L)

  mine <- trusted_hpc_call(hpcStudioInternal,
    scope = list(.owner = "alice"), mode = "mine")
  expect_equal(mine$mode, "global")
  expect_equal(mine$jobs$job_id, "job_global")
  expect_equal(mine$jobs$name, "Shared queue")
  expect_false("owner_id" %in% names(mine$jobs))
  expect_false("path_or_ref" %in% names(mine$outputs))
  expect_equal(mine$steps$node_id, "step_1")
  expect_equal(nrow(mine$dag_edges), 0L)
  expect_true(all(c("node", "usage", "executor", "cell") %in%
    names(mine$scheduler)))

  mixed <- trusted_hpc_call(hpcStudioInternal,
    scope = list(.owner = "alice"), mode = "mine+global")
  expect_equal(mixed$jobs$job_id, "job_global")
  expect_false("job_private_a" %in% mixed$jobs$job_id)
  expect_false("job_private_b" %in% mixed$jobs$job_id)
})

test_that("hpcListInternal ignores owner scopes and lists global jobs", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  spec <- dsHPC:::.validate_job_spec(list(
    name = "Named job",
    visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "out", value = 1))
  ))
  dsHPC:::.store_create_job(db, "job_named", "alice", spec, 1L)

  global_spec <- dsHPC:::.validate_job_spec(list(
    name = "Global job",
    visibility = "global",
    steps = list(list(type = "emit", plane = "session",
      output_name = "out", value = 1))
  ))
  dsHPC:::.store_create_job(db, "job_global_named", "bob", global_spec, 1L)

  listed <- trusted_hpc_call(hpcListInternal,
    scope = list(.owner = "alice"), mode = "mine")
  expect_equal(listed$job_id, "job_global_named")
  expect_equal(listed$name, "Global job")
  expect_false("visibility" %in% names(listed))
  expect_equal(trusted_hpc_call(hpcListInternal,
    scope = list(.owner = "bob"), mode = "mine")$job_id,
    "job_global_named")
})
