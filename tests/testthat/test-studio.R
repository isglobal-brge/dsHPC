test_that("Studio snapshot exposes scoped jobs, names, DAG and safe metadata", {
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

  mine <- hpcStudioDS(scope = list(.owner = "alice"), mode = "mine")
  expect_equal(mine$jobs$job_id, "job_private_a")
  expect_equal(mine$jobs$name, "Radiomics cohort A")
  expect_false("owner_id" %in% names(mine$jobs))
  expect_false("path_or_ref" %in% names(mine$outputs))
  expect_equal(mine$steps$node_id, c("resolve", "features", "summary"))
  expect_equal(nrow(mine$dag_edges), 2L)
  expect_equal(mine$dag_edges$from_node, c("resolve", "features"))
  expect_true(all(c("node", "usage", "executor", "cell") %in%
    names(mine$scheduler)))

  mixed <- hpcStudioDS(scope = list(.owner = "alice"), mode = "mine+global")
  expect_setequal(mixed$jobs$job_id, c("job_private_a", "job_global"))
  expect_false("job_private_b" %in% mixed$jobs$job_id)
})

test_that("hpcListDS scopes jobs and includes display name", {
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

  listed <- hpcListDS(scope = list(.owner = "alice"), mode = "mine")
  expect_equal(listed$name, "Named job")
  expect_false("visibility" %in% names(listed))
  expect_equal(nrow(hpcListDS(scope = list(.owner = "bob"), mode = "mine")),
    0L)
})
