.dag_scheduler_spec <- function(home, runner = "dag_resources", resources = list()) {
  yaml::write_yaml(list(name = runner, command = "/bin/true",
    args_template = character(0), resources = modifyList(list(
      memory_mb = 32L, cpu_slots = 1L), resources)),
    file.path(home, "runners", paste0(runner, ".yml")))
  step <- list(type = "run_artifact", plane = "artifact", runner = runner)
  list(label = "dsHPC_test", steps = rep(list(step), 3L),
    dag_compiled = list(node_order = c("a", "b", "c"),
      node_step_index = list(a = 1L, b = 2L, c = 3L)))
}

.claim_dag_test_step <- function(db, job_id, index, decision) {
  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  dsHPC:::.store_update_step(db, job_id, index, state = "running")
  dsHPC:::.scheduler_acquire_step_lease(db, job_id, index, decision)
  DBI::dbExecute(db, "COMMIT")
}

test_that("DAG resources are admitted per running step and released separately", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.executor_backend = "embedded", dshpc.node_memory_mb = 1024L,
    dshpc.memory_reserve_mb = 0L, dshpc.cpu_slots = 1L,
    dshpc.max_jobs_global = 1L))
  spec <- .dag_scheduler_spec(home)
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  dsHPC:::.store_create_job(db, "job_dag_cpu", "owner", spec, 3L)
  admission <- dsHPC:::.scheduler_can_start_job(db, "job_dag_cpu", spec)
  expect_true(admission$ok)
  expect_equal(admission$plan$cpu_slots, 0L)
  dsHPC:::.store_update_job(db, "job_dag_cpu", state = "RUNNING", dag = 1L)

  first <- dsHPC:::.scheduler_can_start_job(db, "job_dag_cpu", spec,
    step_index = 1L)
  expect_true(first$ok)
  .claim_dag_test_step(db, "job_dag_cpu", 1L, first)
  second <- dsHPC:::.scheduler_can_start_job(db, "job_dag_cpu", spec,
    step_index = 2L)
  expect_false(second$ok)
  expect_identical(second$reason, "cpu_budget")
  usage <- dsHPC:::.scheduler_running_usage(db)
  expect_equal(usage$memory_mb, 32L)
  expect_equal(usage$cpu_slots, 1L)
  expect_equal(unname(usage$runners), 1L)
  expect_equal(usage$running_jobs, 1L)

  dsHPC:::.store_update_step(db, "job_dag_cpu", 1L, state = "done")
  dsHPC:::.scheduler_release_step_lease(db, "job_dag_cpu", 1L)
  second <- dsHPC:::.scheduler_can_start_job(db, "job_dag_cpu", spec,
    step_index = 2L)
  expect_true(second$ok)
  .claim_dag_test_step(db, "job_dag_cpu", 2L, second)
  expect_equal(dsHPC:::.scheduler_running_usage(db)$cpu_slots, 1L)
  dsHPC:::.store_create_job(db, "job_other", "owner", spec, 3L)
  expect_identical(dsHPC:::.scheduler_can_start_job(db, "job_other", spec)$reason,
    "max_jobs_global")
})

test_that("DAG runner limits count simultaneous steps within one job", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.executor_backend = "embedded", dshpc.node_memory_mb = 1024L,
    dshpc.memory_reserve_mb = 0L, dshpc.cpu_slots = 8L))
  spec <- .dag_scheduler_spec(home,
    resources = list(max_concurrent = 2L, concurrency_group = "shared_group"))
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  dsHPC:::.store_create_job(db, "job_dag_limits", "owner", spec, 3L)
  dsHPC:::.store_update_job(db, "job_dag_limits", state = "RUNNING", dag = 1L)
  for (index in 1:2) {
    decision <- dsHPC:::.scheduler_can_start_job(db, "job_dag_limits", spec,
      step_index = index)
    expect_true(decision$ok)
    .claim_dag_test_step(db, "job_dag_limits", index, decision)
  }
  decision <- dsHPC:::.scheduler_can_start_job(db, "job_dag_limits", spec,
    step_index = 3L)
  expect_false(decision$ok)
  expect_identical(decision$reason, "runner_concurrency:dag_resources")
  expect_equal(dsHPC:::.scheduler_running_usage(db)$groups[["shared_group"]], 2L)
  expect_equal(DBI::dbGetQuery(db,
    "SELECT amount FROM resource_leases
      WHERE job_id = 'job_dag_limits' AND resource = 'memory_mb'")$amount, 64)

  dsHPC:::.store_update_step(db, "job_dag_limits", 1L, state = "done")
  dsHPC:::.scheduler_release_step_lease(db, "job_dag_limits", 1L)
  expect_equal(dsHPC:::.scheduler_running_usage(db)$cpu_slots, 1L)
  expect_true(dsHPC:::.scheduler_can_start_job(db, "job_dag_limits", spec,
    step_index = 3L)$ok)
})

test_that("concurrent DAG GPU allocations remain disjoint and durable", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.executor_backend = "embedded", dshpc.node_memory_mb = 1024L,
    dshpc.memory_reserve_mb = 0L, dshpc.cpu_slots = 4L,
    dshpc.gpu_count = 2L))
  spec <- .dag_scheduler_spec(home, resources = list(gpus = 1L))
  db <- dsHPC:::.db_connect()
  dsHPC:::.store_create_job(db, "job_dag_gpu", "owner", spec, 3L)
  dsHPC:::.store_update_job(db, "job_dag_gpu", state = "RUNNING", dag = 1L)
  for (index in 1:2) {
    decision <- dsHPC:::.scheduler_can_start_job(db, "job_dag_gpu", spec,
      step_index = index)
    expect_true(decision$ok)
    expect_equal(decision$gpu_devices, as.character(index - 1L))
    .claim_dag_test_step(db, "job_dag_gpu", index, decision)
  }
  dsHPC:::.db_close(db)
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  expect_equal(dsHPC:::.scheduler_job_gpu_devices(db, "job_dag_gpu", 1L), "0")
  expect_equal(dsHPC:::.scheduler_job_gpu_devices(db, "job_dag_gpu", 2L), "1")
  expect_equal(dsHPC:::.scheduler_running_usage(db)$gpus, 2L)
  expect_false(dsHPC:::.scheduler_can_start_job(db, "job_dag_gpu", spec,
    step_index = 3L)$ok)
  dsHPC:::.store_update_step(db, "job_dag_gpu", 1L, state = "done")
  dsHPC:::.scheduler_release_step_lease(db, "job_dag_gpu", 1L)
  expect_equal(unlist(dsHPC:::.scheduler_job_gpu_devices(db, "job_dag_gpu")), "1")
  expect_equal(dsHPC:::.scheduler_can_start_job(db, "job_dag_gpu", spec,
    step_index = 3L)$gpu_devices, "0")
})
