test_that("scheduler derives conservative node budgets without configuration", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  budget <- dsJobs:::.scheduler_node_budget()
  expect_true(is.finite(budget$memory_mb))
  expect_gte(budget$memory_mb, 256)
  expect_gte(budget$cpu_slots, 1)
  expect_gte(budget$gpus, 0)
})

test_that("runner resources are read from YAML and options can override them", {
  home <- setup_test_home()
  runner_yaml <- file.path(home, "runners", "heavy.yml")
  writeLines(c(
    "name: heavy",
    "plane: artifact",
    "resource_class: cpu_heavy",
    "resources:",
    "  memory_mb: 4096",
    "  cpu_slots: 3",
    "  max_concurrent: 1",
    "  concurrency_group: torch_cpu"
  ), runner_yaml)

  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  p <- dsJobs:::.scheduler_runner_profile("heavy")
  expect_equal(p$memory_mb, 4096L)
  expect_equal(p$cpu_slots, 3L)
  expect_equal(p$max_concurrent, 1)
  expect_equal(p$concurrency_group, "torch_cpu")

  withr::local_options(list(dsjobs.runner_overrides = list(
    heavy = list(memory_mb = 2048, max_concurrent = 2)
  )))
  p2 <- dsJobs:::.scheduler_runner_profile("heavy")
  expect_equal(p2$memory_mb, 2048L)
  expect_equal(p2$max_concurrent, 2)
})

test_that("scheduler gates pending jobs by memory budget", {
  home <- setup_test_home()
  writeLines(c(
    "name: heavy",
    "plane: artifact",
    "resource_class: cpu_heavy",
    "resources:",
    "  memory_mb: 4096",
    "  cpu_slots: 1"
  ), file.path(home, "runners", "heavy.yml"))
  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.executor_backend = "embedded",
    dsjobs.node_memory_mb = 6144,
    dsjobs.memory_reserve_mb = 1024,
    dsjobs.cpu_slots = 4
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "heavy", config = list())))
  dsJobs:::.store_create_job(db, "job_a", "user", spec, 1L)
  d1 <- dsJobs:::.scheduler_can_start_job(db, "job_a", spec)
  expect_true(d1$ok)
  dsJobs:::.scheduler_acquire_leases(db, "job_a", d1)
  dsJobs:::.store_update_job(db, "job_a", state = "RUNNING")

  dsJobs:::.store_create_job(db, "job_b", "user", spec, 1L)
  d2 <- dsJobs:::.scheduler_can_start_job(db, "job_b", spec)
  expect_false(d2$ok)
  expect_equal(d2$reason, "memory_budget")
})

test_that("scheduler records OOM cooldowns for runners", {
  home <- setup_test_home()
  writeLines(c(
    "name: oom_runner",
    "plane: artifact",
    "resources:",
    "  memory_mb: 1024",
    "  oom_cooldown_secs: 60"
  ), file.path(home, "runners", "oom_runner.yml"))
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  expect_true(dsJobs:::.scheduler_record_runner_failure(db, "oom_runner", -9L))
  cd <- dsJobs:::.scheduler_active_cooldown(db, "oom_runner")
  expect_equal(cd$runner, "oom_runner")
  expect_equal(cd$reason, "oom")
})

test_that("optional GPU runners receive devices only when available", {
  home <- setup_test_home()
  writeLines(c(
    "name: gpuish",
    "plane: artifact",
    "resource_class: gpu_optional",
    "resources:",
    "  memory_mb: 1024",
    "  optional_gpus: 1"
  ), file.path(home, "runners", "gpuish.yml"))
  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.executor_backend = "embedded",
    dsjobs.node_memory_mb = 8192,
    dsjobs.memory_reserve_mb = 1024,
    dsjobs.cpu_slots = 4,
    dsjobs.gpu_count = 1
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "gpuish", config = list())))
  d <- dsJobs:::.scheduler_can_start_job(db, "job_gpu", spec)
  expect_true(d$ok)
  expect_equal(d$gpu_devices, "0")
})

test_that("worker leader election creates one active scheduler per cell", {
  home <- setup_test_home()
  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.worker_leader_ttl_secs = 30
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  resources <- list(memory_mb = 1024L, cpu_slots = 1L,
                    gpus = 0L, gpu_devices = character(0))

  expect_true(dsJobs:::.scheduler_renew_worker_leader(db, "worker_a", resources))
  expect_true(dsJobs:::.scheduler_has_active_worker(db))
  expect_false(dsJobs:::.scheduler_renew_worker_leader(db, "worker_b", resources))

  dsJobs:::.scheduler_release_worker_leader(db, "worker_a")
  expect_true(dsJobs:::.scheduler_renew_worker_leader(db, "worker_b", resources))
  status <- dsJobs:::.scheduler_status(db)
  expect_equal(status$cell$leader$holder, "worker_b")
})
