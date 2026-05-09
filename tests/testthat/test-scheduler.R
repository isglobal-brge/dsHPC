test_that("scheduler derives conservative node budgets without configuration", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  budget <- dsHPC:::.scheduler_node_budget()
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

  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  p <- dsHPC:::.scheduler_runner_profile("heavy")
  expect_equal(p$memory_mb, 4096L)
  expect_equal(p$cpu_slots, 3L)
  expect_equal(p$max_concurrent, 1)
  expect_equal(p$concurrency_group, "torch_cpu")

  withr::local_options(list(dshpc.runner_overrides = list(
    heavy = list(memory_mb = 2048, max_concurrent = 2)
  )))
  p2 <- dsHPC:::.scheduler_runner_profile("heavy")
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
    dshpc.home = home,
    dshpc.executor_backend = "embedded",
    dshpc.node_memory_mb = 6144,
    dshpc.memory_reserve_mb = 1024,
    dshpc.cpu_slots = 4
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "heavy", config = list())))
  dsHPC:::.store_create_job(db, "job_a", "user", spec, 1L)
  d1 <- dsHPC:::.scheduler_can_start_job(db, "job_a", spec)
  expect_true(d1$ok)
  dsHPC:::.scheduler_acquire_leases(db, "job_a", d1)
  dsHPC:::.store_update_job(db, "job_a", state = "RUNNING")

  dsHPC:::.store_create_job(db, "job_b", "user", spec, 1L)
  d2 <- dsHPC:::.scheduler_can_start_job(db, "job_b", spec)
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
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  expect_true(dsHPC:::.scheduler_record_runner_failure(db, "oom_runner", -9L))
  cd <- dsHPC:::.scheduler_active_cooldown(db, "oom_runner")
  expect_equal(cd$runner, "oom_runner")
  expect_equal(cd$reason, "oom")
})

test_that("recent OOM failures throttle runner concurrency after cooldown", {
  home <- setup_test_home()
  writeLines(c(
    "name: oom_runner",
    "plane: artifact",
    "resources:",
    "  memory_mb: 1024",
    "  cpu_slots: 1",
    "  max_concurrent: 2",
    "  concurrency_group: oom_group",
    "  oom_cooldown_secs: 60"
  ), file.path(home, "runners", "oom_runner.yml"))
  withr::local_options(list(
    dshpc.home = home,
    dshpc.node_memory_mb = 8192,
    dshpc.memory_reserve_mb = 1024,
    dshpc.cpu_slots = 4,
    dshpc.oom_throttle_hours = 24,
    dshpc.oom_throttle_max_concurrent = 1
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  expect_true(dsHPC:::.scheduler_record_runner_failure(db, "oom_runner", 137L))
  DBI::dbExecute(db,
    "UPDATE runner_cooldowns SET until = ? WHERE runner = ?",
    params = list(format(Sys.time() - 60, "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"),
                  "oom_runner"))

  throttle <- dsHPC:::.scheduler_runner_throttle(db, "oom_runner")
  expect_equal(throttle$max_concurrent, 1L)

  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "oom_runner", config = list())))
  dsHPC:::.store_create_job(db, "job_oom_a", "user", spec, 1L)
  dsHPC:::.store_update_job(db, "job_oom_a", state = "RUNNING")
  dsHPC:::.store_create_job(db, "job_oom_b", "user", spec, 1L)

  decision <- dsHPC:::.scheduler_can_start_job(db, "job_oom_b", spec)
  expect_false(decision$ok)
  expect_equal(decision$reason, "runner_concurrency:oom_runner")

  status <- dsHPC:::.scheduler_status(db)
  expect_equal(status$throttles$runner[1], "oom_runner")
  expect_equal(status$throttles$max_concurrent[1], 1L)
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
    dshpc.home = home,
    dshpc.executor_backend = "embedded",
    dshpc.node_memory_mb = 8192,
    dshpc.memory_reserve_mb = 1024,
    dshpc.cpu_slots = 4,
    dshpc.gpu_count = 1
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(steps = list(list(type = "run", plane = "artifact",
    runner = "gpuish", config = list())))
  d <- dsHPC:::.scheduler_can_start_job(db, "job_gpu", spec)
  expect_true(d$ok)
  expect_equal(d$gpu_devices, "0")
})

test_that("worker leader election creates one active scheduler per cell", {
  home <- setup_test_home()
  withr::local_options(list(
    dshpc.home = home,
    dshpc.worker_leader_ttl_secs = 30
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  resources <- list(memory_mb = 1024L, cpu_slots = 1L,
                    gpus = 0L, gpu_devices = character(0))

  expect_true(dsHPC:::.scheduler_renew_worker_leader(db, "worker_a", resources))
  expect_true(dsHPC:::.scheduler_has_active_worker(db))
  expect_false(dsHPC:::.scheduler_renew_worker_leader(db, "worker_b", resources))

  dsHPC:::.scheduler_release_worker_leader(db, "worker_a")
  expect_true(dsHPC:::.scheduler_renew_worker_leader(db, "worker_b", resources))
  status <- dsHPC:::.scheduler_status(db)
  expect_equal(status$cell$leader$holder, "worker_b")
})

test_that("dead worker leaders can be marked stale for shared-cell takeover", {
  home <- setup_test_home()
  withr::local_options(list(
    dshpc.home = home,
    dshpc.worker_leader_ttl_secs = 30
  ))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  expires <- format(Sys.time() + 3600, "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

  DBI::dbExecute(db,
    "INSERT OR REPLACE INTO worker_nodes
      (worker_id, cell_id, node_id, hostname, pid, state, started_at,
       last_heartbeat, resources_json, details_json)
     VALUES ('dead_worker', 'cell_shared', 'node_a', 'host_a', 999999,
       'running', ?, ?, '{}', '{}')",
    params = list(now, now))
  DBI::dbExecute(db,
    "INSERT OR REPLACE INTO scheduler_locks
      (name, holder, acquired_at, heartbeat_at, expires_at)
     VALUES ('worker_leader', 'dead_worker', ?, ?, ?)",
    params = list(now, now, expires))

  leader <- dsHPC:::.scheduler_worker_leader(db)
  expect_equal(leader$holder, "dead_worker")
  expect_false(dsHPC:::.leader_process_alive(db, leader))

  dsHPC:::.mark_workers_stopped(worker_ids = "dead_worker")
  expect_null(dsHPC:::.scheduler_worker_leader(db))
})
