.seal_step_cache_spec <- function(spec, provider = "dsHPC") {
  spec$.dshpc_provider <- provider
  spec$.dshpc_runtime_revision <- dsHPC:::.dshpc_runtime_revision(spec)
  spec$.dshpc_runtime_identity <- dsHPC:::.dshpc_runtime_identity(
    spec, provider)
  spec
}

test_that("step cache hashes input content and executable step definition", {
  input_a <- tempfile("input_a_")
  input_b <- tempfile("input_b_")
  dir.create(input_a)
  dir.create(input_b)
  on.exit(unlink(c(input_a, input_b), recursive = TRUE), add = TRUE)
  writeLines("same-content", file.path(input_a, "x.txt"))
  writeLines("same-content", file.path(input_b, "x.txt"))

  step <- list(type = "run", plane = "artifact", runner = "dummy",
               config = list(alpha = 1), inputs = list(1L))

  expect_equal(
    dsHPC:::.step_cache_hash(step, input_a),
    dsHPC:::.step_cache_hash(step, input_b)
  )

  writeLines("different-content", file.path(input_b, "x.txt"))
  expect_false(identical(
    dsHPC:::.step_cache_hash(step, input_a),
    dsHPC:::.step_cache_hash(step, input_b)
  ))

  writeLines("same-content", file.path(input_b, "x.txt"))
  step_other_param <- step
  step_other_param$config$alpha <- 2
  expect_false(identical(
    dsHPC:::.step_cache_hash(step, input_a),
    dsHPC:::.step_cache_hash(step_other_param, input_a)
  ))

  step_other_runner <- step
  step_other_runner$runner <- "other_dummy"
  expect_false(identical(
    dsHPC:::.step_cache_hash(step, input_a),
    dsHPC:::.step_cache_hash(step_other_runner, input_a)
  ))

  step_other_input_ref <- step
  step_other_input_ref$inputs <- list(upstream = list(step = 99L))
  expect_equal(
    dsHPC:::.step_cache_hash(step, input_a),
    dsHPC:::.step_cache_hash(step_other_input_ref, input_a)
  )

  step_other_node <- step
  step_other_node$node_id <- "renamed_dag_node"
  expect_equal(
    dsHPC:::.step_cache_hash(step, input_a),
    dsHPC:::.step_cache_hash(step_other_node, input_a)
  )

  expect_false(identical(
    dsHPC:::.step_cache_hash(step, input_a,
      reuse_fingerprint = strrep("a", 64L)),
    dsHPC:::.step_cache_hash(step, input_a,
      reuse_fingerprint = strrep("b", 64L))
  ))
})

test_that("step cache detects relative names, empty dirs and symlinked input content", {
  root_a <- tempfile("cache_root_a_")
  root_b <- tempfile("cache_root_b_")
  dir.create(file.path(root_a, "input"), recursive = TRUE)
  dir.create(file.path(root_b, "input"), recursive = TRUE)
  on.exit(unlink(c(root_a, root_b), recursive = TRUE), add = TRUE)

  writeLines("x", file.path(root_a, "input", "a.txt"))
  writeLines("x", file.path(root_b, "input", "b.txt"))

  expect_false(identical(
    dsHPC:::.step_cache_hash_path(file.path(root_a, "input")),
    dsHPC:::.step_cache_hash_path(file.path(root_b, "input"))
  ))

  unlink(file.path(root_b, "input", "b.txt"))
  writeLines("x", file.path(root_b, "input", "a.txt"))
  expect_equal(
    dsHPC:::.step_cache_hash_path(file.path(root_a, "input")),
    dsHPC:::.step_cache_hash_path(file.path(root_b, "input"))
  )

  dir.create(file.path(root_b, "input", "empty_subdir"))
  expect_false(identical(
    dsHPC:::.step_cache_hash_path(file.path(root_a, "input")),
    dsHPC:::.step_cache_hash_path(file.path(root_b, "input"))
  ))

  symlink_root <- tempfile("cache_symlink_")
  dir.create(symlink_root)
  on.exit(unlink(symlink_root, recursive = TRUE), add = TRUE)
  ok <- tryCatch(file.symlink(file.path(root_a, "input"),
                              file.path(symlink_root, "input")),
                 error = function(e) FALSE)
  if (isTRUE(ok)) {
    expect_error(dsHPC:::.step_cache_hash_path(symlink_root),
      "artifact tree failed validation")
  }
})

test_that("step cache ignores volatile staged input paths", {
  root_a <- tempfile("cache_staged_a_")
  root_b <- tempfile("cache_staged_b_")
  dir.create(file.path(root_a, "input", "left"), recursive = TRUE)
  dir.create(file.path(root_b, "input", "left"), recursive = TRUE)
  on.exit(unlink(c(root_a, root_b), recursive = TRUE), add = TRUE)

  writeLines("same-content", file.path(root_a, "input", "left", "x.txt"))
  writeLines("same-content", file.path(root_b, "input", "left", "x.txt"))
  jsonlite::write_json(list(
    left = list(step = 1L, ref = "features",
                source = "/dshpc/artifacts/job_a/step_001/output",
                path = "/dshpc/artifacts/job_b/step_003/input/left")
  ), file.path(root_a, "input", "inputs.json"),
  auto_unbox = TRUE, pretty = TRUE)
  jsonlite::write_json(list(
    left = list(path = "/dshpc/artifacts/job_z/step_004/input/left",
                source = "/dshpc/artifacts/job_q/step_001/output",
                ref = "features", step = 1L)
  ), file.path(root_b, "input", "inputs.json"),
  auto_unbox = TRUE, pretty = TRUE)

  expect_equal(
    dsHPC:::.step_cache_hash_path(file.path(root_a, "input")),
    dsHPC:::.step_cache_hash_path(file.path(root_b, "input"))
  )

  jsonlite::write_json(list(
    left = list(step = 2L, ref = "features",
                source = "/dshpc/artifacts/job_q/step_002/output",
                path = "/dshpc/artifacts/job_z/step_004/input/left")
  ), file.path(root_b, "input", "inputs.json"),
  auto_unbox = TRUE, pretty = TRUE)
  expect_false(identical(
    dsHPC:::.step_cache_hash_path(file.path(root_a, "input")),
    dsHPC:::.step_cache_hash_path(file.path(root_b, "input"))
  ))
})

test_that("step cache includes runner registry definition in the hash", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  runner_file <- file.path(home, "runners", "hash_runner.yml")
  writeLines(c(
    "name: hash_runner",
    "command: /bin/sh",
    "args_template:",
    "  - -c",
    "  - echo one > {output_dir}/x.txt"
  ), runner_file)

  step <- list(type = "run", plane = "artifact", runner = "hash_runner",
               config = list(alpha = 1))
  hash_one <- dsHPC:::.step_cache_hash(step, NULL)

  writeLines(c(
    "name: hash_runner",
    "command: /bin/sh",
    "args_template:",
    "  - -c",
    "  - echo two > {output_dir}/x.txt"
  ), runner_file)
  hash_two <- dsHPC:::.step_cache_hash(step, NULL)

  expect_false(identical(hash_one, hash_two))
})

test_that("completed artifact steps are reused across different jobs", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.step_cache = TRUE))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  step <- list(type = "run", plane = "artifact", runner = "dummy",
               config = list(alpha = 1))
  source_spec <- .seal_step_cache_spec(list(steps = list(step), label = "dsHPC_test",
                      resource_class = "default", visibility = "global",
                      reuse_fingerprint = strrep("a", 64L)))
  target_spec <- .seal_step_cache_spec(list(steps = list(step), label = "dsHPC_test",
                      resource_class = "default", visibility = "global",
                      reuse_fingerprint = strrep("a", 64L)))

  dsHPC:::.store_create_job(db, "job_source", "user_a", source_spec, 1L)
  source_step_dir <- dsHPC:::.ensure_step_dir("job_source", 1L)
  source_out <- file.path(source_step_dir, "output", "result.txt")
  writeLines("cached-output", source_out)
  source_ref <- file.path("artifacts", "job_source", "step_001", "output")
  step_hash <- dsHPC:::.step_cache_hash(step, NULL,
    reuse_fingerprint = source_spec$reuse_fingerprint)
  dsHPC:::.db_register_output(db, "job_source", 1L, "result.txt",
    "artifact_file", source_out, size_bytes = file.info(source_out)$size,
    safe_for_client = FALSE)
  dsHPC:::.store_update_step(db, "job_source", 1L, state = "done",
    output_ref = source_ref, step_hash = step_hash,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
  dsHPC:::.store_update_job(db, "job_source", state = "FINISHED",
    step_index = 1L,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))

  dsHPC:::.store_create_job(db, "job_target", "user_b", target_spec, 1L)
  dsHPC:::.store_update_job(db, "job_target", state = "RUNNING",
    step_index = 1L)

  # Cache discovery and rebasing rely only on durable DB/artifact state.
  dsHPC:::.db_close(db)
  db <- dsHPC:::.db_connect()
  dsHPC:::.executor_run_step(db, "job_target", 1L, target_spec)

  target_job <- dsHPC:::.store_get_job(db, "job_target")
  expect_equal(target_job$state, "FINISHED")

  target_step <- DBI::dbGetQuery(db,
    "SELECT state, step_hash, cache_hit, cache_source_job_id,
            cache_source_step_index, output_ref
     FROM steps WHERE job_id = 'job_target' AND step_index = 1")
  expect_equal(target_step$state[1], "done")
  expect_equal(target_step$step_hash[1], step_hash)
  expect_equal(as.integer(target_step$cache_hit[1]), 1L)
  expect_equal(target_step$cache_source_job_id[1], "job_source")
  expect_equal(as.integer(target_step$cache_source_step_index[1]), 1L)

  target_out <- file.path(home, "artifacts", "job_target", "step_001",
                          "output", "result.txt")
  expect_true(file.exists(target_out))
  expect_equal(readLines(target_out, warn = FALSE), "cached-output")

  target_outputs <- DBI::dbGetQuery(db,
    "SELECT name, path_or_ref FROM outputs WHERE job_id = 'job_target'")
  expect_equal(target_outputs$name[1], "result.txt")
  expect_true(grepl("job_target", target_outputs$path_or_ref[1], fixed = TRUE))

  events <- DBI::dbGetQuery(db,
    "SELECT event FROM events WHERE job_id = 'job_target'")
  expect_true("step_cached" %in% events$event)
})

test_that("step cache does not reuse running steps or missing cached outputs", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.step_cache = TRUE))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  step <- list(type = "run", plane = "artifact", runner = "dummy",
               config = list(alpha = 1))
  spec <- list(steps = list(step), label = "dsHPC_test",
               resource_class = "default")
  step_hash <- dsHPC:::.step_cache_hash(step, NULL)

  dsHPC:::.store_create_job(db, "job_running_source", "user_a", spec, 1L)
  running_dir <- dsHPC:::.ensure_step_dir("job_running_source", 1L)
  writeLines("running-output", file.path(running_dir, "output", "result.txt"))
  dsHPC:::.store_update_step(db, "job_running_source", 1L, state = "running",
    output_ref = file.path("artifacts", "job_running_source", "step_001",
                           "output"),
    step_hash = step_hash)
  dsHPC:::.store_update_job(db, "job_running_source", state = "RUNNING",
    step_index = 1L)

  expect_null(dsHPC:::.step_cache_find(db, step_hash))

  dsHPC:::.store_create_job(db, "job_missing_source", "user_a", spec, 1L)
  dsHPC:::.store_update_step(db, "job_missing_source", 1L, state = "done",
    output_ref = file.path("artifacts", "job_missing_source", "step_001",
                           "output"),
    step_hash = step_hash)
  dsHPC:::.store_update_job(db, "job_missing_source", state = "FINISHED",
    step_index = 1L,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))

  expect_null(dsHPC:::.step_cache_find(db, step_hash))
})

test_that("step cache reuses completed steps from still-running parent jobs", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.step_cache = TRUE))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  step <- list(type = "run", plane = "artifact", runner = "dummy",
               config = list(alpha = 1))
  source_spec <- .seal_step_cache_spec(list(steps = list(step, step), label = "dsHPC_test",
                      resource_class = "default", visibility = "global",
                      reuse_fingerprint = strrep("a", 64L)))
  target_spec <- .seal_step_cache_spec(list(steps = list(step), label = "dsHPC_test",
                      resource_class = "default", visibility = "global",
                      reuse_fingerprint = strrep("a", 64L)))

  dsHPC:::.store_create_job(db, "job_source_running", "user_a",
    source_spec, 2L)
  source_step_dir <- dsHPC:::.ensure_step_dir("job_source_running", 1L)
  source_out <- file.path(source_step_dir, "output", "result.txt")
  writeLines("completed-prefix-output", source_out)
  source_ref <- file.path("artifacts", "job_source_running", "step_001",
                          "output")
  step_hash <- dsHPC:::.step_cache_hash(step, NULL,
    reuse_fingerprint = source_spec$reuse_fingerprint)
  dsHPC:::.db_register_output(db, "job_source_running", 1L, "result.txt",
    "artifact_file", source_out, size_bytes = file.info(source_out)$size,
    safe_for_client = FALSE)
  dsHPC:::.store_update_step(db, "job_source_running", 1L, state = "done",
    output_ref = source_ref, step_hash = step_hash,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
  dsHPC:::.store_update_job(db, "job_source_running", state = "RUNNING",
    step_index = 2L)

  dsHPC:::.store_create_job(db, "job_target_from_running", "user_b",
    target_spec, 1L)
  dsHPC:::.store_update_job(db, "job_target_from_running", state = "RUNNING",
    step_index = 1L)

  dsHPC:::.executor_run_step(db, "job_target_from_running", 1L, target_spec)

  target_job <- dsHPC:::.store_get_job(db, "job_target_from_running")
  expect_equal(target_job$state, "FINISHED")

  target_step <- DBI::dbGetQuery(db,
    "SELECT state, cache_hit, cache_source_job_id
     FROM steps WHERE job_id = 'job_target_from_running' AND step_index = 1")
  expect_equal(target_step$state[1], "done")
  expect_equal(as.integer(target_step$cache_hit[1]), 1L)
  expect_equal(target_step$cache_source_job_id[1], "job_source_running")
})

test_that("step cache coalesces equivalent in-flight steps", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.step_cache = TRUE))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  step <- list(type = "run", plane = "artifact", runner = "dummy",
               config = list(alpha = 1))
  spec <- .seal_step_cache_spec(list(steps = list(step), label = "dsHPC_test",
               resource_class = "default", visibility = "global",
               reuse_fingerprint = strrep("a", 64L)))
  step_hash <- dsHPC:::.step_cache_hash(step, NULL,
    reuse_fingerprint = spec$reuse_fingerprint)

  dsHPC:::.store_create_job(db, "job_inflight_source", "user_a", spec, 1L)
  dsHPC:::.store_update_job(db, "job_inflight_source", state = "RUNNING",
    step_index = 1L)
  dsHPC:::.store_update_step(db, "job_inflight_source", 1L,
    state = "running", step_hash = step_hash,
    started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))

  dsHPC:::.store_create_job(db, "job_inflight_target", "user_b", spec, 1L)
  dsHPC:::.scheduler_acquire_leases(db, "job_inflight_target",
    list(plan = list(memory_mb = 1L, cpu_slots = 1L)))
  dsHPC:::.store_update_job(db, "job_inflight_target", state = "RUNNING",
    step_index = 1L)

  dsHPC:::.executor_run_step(db, "job_inflight_target", 1L, spec)

  target_job <- dsHPC:::.store_get_job(db, "job_inflight_target")
  expect_equal(target_job$state, "PENDING")
  expect_true(is.na(target_job$worker_pid))
  expect_equal(nrow(DBI::dbGetQuery(db,
    "SELECT * FROM resource_leases WHERE job_id = 'job_inflight_target'")), 0L)

  target_step <- DBI::dbGetQuery(db,
    "SELECT state, step_hash, cache_source_job_id, cache_source_step_index
     FROM steps WHERE job_id = 'job_inflight_target' AND step_index = 1")
  expect_equal(target_step$state[1], "waiting_cache")
  expect_equal(target_step$step_hash[1], step_hash)
  expect_equal(target_step$cache_source_job_id[1], "job_inflight_source")
  expect_equal(as.integer(target_step$cache_source_step_index[1]), 1L)
  expect_true(dsHPC:::.step_cache_waiting_active(db,
    "job_inflight_target", 1L))

  events <- DBI::dbGetQuery(db,
    "SELECT event FROM events WHERE job_id = 'job_inflight_target'")
  expect_true("step_cache_wait" %in% events$event)

  dsHPC:::.worker_dispatch(db)
  expect_equal(dsHPC:::.store_get_job(db, "job_inflight_target")$state,
    "PENDING")

  source_step_dir <- dsHPC:::.ensure_step_dir("job_inflight_source", 1L)
  source_out <- file.path(source_step_dir, "output", "result.txt")
  writeLines("single-flight-output", source_out)
  source_ref <- file.path("artifacts", "job_inflight_source", "step_001",
                          "output")
  dsHPC:::.db_register_output(db, "job_inflight_source", 1L, "result.txt",
    "artifact_file", source_out, size_bytes = file.info(source_out)$size,
    safe_for_client = FALSE)
  dsHPC:::.store_update_step(db, "job_inflight_source", 1L,
    state = "done", output_ref = source_ref,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
  dsHPC:::.store_update_job(db, "job_inflight_target", state = "RUNNING",
    step_index = 1L, error_message = NA_character_)

  dsHPC:::.executor_run_step(db, "job_inflight_target", 1L, spec)

  expect_equal(dsHPC:::.store_get_job(db, "job_inflight_target")$state,
    "FINISHED")
  target_step_done <- DBI::dbGetQuery(db,
    "SELECT state, cache_hit, cache_source_job_id
     FROM steps WHERE job_id = 'job_inflight_target' AND step_index = 1")
  expect_equal(target_step_done$state[1], "done")
  expect_equal(as.integer(target_step_done$cache_hit[1]), 1L)
  expect_equal(target_step_done$cache_source_job_id[1], "job_inflight_source")
})

test_that("step cache reuses shared multi-step prefixes by content", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.step_cache = TRUE))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  step_a <- list(type = "run", plane = "artifact", runner = "dummy_a",
                 config = list(stage = "A"))
  step_b <- list(type = "run", plane = "artifact", runner = "dummy_b",
                 config = list(stage = "B"))
  source_spec <- .seal_step_cache_spec(list(steps = list(step_a, step_b), label = "dsHPC_test",
                      resource_class = "default", visibility = "global",
                      reuse_fingerprint = strrep("a", 64L)))
  target_spec <- source_spec

  dsHPC:::.store_create_job(db, "job_prefix_source", "user_a", source_spec, 2L)
  source_dir_a <- dsHPC:::.ensure_step_dir("job_prefix_source", 1L)
  source_dir_b <- dsHPC:::.ensure_step_dir("job_prefix_source", 2L)
  writeLines("A-output", file.path(source_dir_a, "output", "a.txt"))
  writeLines("B-output", file.path(source_dir_b, "output", "b.txt"))

  source_ref_a <- file.path("artifacts", "job_prefix_source", "step_001",
                            "output")
  source_ref_b <- file.path("artifacts", "job_prefix_source", "step_002",
                            "output")
  hash_a <- dsHPC:::.step_cache_hash(step_a, NULL,
    reuse_fingerprint = source_spec$reuse_fingerprint)
  hash_b <- dsHPC:::.step_cache_hash(step_b, file.path(home, source_ref_a),
    reuse_fingerprint = source_spec$reuse_fingerprint)

  dsHPC:::.db_register_output(db, "job_prefix_source", 1L, "a.txt",
    "artifact_file", file.path(source_dir_a, "output", "a.txt"),
    size_bytes = file.info(file.path(source_dir_a, "output", "a.txt"))$size,
    safe_for_client = FALSE)
  dsHPC:::.db_register_output(db, "job_prefix_source", 2L, "b.txt",
    "artifact_file", file.path(source_dir_b, "output", "b.txt"),
    size_bytes = file.info(file.path(source_dir_b, "output", "b.txt"))$size,
    safe_for_client = FALSE)
  dsHPC:::.store_update_step(db, "job_prefix_source", 1L, state = "done",
    output_ref = source_ref_a, step_hash = hash_a)
  dsHPC:::.store_update_step(db, "job_prefix_source", 2L, state = "done",
    output_ref = source_ref_b, step_hash = hash_b)
  dsHPC:::.store_update_job(db, "job_prefix_source", state = "FINISHED",
    step_index = 2L,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))

  dsHPC:::.store_create_job(db, "job_prefix_target", "user_b", target_spec, 2L)
  dsHPC:::.store_update_job(db, "job_prefix_target", state = "RUNNING",
    step_index = 1L)

  dsHPC:::.executor_run_step(db, "job_prefix_target", 1L, target_spec)

  target_job <- dsHPC:::.store_get_job(db, "job_prefix_target")
  expect_equal(target_job$state, "FINISHED")

  target_steps <- DBI::dbGetQuery(db,
    "SELECT step_index, cache_hit, cache_source_job_id
     FROM steps WHERE job_id = 'job_prefix_target'
     ORDER BY step_index")
  expect_equal(as.integer(target_steps$cache_hit), c(1L, 1L))
  expect_equal(target_steps$cache_source_job_id, rep("job_prefix_source", 2L))

  expect_equal(
    readLines(file.path(home, "artifacts", "job_prefix_target", "step_001",
                        "output", "a.txt"), warn = FALSE),
    "A-output"
  )
  expect_equal(
    readLines(file.path(home, "artifacts", "job_prefix_target", "step_002",
                        "output", "b.txt"), warn = FALSE),
    "B-output"
  )
})

test_that("step cache can be disabled per step and skips session side effects", {
  artifact_step <- list(type = "run", plane = "artifact", runner = "dummy")
  expect_true(dsHPC:::.step_cache_enabled(artifact_step))

  artifact_step$cache <- FALSE
  expect_false(dsHPC:::.step_cache_enabled(artifact_step))

  publish_step <- list(type = "publish_asset", plane = "session",
                       dataset_id = "ds", asset_name = "asset")
  expect_false(dsHPC:::.step_cache_enabled(publish_step))
})

test_that("step cache never crosses a private job boundary", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.step_cache = TRUE))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  step <- list(type = "run", plane = "artifact", runner = "dummy")
  private_spec <- list(steps = list(step), label = "dsHPC_test",
    visibility = "private")
  global_spec <- private_spec
  global_spec$visibility <- "global"
  global_spec$reuse_fingerprint <- strrep("a", 64L)
  global_spec <- .seal_step_cache_spec(global_spec)
  unsealed_global_spec <- global_spec
  unsealed_global_spec$reuse_fingerprint <- NULL
  step_hash <- dsHPC:::.step_cache_hash(step, NULL,
    reuse_fingerprint = global_spec$reuse_fingerprint)

  dsHPC:::.store_create_job(db, "job_private_source", "user_a",
    private_spec, 1L)
  private_dir <- dsHPC:::.ensure_step_dir("job_private_source", 1L)
  writeLines("private", file.path(private_dir, "output", "value.txt"))
  dsHPC:::.store_update_step(db, "job_private_source", 1L, state = "done",
    output_ref = file.path("artifacts", "job_private_source", "step_001",
      "output"), step_hash = step_hash)
  dsHPC:::.store_update_job(db, "job_private_source", state = "FINISHED",
    step_index = 1L)

  dsHPC:::.store_create_job(db, "job_private_target", "user_b",
    private_spec, 1L)
  dsHPC:::.store_create_job(db, "job_global_target", "user_b",
    global_spec, 1L)
  dsHPC:::.store_create_job(db, "job_unsealed_global_target", "user_b",
    unsealed_global_spec, 1L)

  # Cache policy must come entirely from durable job metadata, not an
  # in-memory worker/session association.
  dsHPC:::.db_close(db)
  db <- dsHPC:::.db_connect()

  expect_false(dsHPC:::.step_cache_enabled_for_job(
    db, "job_private_target", step))
  expect_true(dsHPC:::.step_cache_enabled_for_job(
    db, "job_global_target", step))
  expect_false(dsHPC:::.step_cache_enabled_for_job(
    db, "job_unsealed_global_target", step))
  expect_null(dsHPC:::.step_cache_find(db, step_hash,
    current_job_id = "job_global_target"))
})

test_that("step cache never crosses a domain label boundary", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.step_cache = TRUE))
  on.exit(cleanup_test_home(home))
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  step <- list(type = "run", plane = "artifact", runner = "dummy")
  source_spec <- .seal_step_cache_spec(list(steps = list(step),
    label = "domain_a", visibility = "global",
    reuse_fingerprint = strrep("a", 64L)))
  target_spec <- .seal_step_cache_spec(list(steps = list(step),
    label = "domain_b", visibility = "global",
    reuse_fingerprint = strrep("a", 64L)))
  step_hash <- dsHPC:::.step_cache_hash(step, NULL,
    reuse_fingerprint = target_spec$reuse_fingerprint)
  dsHPC:::.store_create_job(db, "job_label_source", "user_a",
    source_spec, 1L)
  source_dir <- dsHPC:::.ensure_step_dir("job_label_source", 1L)
  writeLines("domain-a", file.path(source_dir, "output", "value.txt"))
  dsHPC:::.store_update_step(db, "job_label_source", 1L, state = "done",
    output_ref = file.path("artifacts", "job_label_source", "step_001",
      "output"), step_hash = step_hash)
  dsHPC:::.store_update_job(db, "job_label_source", state = "FINISHED",
    step_index = 1L)
  dsHPC:::.store_create_job(db, "job_label_target", "user_b",
    target_spec, 1L)

  expect_null(dsHPC:::.step_cache_find(
    db, step_hash, current_job_id = "job_label_target"))
  forged <- list(job_id = "job_label_source", step_index = 1L,
    output_ref = file.path("artifacts", "job_label_source", "step_001",
      "output"))
  expect_false(dsHPC:::.step_cache_apply(
    db, "job_label_target", 1L, step_hash, forged,
    dsHPC:::.ensure_step_dir("job_label_target", 1L)))
})
