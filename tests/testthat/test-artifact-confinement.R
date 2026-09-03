test_that("registered outputs and step references stay inside their job", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, nfilter.subset = 3))
  on.exit(cleanup_test_home(home), add = TRUE)

  handle <- trusted_hpc_call(hpcSubmitInternal, list(
    .owner = "owner",
    label = "confinement-test",
    visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "values", value = 1:5))
  ))
  outside <- file.path(home, "outside.rds")
  saveRDS(data.frame(secret = 1:5), outside)

  db <- dsHPC:::.db_connect()
  expect_error(dsHPC:::.db_register_output(db, handle$job_id, 1L,
    "outside", "summary", outside, safe_for_client = TRUE),
    "outside its job boundary")

  # Simulate a row written by an older build or a corrupted trusted plugin.
  DBI::dbExecute(db,
    "INSERT INTO outputs
       (job_id, step_index, name, kind, path_or_ref, size_bytes,
        safe_for_client, created_at)
     VALUES (?, 1, 'forged', 'summary', ?, 1, 1, 'now')",
    params = list(handle$job_id, outside))
  DBI::dbExecute(db,
    "UPDATE steps SET output_ref = ?
     WHERE job_id = ? AND step_index = 1",
    params = list(file.path("artifacts", handle$job_id, "..", "other-job",
      "step_001", "output"), handle$job_id))
  expect_error(dsHPC:::.step_output_path(db, handle$job_id, 1L),
    "reference is invalid")
  dsHPC:::.db_close(db)

  load_error <- tryCatch(
    trusted_hpc_call(hpcLoadOutputInternal, handle, "forged",
      required_label = "confinement-test"),
    error = function(e) conditionMessage(e))
  expect_match(load_error, "outside its job boundary", fixed = TRUE)
  expect_false(grepl(outside, load_error, fixed = TRUE))
  result_error <- tryCatch(hpcResultDS(handle),
    error = function(e) conditionMessage(e))
  expect_identical(result_error, "Job result is unavailable.")
  expect_false(grepl(outside, result_error, fixed = TRUE))
  expect_error(trusted_hpc_call(get_job_output_ref, handle$job_id, "forged",
    required_label = "confinement-test"), "outside its job boundary")
})

test_that("successful runners cannot register symlinked artifact trees", {
  skip_on_os("windows")
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.max_retries = 0L))
  on.exit(cleanup_test_home(home), add = TRUE)

  capability <- dsHPC:::.generate_job_capability()
  handle <- list(job_id = "job_symlink_output",
    .dshpc_capability = capability)
  spec <- list(label = "confinement-test", visibility = "private",
    resource_class = "default", steps = list(list(
      type = "run_artifact", plane = "artifact", runner = "dummy")))
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  dsHPC:::.store_create_job(db, handle$job_id, "owner", spec, 1L,
    access_token_hash = dsHPC:::.hash_job_capability(capability))
  dsHPC:::.store_update_job(db, handle$job_id,
    state = "RUNNING", step_index = 1L)
  dsHPC:::.store_update_step(db, handle$job_id, 1L, state = "running")
  output <- file.path(dsHPC:::.ensure_step_dir(handle$job_id, 1L), "output")
  secret <- file.path(home, "outside-secret.txt")
  writeLines("patient-secret", secret)
  linked <- tryCatch(file.symlink(secret, file.path(output, "result.txt")),
    error = function(e) FALSE)
  skip_if_not(isTRUE(linked), "symbolic links are unavailable")

  dsHPC:::.worker_finalize_artifact_step(db, handle$job_id, 1L,
    "dummy", 0L)
  job <- dsHPC:::.store_get_job(db, handle$job_id)
  outputs <- DBI::dbGetQuery(db,
    "SELECT * FROM outputs WHERE job_id = ?", params = list(handle$job_id))
  expect_equal(job$state, "FAILED")
  expect_equal(nrow(outputs), 0L)
  expect_identical(hpcStatusDS(handle)$error, "Job execution failed.")
})

test_that("cache, copy, and publishers reject symlink traversal", {
  skip_on_os("windows")
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, dshpc.step_cache = TRUE))
  on.exit(cleanup_test_home(home), add = TRUE)

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  step <- list(type = "run", plane = "artifact", runner = "dummy")
  spec <- list(label = "confinement-test", visibility = "global",
    resource_class = "default", steps = list(step))
  dsHPC:::.store_create_job(db, "job_link_source", "owner", spec, 1L)
  source_step <- dsHPC:::.ensure_step_dir("job_link_source", 1L)
  outside_dir <- file.path(home, "outside-tree")
  dir.create(outside_dir)
  writeLines("secret", file.path(outside_dir, "secret.txt"))
  linked <- tryCatch(file.symlink(outside_dir,
    file.path(source_step, "output", "linked")), error = function(e) FALSE)
  skip_if_not(isTRUE(linked), "symbolic links are unavailable")

  hash <- dsHPC:::.step_cache_hash(step, NULL)
  dsHPC:::.store_update_step(db, "job_link_source", 1L,
    state = "done", step_hash = hash,
    output_ref = file.path("artifacts", "job_link_source", "step_001",
      "output"))
  dsHPC:::.store_update_job(db, "job_link_source", state = "FINISHED",
    step_index = 1L)
  dsHPC:::.store_create_job(db, "job_link_target", "owner", spec, 1L)
  expect_null(dsHPC:::.step_cache_find(db, hash,
    current_job_id = "job_link_target"))

  target_escape <- file.path(home, "artifacts", "target-link")
  expect_true(file.symlink(outside_dir, target_escape))
  safe_source <- file.path(home, "safe-source")
  dir.create(safe_source)
  writeLines("safe", file.path(safe_source, "value.txt"))
  copy_error <- tryCatch(dsHPC:::.copy_input_tree(
    safe_source, file.path(target_escape, "copied"),
    target_root = file.path(home, "artifacts")),
    error = function(e) conditionMessage(e))
  expect_match(copy_error, "validation")
  expect_false(file.exists(file.path(outside_dir, "copied")))

  called <- FALSE
  kind <- paste0("test_confined_", Sys.getpid())
  publisher_env <- dsHPC:::.dshpc_env
  old_publishers <- publisher_env$.publishers
  on.exit(publisher_env$.publishers <- old_publishers, add = TRUE)
  trusted_hpc_call(register_dshpc_publisher, kind, function(...) {
    called <<- TRUE
    list(status = "published")
  })
  expect_error(dsHPC:::.execute_publish("job_link_source",
    list(publish_kind = kind), file.path(source_step, "output"), db),
    "artifact tree failed validation")
  expect_false(called)
})
