test_that("hpcSubmitInternal requires a domain label", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  expect_error(
    trusted_hpc_call(hpcSubmitInternal, list(
      .owner = "alice",
      job_id = "job_unlabelled",
      visibility = "private",
      steps = list(list(type = "emit", plane = "session",
        output_name = "out", value = 1))
    )),
    "submission requires a domain label"
  )
})

test_that("hpcLoadOutputInternal always requires a domain label", {
  expect_error(
    trusted_hpc_call(hpcLoadOutputInternal, "job_missing", "output"),
    "load operation requires a domain label"
  )
  expect_error(
    trusted_hpc_call(hpcLoadOutputInternal, "job_missing", "output",
      required_label = ""),
    "load operation requires a domain label"
  )
})

test_that("get_job_output_ref always requires a domain label", {
  expect_error(
    trusted_hpc_call(get_job_output_ref, "job_missing", "output"),
    "load operation requires a domain label"
  )
})

test_that("hpcStatusInternal requires a trusted caller and exact label", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home), add = TRUE)
  handle <- trusted_hpc_call(hpcSubmitInternal, list(
    .owner = "alice", label = "dsHPC_test", visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "values", value = 1:5))))

  expect_identical(trusted_hpc_call(hpcStatusInternal, handle,
    required_label = "dsHPC_test")$state, "FINISHED")
  expect_error(trusted_hpc_call(hpcStatusInternal, handle,
    required_label = "dsHPC"), "required domain", fixed = TRUE)
  expect_error(hpcStatusInternal(handle, required_label = "dsHPC_test"),
    "trusted server packages", fixed = TRUE)
})

test_that("server-only APIs reject direct non-package callers", {
  valid <- list(
    .owner = "alice", label = "dsHPC_test", visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "out", value = 1:5)))
  expect_error(hpcSubmitInternal(valid), "trusted server packages")
  expect_error(hpcStatusInternal(list(), required_label = "dsHPC_test"),
    "trusted server packages")
  expect_error(hpcLoadOutputInternal("job_missing", "output",
    required_label = "dsHPC_test"), "trusted server packages")
  expect_error(get_job_output_ref("job_missing", "output",
    required_label = "dsHPC_test"), "trusted server packages")
  expect_error(hpcListInternal(), "trusted server packages")
  expect_error(hpcStudioInternal(), "trusted server packages")
  expect_error(hpcSchedulerStatusInternal(), "trusted server packages")
  expect_error(query_jobs_by_tag("%"), "trusted server packages")
  expect_error(query_failed_jobs("%"), "trusted server packages")
  expect_error(count_active_jobs("%"), "trusted server packages")
  expect_error(get_owner_id(), "trusted server packages")
  expect_error(register_dshpc_publisher("test", identity),
    "trusted server packages")
  expect_error(register_dshpc_runner(list()), "trusted server packages")
  expect_error(cancel_jobs_by_tag("%", admin_key = "unused"),
    "trusted server packages")
})

test_that("server-only entry guards do not force nested side effects", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home), add = TRUE)
  spec <- list(
    .owner = "alice", label = "dsHPC_test", visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "out", value = 1:5)))

  expect_error(hpcSubmitInternal(hpcSubmitInternal(spec)),
    "trusted server packages")
  expect_error(register_dshpc_publisher("nested",
    hpcSubmitInternal(spec)), "trusted server packages")
  db <- dsHPC:::.db_connect()
  expect_equal(DBI::dbGetQuery(db, "SELECT COUNT(*) AS n FROM jobs")$n, 0)
  dsHPC:::.db_close(db)
})

test_that("get_job_output_ref matches domain labels exactly", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))
  handle <- trusted_hpc_call(hpcSubmitInternal, list(
    .owner = "alice", label = "dsImaging", visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "values", value = seq_len(5L)))))

  expect_equal(trusted_hpc_call(get_job_output_ref, handle$job_id, "values",
    required_label = "dsImaging")$name, "values")
  for (label in c("ds", "dsimaging", "dsImaging ")) {
    expect_error(trusted_hpc_call(get_job_output_ref, handle$job_id, "values",
      required_label = label), "does not belong", fixed = TRUE)
  }
})

test_that("publisher registration denies replacement and enforces its label", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home), add = TRUE)
  publisher_env <- dsHPC:::.dshpc_env
  publishers <- publisher_env$.publishers
  on.exit(publisher_env$.publishers <- publishers, add = TRUE)

  first <- function(...) list(status = "first")
  second <- function(...) list(status = "second")
  kind <- paste0("owned_publisher_", Sys.getpid())
  trusted_hpc_call(register_dshpc_publisher, kind, first)
  expect_error(trusted_hpc_call(register_dshpc_publisher, kind, second),
    "already exists")
  expect_silent(trusted_hpc_call(register_dshpc_publisher, kind, second,
    overwrite = TRUE))

  publisher_env$.publishers[[kind]] <- list(
    fn = second, owner = "dsImaging")
  expect_error(trusted_hpc_call(register_dshpc_publisher, kind, first,
    overwrite = TRUE), "owned by another server package")

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  for (label in c("dsImaging_image", "dsRadiomics")) {
    job_id <- paste0("job_", label)
    spec <- list(label = label, steps = list(list(type = "emit",
      plane = "session", output_name = "x", value = 1:3)))
    dsHPC:::.store_create_job(db, job_id, "owner", spec, 1L)
    dsHPC:::.ensure_step_dir(job_id, 1L)
  }
  output <- file.path(home, "artifacts", "job_dsImaging_image",
    "step_001", "output")
  expect_equal(dsHPC:::.execute_publish("job_dsImaging_image",
    list(publish_kind = kind), output, db)$status, "second")
  other_output <- file.path(home, "artifacts", "job_dsRadiomics",
    "step_001", "output")
  expect_error(dsHPC:::.execute_publish("job_dsRadiomics",
    list(publish_kind = kind), other_output, db),
    "not registered for this job label")
})
