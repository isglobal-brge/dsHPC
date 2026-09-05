make_capability_job <- function(home, visibility = "private") {
  withr::local_options(list(dshpc.home = home, nfilter.subset = 3))
  trusted_hpc_call(hpcSubmitInternal, list(
    .owner = "alice",
    label = "privacy-test",
    visibility = visibility,
    steps = list(list(type = "emit", plane = "session",
      output_name = "values", value = seq_len(5L)))
  ))
}

test_that("per-job capabilities protect every analyst read surface", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, nfilter.subset = 3))
  on.exit(cleanup_test_home(home))

  handle <- make_capability_job(home)
  expect_match(handle$.dshpc_capability, "^cap_[0-9a-f]{64}$")

  db <- dsHPC:::.db_connect()
  job <- dsHPC:::.store_get_job(db, handle$job_id)
  expect_identical(job$access_token_hash,
    dsHPC:::.hash_job_capability(handle$.dshpc_capability))
  expect_false(grepl(handle$.dshpc_capability, job$spec_json, fixed = TRUE))
  dsHPC:::.db_close(db)

  bearer <- hpcJobReferenceDS(handle)
  expect_match(bearer, "^B64:")
  expect_identical(hpcJobReferenceDS(bearer), bearer)

  status <- hpcStatusDS(handle)
  expect_named(status, c("state", "is_done", "error"))
  expect_true(status$is_done)
  expect_true(is.na(status$error))
  expect_equal(hpcStatusDS(bearer)$state, "FINISHED")
  expect_equal(trusted_hpc_call(hpcLoadOutputInternal, bearer, "values",
    required_label = "privacy-test"), seq_len(5L))
  expect_error(trusted_hpc_call(hpcLoadOutputInternal, bearer, "values",
    required_label = "privacy"), "does not belong", fixed = TRUE)
  expect_error(trusted_hpc_call(hpcLoadOutputInternal, bearer, "values",
    required_label = "Privacy-test"), "does not belong", fixed = TRUE)
  expect_error(trusted_hpc_call(hpcLoadOutputInternal, bearer, "values",
    required_label = "privacy-test "), "does not belong", fixed = TRUE)

  for (fn in list(hpcJobReferenceDS, hpcStatusDS, hpcResultDS, hpcLogsDS,
                  hpcOutputsDS)) {
    raw_job_id <- handle$job_id
    expect_error(fn(raw_job_id), "Job not found or access denied", fixed = TRUE)
  }
  expect_error(
    trusted_hpc_call(hpcLoadOutputInternal, handle$job_id, "values",
      required_label = "privacy-test"),
    "Job not found or access denied", fixed = TRUE)

  bad <- handle
  bad$.dshpc_capability <- paste0("cap_", strrep("0", 64L))
  expect_error(hpcJobReferenceDS(bad), "Job not found or access denied",
    fixed = TRUE)
  expect_error(hpcStatusDS(bad), "Job not found or access denied", fixed = TRUE)
  expect_error(hpcStatusDS("B64:not-valid"),
    "Job not found or access denied", fixed = TRUE)

  outputs <- hpcOutputsDS(handle)
  expect_true(all(is.na(outputs$size_bytes)))
  expect_true(all(outputs$safe_for_client))
  expect_length(hpcLogsDS(handle, 10000L), 0L)

  db <- dsHPC:::.db_connect()
  dsHPC:::.store_update_job(db, handle$job_id, state = "PENDING")
  dsHPC:::.db_close(db)
  pending_result <- hpcResultDS(handle)
  expect_named(pending_result, c("state", "ready", "error"))
  expect_false(any(c("job_id", "capability", ".dshpc_capability") %in%
    names(pending_result)))

  raw_error <- paste("patient-A", file.path(home, "records.csv"), "value=42")
  db <- dsHPC:::.db_connect()
  dsHPC:::.store_update_job(db, handle$job_id, state = "FAILED",
    error_message = raw_error)
  dsHPC:::.db_close(db)
  failed_status <- hpcStatusDS(handle)
  failed_result <- hpcResultDS(handle)
  expect_identical(failed_status$error, "Job execution failed.")
  expect_identical(failed_result$error, "Job execution failed.")
  expect_false(grepl("patient-A|records\\.csv|value=42",
    paste(capture.output(str(list(failed_status, failed_result))),
      collapse = "\n")))
})

test_that("legacy jobs without capability hashes fail closed", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  spec <- make_test_spec()
  dsHPC:::.store_create_job(db, "job_legacy", "alice", spec, 1L)
  dsHPC:::.db_close(db)

  forged <- list(job_id = "job_legacy", .dshpc_capability = "cap_forged")
  expect_error(hpcStatusDS(forged), "Job not found or access denied", fixed = TRUE)
})

test_that("output loading rejects unknown and small cardinalities", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, nfilter.subset = 3))
  on.exit(cleanup_test_home(home))
  handle <- make_capability_job(home)

  output_dir <- file.path(home, "artifacts", handle$job_id, "extra")
  dir.create(output_dir, recursive = TRUE)
  list_path <- file.path(output_dir, "unknown.rds")
  json_path <- file.path(output_dir, "unknown.json")
  small_path <- file.path(output_dir, "small.rds")
  saveRDS(list(secret = "single record"), list_path)
  writeLines('{"secret":"single record"}', json_path)
  saveRDS(1:2, small_path)

  db <- dsHPC:::.db_connect()
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "unknown_rds",
    "emit_value", list_path, safe_for_client = FALSE)
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "unknown_json",
    "artifact_file", json_path, safe_for_client = FALSE)
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "small_rds",
    "emit_value", small_path, safe_for_client = FALSE)
  dsHPC:::.db_close(db)

  expect_error(trusted_hpc_call(hpcLoadOutputInternal, handle, "unknown_rds",
    required_label = "privacy-test"), "cardinality cannot be established")
  expect_error(trusted_hpc_call(hpcLoadOutputInternal, handle, "unknown_json",
    required_label = "privacy-test"), "cardinality cannot be established")
  expect_error(trusted_hpc_call(hpcLoadOutputInternal, handle, "small_rds",
    required_label = "privacy-test"), "disclosure minimum")
  expect_error(trusted_hpc_call(hpcLoadOutputInternal, handle, "../values",
    required_label = "privacy-test"), "output_name")
})

test_that("output loading uses record counts and rejects non-tabular paths", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, nfilter.subset = 3))
  on.exit(cleanup_test_home(home))
  handle <- make_capability_job(home)

  output_dir <- file.path(home, "artifacts", handle$job_id, "extra")
  dir.create(output_dir, recursive = TRUE)
  csv_path <- file.path(output_dir, "multiline.csv")
  binary_path <- file.path(output_dir, "image.bin")
  utils::write.csv(data.frame(value = c("first\nrecord", "second\nrecord")),
    csv_path, row.names = FALSE)
  writeBin(as.raw(1:8), binary_path)

  # The CSV has enough physical lines to clear nfilter, but only two records.
  expect_gt(length(readLines(csv_path, warn = FALSE)) - 1L, 2L)
  db <- dsHPC:::.db_connect()
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "multiline_csv",
    "artifact_file", csv_path, safe_for_client = FALSE)
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "binary_asset",
    "artifact_file", binary_path, safe_for_client = FALSE)
  dsHPC:::.db_close(db)

  expect_error(trusted_hpc_call(hpcLoadOutputInternal, handle, "multiline_csv",
    required_label = "privacy-test"), "disclosure minimum")
  expect_error(trusted_hpc_call(hpcLoadOutputInternal, handle, "binary_asset",
    required_label = "privacy-test"), "cardinality cannot be established")
})

test_that("hpcResultDS requires both the safe flag and an allowed kind", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))
  handle <- make_capability_job(home)

  output_dir <- file.path(home, "artifacts", handle$job_id, "safe-tests")
  dir.create(output_dir, recursive = TRUE)
  unsafe_flag <- file.path(output_dir, "unsafe-flag.rds")
  unsafe_kind <- file.path(output_dir, "unsafe-kind.rds")
  safe <- file.path(output_dir, "safe.rds")
  saveRDS(list(secret = "unsafe flag"), unsafe_flag)
  saveRDS(list(secret = "unsafe kind"), unsafe_kind)
  saveRDS(list(n_samples = 4L), safe)

  db <- dsHPC:::.db_connect()
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "unsafe_flag",
    "summary", unsafe_flag, safe_for_client = FALSE)
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "unsafe_kind",
    "emit_value", unsafe_kind, safe_for_client = TRUE)
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "approved",
    "summary", safe, safe_for_client = TRUE)
  result_path <- file.path(home, "artifacts", handle$job_id, "result",
    "result.rds")
  saveRDS(list(job_id = handle$job_id,
    summaries = list(list(value = list(secret = "stale cache")))), result_path)
  dsHPC:::.db_close(db)

  result <- hpcResultDS(handle)
  expect_equal(vapply(result$summaries, `[[`, character(1), "name"),
    "approved")
  expect_equal(result$summaries[[1]]$value$n_samples, 4L)
  expect_equal(vapply(result$available_outputs, `[[`, character(1), "name"),
    "approved")
  expect_false("size_bytes" %in% names(result$available_outputs[[1]]))
  expect_false(any(c("job_id", "capability", ".dshpc_capability") %in%
    names(result)))
})

test_that("hpcResultDS rejects marked-safe values below the disclosure floor", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, nfilter.subset = 3))
  on.exit(cleanup_test_home(home))
  handle <- make_capability_job(home)

  output_dir <- file.path(home, "artifacts", handle$job_id, "safe-tests")
  dir.create(output_dir, recursive = TRUE)
  small <- file.path(output_dir, "small.rds")
  malformed <- file.path(output_dir, "malformed.rds")
  saveRDS(1:2, small)
  saveRDS(list(message = "secret-marker"), malformed)

  db <- dsHPC:::.db_connect()
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "small",
    "aggregate_result", small, safe_for_client = TRUE)
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "malformed",
    "summary", malformed, safe_for_client = TRUE)
  dsHPC:::.db_close(db)

  error <- tryCatch(hpcResultDS(handle), error = identity)
  expect_s3_class(error, "error")
  expect_identical(conditionMessage(error), "Job result is unavailable.")
  expect_false(grepl("secret-marker", conditionMessage(error), fixed = TRUE))
})

test_that("private jobs stay separate and shared jobs reuse one execution", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  private_spec <- list(.owner = "alice", label = "privacy-test",
    visibility = "private", steps = list(list(type = "emit", plane = "session",
      output_name = "values", value = 1:5)))
  first <- trusted_hpc_call(hpcSubmitInternal, private_spec)
  second <- trusted_hpc_call(hpcSubmitInternal, private_spec)
  expect_false(isTRUE(second$deduplicated))

  global_spec <- private_spec
  global_spec$visibility <- "global"
  global_spec$reuse_fingerprint <- strrep("a", 64L)
  global_first <- trusted_hpc_call(hpcSubmitInternal, global_spec)
  global_second <- trusted_hpc_call(hpcSubmitInternal, global_spec)
  expect_true(isTRUE(global_second$deduplicated))
  expect_identical(global_second$job_id, global_first$job_id)
  expect_identical(global_second$tracking_id, global_first$tracking_id)
  expect_null(global_second$.dshpc_capability)

  db <- dsHPC:::.db_connect()
  private_events <- DBI::dbGetQuery(db,
    "SELECT event FROM events WHERE job_id IN (?, ?)",
    params = list(first$job_id, second$job_id))
  expect_false("deduplicated" %in% private_events$event)
  global_jobs <- DBI::dbGetQuery(db,
    "SELECT job_id FROM jobs WHERE job_id = ?",
    params = list(global_first$job_id))
  dsHPC:::.db_close(db)
  expect_equal(nrow(global_jobs), 1L)
  expect_equal(hpcStatusDS(global_first)$state, "FINISHED")
})

test_that("whole-job dedup requires the exact persisted domain label", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home), add = TRUE)
  spec <- list(.owner = "alice", label = "dsHPC_target",
    visibility = "global", steps = list(list(
      type = "emit", plane = "session", output_name = "values", value = 1:5)))
  first <- trusted_hpc_call(hpcSubmitInternal, spec)

  # Preserve the exact spec hash but simulate corrupted/legacy ownership data.
  db <- dsHPC:::.db_connect()
  dsHPC:::.store_update_job(db, first$job_id, label = "dsHPC_other")
  dsHPC:::.db_close(db)

  second <- trusted_hpc_call(hpcSubmitInternal, spec)
  expect_false(isTRUE(second$deduplicated))
  expect_false(identical(first$job_id, second$job_id))
})

test_that("large artifact sizes remain exact internally and hidden publicly", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home), add = TRUE)
  handle <- make_capability_job(home)
  path <- file.path(home, "artifacts", handle$job_id, "large-summary.rds")
  saveRDS(list(n_samples = 4L), path)
  large_size <- 3 * 1024^3

  db <- dsHPC:::.db_connect()
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "large_summary",
    "summary", path, size_bytes = large_size, safe_for_client = TRUE)
  stored <- DBI::dbGetQuery(db,
    "SELECT size_bytes, typeof(size_bytes) AS storage_type
     FROM outputs WHERE job_id = ? AND name = 'large_summary'",
    params = list(handle$job_id))
  dsHPC:::.db_close(db)

  expect_equal(stored$size_bytes, large_size)
  expect_identical(stored$storage_type, "integer")
  ref <- trusted_hpc_call(get_job_output_ref, handle$job_id,
    "large_summary", required_label = "privacy-test")
  expect_equal(ref$size_bytes, large_size)
  public <- hpcOutputsDS(handle)
  expect_true(all(is.na(public$size_bytes)))
  result <- hpcResultDS(handle)
  expect_false(any(vapply(result$available_outputs,
    function(x) "size_bytes" %in% names(x), logical(1))))

  expect_error(dsHPC:::.normalize_output_size_bytes(-1), "non-negative")
  expect_error(dsHPC:::.normalize_output_size_bytes(1.5), "whole number")
  expect_error(dsHPC:::.normalize_output_size_bytes(2^53 + 2), "2\\^53")
})
