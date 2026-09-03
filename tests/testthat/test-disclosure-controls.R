test_that("log tails reject invalid limits and never exceed 200 lines", {
  lines <- paste0("line-", seq_len(250L))

  expect_length(dsHPC:::.sanitize_job_logs(lines, 0L), 0L)
  expect_length(dsHPC:::.sanitize_job_logs(lines, -2L), 0L)
  expect_length(dsHPC:::.sanitize_job_logs(lines, NA_integer_), 0L)
  expect_length(dsHPC:::.sanitize_job_logs(lines, Inf), 0L)
  expect_length(dsHPC:::.sanitize_job_logs(lines, 10000L), 200L)
  expect_equal(dsHPC:::.sanitize_job_logs(lines, 2L), c("line-249", "line-250"))
})

test_that("session summaries suppress counts below nfilter.subset", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, nfilter.subset = 5))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(label = "privacy-test", visibility = "private",
    steps = list(list(type = "safe_summary", plane = "session")))
  dsHPC:::.store_create_job(db, "job_summary", "alice", spec, 1L)
  step_dir <- dsHPC:::.ensure_step_dir("job_summary", 1L)
  input_dir <- file.path(home, "summary-input")
  dir.create(input_dir)
  csv_path <- file.path(input_dir, "small.csv")
  utils::write.csv(data.frame(x = c("a\nb\nc", "d\ne\nf")), csv_path,
    row.names = FALSE)
  expect_gte(length(readLines(csv_path, warn = FALSE)) - 1L, 5L)
  writeLines("aux", file.path(input_dir, "aux.txt"))

  summary <- dsHPC:::.session_safe_summary(
    "job_summary", step_dir, input_dir, db, 1L)

  expect_true(is.na(summary$n_output_files))
  expect_false("job_id" %in% names(summary))
  expect_true(is.na(summary$n_samples))
  expect_false("output_size_bytes" %in% names(summary))
  expect_true(is.na(dsHPC:::.safe_summary_count(0L)))
  expect_true(is.na(dsHPC:::.safe_summary_count(4L)))
  expect_gte(dsHPC:::.safe_summary_count(5L), 5L)
})

test_that("malformed nfilter settings cannot lower the privacy floor", {
  for (value in list(0, -1, NA_real_, Inf, c(1, 100), "invalid")) {
    withr::local_options(list(nfilter.subset = value))
    expect_gte(dsHPC:::.dshpc_disclosure_settings()$nfilter_subset, 3L)
    expect_true(is.na(dsHPC:::.safe_summary_count(2L)))
  }
})

test_that("deserialized output cardinality fails closed for lists and maps", {
  expect_equal(dsHPC:::.output_object_cardinality(data.frame(x = 1:5)), 5L)
  expect_equal(dsHPC:::.output_object_cardinality(matrix(1:6, nrow = 3)), 3L)
  expect_true(is.na(dsHPC:::.output_object_cardinality(
    array(1:27, dim = c(3, 3, 3)))))
  expect_equal(dsHPC:::.output_object_cardinality(letters[1:5]), 5L)
  expect_true(is.na(dsHPC:::.output_object_cardinality(list(x = 1:5))))
  expect_true(is.na(dsHPC:::.output_object_cardinality(
    list(list(id = 1), list(id = 2)))))
})

test_that("safe job errors never return runner-provided values", {
  expect_true(is.na(dsHPC:::.safe_job_error(NA_character_)))
  expect_equal(dsHPC:::.safe_job_error("patient Alice had value 42"),
    "Job execution failed.")
})

test_that("client-safe results enforce cardinality and the closed summary schema", {
  withr::local_options(list(nfilter.subset = 3))

  expect_false(dsHPC:::.dshpc_client_safe_value(1:2, "aggregate_result"))
  expect_true(dsHPC:::.dshpc_client_safe_value(1:3, "aggregate_result"))
  expect_false(dsHPC:::.dshpc_client_safe_value(
    list(message = "record-level value"), "summary"))
  expect_false(dsHPC:::.dshpc_client_safe_value(
    list(n_samples = 2L), "summary"))
  expect_false(dsHPC:::.dshpc_client_safe_value(
    list(n_samples = 6L), "summary"))
  expect_true(dsHPC:::.dshpc_client_safe_value(list(), "summary"))
  expect_false(dsHPC:::.dshpc_client_safe_value(
    list(n_samples = NaN), "summary"))
  expect_true(dsHPC:::.dshpc_client_safe_value(
    list(n_samples = 4L, n_output_files = NA_integer_), "summary"))
})
