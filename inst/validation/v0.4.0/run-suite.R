args <- commandArgs(trailingOnly = TRUE)
.libPaths(c(args[[3]], .libPaths()))
results <- testthat::test_local(args[[1]], load_package = "installed",
  reporter = "summary", stop_on_failure = FALSE)
rows <- as.data.frame(results)
rows$result <- NULL
utils::write.csv(rows, args[[2]], row.names = FALSE)
counts <- c(blocks = nrow(rows), expectations = sum(rows$nb),
  passed = sum(rows$passed), failures = sum(rows$failed),
  errors = sum(rows$error), warnings = sum(rows$warning), skips = sum(rows$skipped))
cat("\nVALIDATION_COUNTS\n")
print(counts)
cat(R.version.string, "\n", R.version$platform, "\n")
if (sum(rows$failed) + sum(rows$error) > 0L) quit(status = 1L)
