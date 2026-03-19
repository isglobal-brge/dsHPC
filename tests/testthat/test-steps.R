test_that("step constructors return dsjobs_step S3 objects", {
  s <- ds_step_assign_table("D", "my_data")
  expect_s3_class(s, "dsjobs_step")
  expect_equal(s$type, "assign_table")
  expect_equal(s$plane, "session")
  expect_equal(s$table, "D")
  expect_equal(s$symbol, "my_data")
})

test_that("artifact step has runner field", {
  s <- ds_step_run_artifact("pyradiomics", config = list(mask = "auto"))
  expect_equal(s$plane, "artifact")
  expect_equal(s$runner, "pyradiomics")
  expect_equal(s$config$mask, "auto")
})

test_that("inputs field is preserved", {
  s <- ds_step_run_artifact("stage_parquet", inputs = list(2L))
  expect_equal(s$inputs, list(2L))
})

test_that("publish_asset has correct fields", {
  s <- ds_step_publish_asset("dataset.v1", "radiomics", "radiomics")
  expect_equal(s$type, "publish_asset")
  expect_equal(s$dataset_id, "dataset.v1")
  expect_equal(s$asset_name, "radiomics")
})

test_that("safe_summary has no extra args", {
  s <- ds_step_safe_summary()
  expect_equal(s$type, "safe_summary")
  expect_equal(s$plane, "session")
})

test_that("step print works", {
  s <- ds_step_run_artifact("pyradiomics")
  expect_output(print(s), "dsjobs_step")
  expect_output(print(s), "artifact")
  expect_output(print(s), "pyradiomics")
})

test_that("all session step types exist", {
  expect_s3_class(ds_step_assign_table("t", "s"), "dsjobs_step")
  expect_s3_class(ds_step_assign_resource("r", "s"), "dsjobs_step")
  expect_s3_class(ds_step_assign_expr("expr", "s"), "dsjobs_step")
  expect_s3_class(ds_step_aggregate("expr"), "dsjobs_step")
  expect_s3_class(ds_step_emit("out"), "dsjobs_step")
  expect_s3_class(ds_step_resolve_dataset("ds.v1"), "dsjobs_step")
  expect_s3_class(ds_step_safe_summary(), "dsjobs_step")
  expect_s3_class(ds_step_publish_asset("ds", "name", "type"), "dsjobs_step")
  expect_s3_class(ds_step_publish_dataset("ds", "title", "mod"), "dsjobs_step")
})
