test_that("step constructors return dshpc_step S3 objects", {
  s <- dsHPC:::ds_step_assign_table("D", "my_data")
  expect_s3_class(s, "dshpc_step")
  expect_equal(s$type, "assign_table")
  expect_equal(s$plane, "session")
  expect_equal(s$table, "D")
  expect_equal(s$symbol, "my_data")
})

test_that("artifact step has runner field", {
  s <- dsHPC:::ds_step_run_artifact("pyradiomics", config = list(mask = "auto"))
  expect_equal(s$plane, "artifact")
  expect_equal(s$runner, "pyradiomics")
  expect_equal(s$config$mask, "auto")
})

test_that("inputs field is preserved", {
  s <- dsHPC:::ds_step_run_artifact("stage_parquet", inputs = list(2L))
  expect_equal(s$inputs, list(2L))
})

test_that("publish_asset has correct fields", {
  s <- dsHPC:::ds_step_publish_asset("dataset.v1", "radiomics", "radiomics")
  expect_equal(s$type, "publish_asset")
  expect_equal(s$dataset_id, "dataset.v1")
  expect_equal(s$asset_name, "radiomics")
})

test_that("safe_summary has no extra args", {
  s <- dsHPC:::ds_step_safe_summary()
  expect_equal(s$type, "safe_summary")
  expect_equal(s$plane, "session")
})

test_that("step print works", {
  s <- dsHPC:::ds_step_run_artifact("pyradiomics")
  expect_output(print(s), "dshpc_step")
  expect_output(print(s), "artifact")
  expect_output(print(s), "pyradiomics")
})

test_that("all session step types exist", {
  expect_s3_class(dsHPC:::ds_step_assign_table("t", "s"), "dshpc_step")
  expect_s3_class(dsHPC:::ds_step_assign_resource("r", "s"), "dshpc_step")
  expect_s3_class(dsHPC:::ds_step_assign_expr("expr", "s"), "dshpc_step")
  expect_s3_class(dsHPC:::ds_step_aggregate("expr"), "dshpc_step")
  expect_s3_class(dsHPC:::ds_step_emit("out"), "dshpc_step")
  expect_s3_class(dsHPC:::ds_step_resolve_dataset("ds.v1"), "dshpc_step")
  expect_s3_class(dsHPC:::ds_step_safe_summary(), "dshpc_step")
  expect_s3_class(dsHPC:::ds_step_publish_asset("ds", "name", "type"),
    "dshpc_step")
  expect_s3_class(dsHPC:::ds_step_publish_dataset("ds", "title", "mod"),
    "dshpc_step")
})

test_that("ds_job creates valid job object", {
  job <- dsHPC:::ds_job(
    steps = list(
      dsHPC:::ds_step_resolve_dataset("dataset.v1"),
      dsHPC:::ds_step_run_artifact("pyradiomics"),
      dsHPC:::ds_step_safe_summary()
    ),
    label = "dsImaging",
    resource_class = "cpu_heavy"
  )
  expect_s3_class(job, "dshpc_job")
  expect_equal(length(job$steps), 3L)
  expect_equal(job$label, "dsImaging")
  expect_equal(job$resource_class, "cpu_heavy")
})

test_that("ds_job requires a label", {
  expect_error(
    dsHPC:::ds_job(steps = list(dsHPC:::ds_step_emit("out"))),
    "non-empty domain label"
  )
})

test_that("ds_job rejects empty steps", {
  expect_error(dsHPC:::ds_job(steps = list(), label = "dsImaging"),
    "at least one step")
})

test_that("ds_job rejects non-step objects", {
  expect_error(
    dsHPC:::ds_job(steps = list(list(type = "fake")), label = "dsImaging"),
    "not a dshpc_step"
  )
})

test_that("ds_job strips S3 classes from steps for serialization", {
  job <- dsHPC:::ds_job(steps = list(dsHPC:::ds_step_emit("out")),
    label = "dsImaging")
  expect_false(inherits(job$steps[[1]], "dshpc_step"))
})

test_that("ds_job with publish config", {
  job <- dsHPC:::ds_job(
    steps = list(dsHPC:::ds_step_emit("out")),
    label = "dsImaging",
    publish = list(dataset_id = "ds.v1", asset_name = "features")
  )
  expect_equal(job$publish$dataset_id, "ds.v1")
})

test_that("job print works", {
  job <- dsHPC:::ds_job(
    steps = list(
      dsHPC:::ds_step_resolve_dataset("ds"),
      dsHPC:::ds_step_run_artifact("pyradiomics")
    ),
    label = "dsImaging"
  )
  expect_output(print(job), "dshpc_job")
  expect_output(print(job), "Steps: 2")
})

test_that("ds_job accepts DAG pipelines", {
  resolve_node <- dsHPC:::ds_pipeline_node("resolve",
    dsHPC:::ds_step_resolve_dataset("study.v1"))
  pipeline <- dsHPC:::ds_pipeline(list(
    resolve_node,
    dsHPC:::ds_pipeline_node("extract",
      dsHPC:::ds_step_run_artifact("dummy_runner"), inputs = "resolve"),
    dsHPC:::ds_pipeline_node("summary", dsHPC:::ds_step_safe_summary(),
      inputs = "extract")
  ))

  job <- dsHPC:::ds_job(pipeline = pipeline, label = "dsImaging")
  expect_s3_class(job, "dshpc_job")
  expect_equal(length(job$dag$nodes), 3L)
  expect_null(job$steps)
  expect_output(print(job), "DAG nodes: 3")
  expect_output(print(resolve_node), "dshpc_pipeline_node")
})
