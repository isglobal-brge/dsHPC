test_that("DAG specs compile to ordered steps", {
  spec <- list(dag = list(nodes = list(
    ingest = list(type = "emit", plane = "session", output_name = "raw",
      value = 1),
    transform = list(type = "emit", plane = "session", output_name = "derived",
      value = 2, inputs = "ingest"),
    publish = list(type = "safe_summary", inputs = list(source = "transform"))
  )))

  out <- dsHPC:::.validate_job_spec(spec)
  expect_equal(length(out$steps), 3L)
  expect_equal(vapply(out$steps, `[[`, character(1), "node_id"),
    c("ingest", "transform", "publish"))
  expect_equal(out$steps[[2]]$inputs[[1]]$step, 1L)
  expect_equal(out$steps[[3]]$inputs[[1]]$name, "source")
  expect_equal(out$dag_compiled$node_step_index$transform, 2L)
})

test_that("DAG validation rejects cycles and unknown nodes", {
  cyclic <- list(dag = list(nodes = list(
    a = list(type = "emit", output_name = "a", inputs = "b"),
    b = list(type = "emit", output_name = "b", inputs = "a")
  )))
  expect_error(dsHPC:::.validate_job_spec(cyclic), "dependency cycle")

  unknown <- list(dag = list(nodes = list(
    a = list(type = "emit", output_name = "a", inputs = "missing")
  )))
  expect_error(dsHPC:::.validate_job_spec(unknown), "unknown node")
})

test_that("DAG and legacy steps are mutually exclusive", {
  spec <- list(
    steps = list(list(type = "emit", plane = "session", output_name = "x")),
    dag = list(nodes = list(a = list(type = "emit", output_name = "a")))
  )
  expect_error(dsHPC:::.validate_job_spec(spec), "both 'steps' and 'dag'")
})
