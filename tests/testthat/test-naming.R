test_that("job name templates use the next visible number", {
  expect_equal(dsHPC:::.job_name_next_number(
    c("LUNG1 1 radiomics", "LUNG1 2 radiomics", "Other"),
    "LUNG1 {number} radiomics"), 3L)
  expect_equal(dsHPC:::.job_name_render_template(
    "run-{number}-qc", 12L), "run-12-qc")
})

test_that("job name template validation is strict", {
  expect_error(dsHPC:::.job_name_template_parts("A {number} {number}"),
    "at most one")
  expect_error(dsHPC:::.job_name_template_parts("A\n{number}"),
    "single-line")
})

test_that("job name templates are resolved when a job is stored", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  make_spec <- function(name, owner_label = "dsImaging",
                        visibility = "private") {
    dsHPC:::.validate_job_spec(list(
      name = name,
      label = owner_label,
      visibility = visibility,
      steps = list(list(type = "emit", plane = "session",
        output_name = "out", value = 1))
    ))
  }

  dsHPC:::.store_create_job(db, "job_alice_1", "alice",
    make_spec("LUNG1 1 radiomics"), 1L)
  dsHPC:::.store_create_job(db, "job_alice_2", "alice",
    make_spec("LUNG1 2 radiomics"), 1L)
  dsHPC:::.store_create_job(db, "job_bob_private_99", "bob",
    make_spec("LUNG1 99 radiomics"), 1L)
  dsHPC:::.store_create_job(db, "job_bob_global_3", "bob",
    make_spec("LUNG1 3 radiomics", visibility = "global"), 1L)
  dsHPC:::.store_create_job(db, "job_other_label_10", "alice",
    make_spec("LUNG1 10 radiomics", owner_label = "dsOther"), 1L)

  dsHPC:::.store_create_job(db, "job_alice_next", "alice",
    make_spec("LUNG1 {number} radiomics"), 1L)

  job <- dsHPC:::.store_get_job(db, "job_alice_next")
  expect_equal(job$name, "LUNG1 4 radiomics")
  expect_equal(dsHPC:::.store_get_spec(db, "job_alice_next")$name,
    "LUNG1 4 radiomics")
})

test_that("hpcSubmitDS resolves job name templates", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))

  first <- hpcSubmitDS(list(
    .owner = "alice",
    job_id = "job_template_1",
    name = "Aerts signature {number}",
    label = "dsImaging",
    visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "out", value = 1))
  ))
  second <- hpcSubmitDS(list(
    .owner = "alice",
    job_id = "job_template_2",
    name = "Aerts signature {number}",
    label = "dsImaging",
    visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "out", value = 2))
  ))

  expect_equal(first$name, "Aerts signature 1")
  expect_equal(second$name, "Aerts signature 2")
})
