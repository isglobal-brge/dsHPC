test_that("external runner registry installs allowlisted YAML runners", {
  home <- setup_test_home()
  reg <- tempfile("runner_registry_")
  dir.create(reg, recursive = TRUE)
  withr::local_options(list(
    dshpc.home = home,
    dshpc.runner_registry_paths = reg,
    dshpc.runner_registry_sync_secs = 0
  ))

  writeLines(c(
    "schema_version: 1",
    "runners:",
    "  hospital_echo:",
    "    command: python",
    "    args_template:",
    "      - -c",
    "      - print('ok')",
    "    allowed_params:",
    "      - patient_group",
    "    resources:",
    "      memory_mb: 128",
    "      cpu_slots: 1"
  ), file.path(reg, "hospital.yml"))

  expect_true(dsHPC:::.dshpc_sync_runner_registries(force = TRUE, quiet = FALSE))
  cfg <- dsHPC:::.load_runner_config("hospital_echo")
  expect_equal(cfg$name, "hospital_echo")
  expect_equal(cfg$allowed_params, "patient_group")
  expect_true(file.exists(file.path(home, "runners", "hospital_echo.yml")))
})

test_that("runner registry rejects unsafe runner names", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  expect_error(
    dsHPC:::register_dshpc_runner(list(
      name = "bad-name",
      command = "python",
      args_template = character(0)
    )),
    "only letters"
  )
})
