test_that("external runner registry installs allowlisted YAML runners", {
  home <- setup_test_home()
  reg <- tempfile("runner_registry_")
  dir.create(reg, recursive = TRUE)
  withr::local_options(list(
    dshpc.home = home,
    dshpc.runner_registry_paths = reg,
    dshpc.runner_registry_autosync = TRUE,
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
    trusted_hpc_call(dsHPC:::register_dshpc_runner, list(
      name = "bad-name",
      command = "python",
      args_template = character(0)
    )),
    "only letters"
  )
})

test_that("managed runners deny implicit and cross-owner replacement", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home))
  config <- list(name = "owned_runner", command = "/usr/bin/env",
    args_template = character(0), allowed_params = character(0))

  path <- trusted_hpc_call(register_dshpc_runner, config)
  expect_equal(yaml::read_yaml(path)$registered_by, "dsHPC")
  expect_length(list.files(file.path(home, "locks"),
    pattern = "^runner\\."), 0L)
  expect_error(trusted_hpc_call(register_dshpc_runner, config),
    "already exists")

  changed <- config
  changed$command <- "/bin/echo"
  expect_silent(trusted_hpc_call(register_dshpc_runner, changed,
    overwrite = TRUE))
  expect_equal(dsHPC:::.load_runner_config("owned_runner")$command,
    "/bin/echo")

  foreign <- changed
  foreign$registered_by <- "dsImaging"
  yaml::write_yaml(foreign, path)
  expect_error(trusted_hpc_call(register_dshpc_runner, changed,
    overwrite = TRUE), "owned by another server package")
})

test_that("writable runner registry ignores symbolic-link entries", {
  skip_on_os("windows")
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home,
    dshpc.runner_registry_autosync = FALSE))
  on.exit(cleanup_test_home(home))
  outside <- tempfile(fileext = ".yml")
  on.exit(unlink(outside, force = TRUE), add = TRUE)
  yaml::write_yaml(list(name = "linked_runner", command = "/bin/false",
    args_template = character(0), allowed_params = character(0)), outside)
  link <- file.path(home, "runners", "linked_runner.yml")
  skip_if_not(isTRUE(file.symlink(outside, link)),
    "symbolic links are unavailable")

  expect_null(dsHPC:::.load_runner_config("linked_runner"))
  expect_error(trusted_hpc_call(register_dshpc_runner, list(
    name = "linked_runner", command = "/bin/echo",
    args_template = character(0), allowed_params = character(0))),
    "symbolic link")
  expect_equal(yaml::read_yaml(outside)$command, "/bin/false")
})

test_that("built-in runners cannot be shadowed by the writable registry", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home,
    dshpc.runner_registry_autosync = FALSE))
  on.exit(cleanup_test_home(home))
  yaml::write_yaml(list(name = "stage_parquet", command = "/bin/false",
    args_template = character(0), allowed_params = character(0)),
    file.path(home, "runners", "stage_parquet.yml"))

  expect_false(identical(dsHPC:::.load_runner_config("stage_parquet")$command,
    "/bin/false"))
  expect_error(trusted_hpc_call(register_dshpc_runner, list(
    name = "stage_parquet", command = "/bin/false",
    args_template = character(0), allowed_params = character(0))),
    "cannot be replaced")
})

test_that("managed runner ownership is bound to the job label", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home,
    dshpc.runner_registry_autosync = FALSE))
  on.exit(cleanup_test_home(home))
  yaml::write_yaml(list(name = "imaging_owned", command = "/usr/bin/env",
    args_template = character(0), allowed_params = character(0),
    registered_by = "dsImaging"),
    file.path(home, "runners", "imaging_owned.yml"))
  make_spec <- function(label) list(label = label, steps = list(list(
    type = "run", plane = "artifact", runner = "imaging_owned",
    config = list())))

  expect_silent(dsHPC:::.validate_job_spec(make_spec("dsImaging_image")))
  expect_error(dsHPC:::.validate_job_spec(make_spec("dsRadiomics")),
    "not registered for this job label")
})
