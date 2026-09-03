private_mode <- function(path) {
  info <- file.info(path)
  !is.na(info$mode) && bitwAnd(as.integer(info$mode), 7L) == 0L
}

test_that("job artifacts and results deny access to other OS users", {
  skip_on_os("windows")
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home))

  step_dir <- dsHPC:::.ensure_step_dir("job_private", 1L)
  expect_true(private_mode(file.path(home, "artifacts", "job_private")))
  expect_true(private_mode(step_dir))
  expect_true(private_mode(file.path(step_dir, "output")))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- make_test_spec()
  dsHPC:::.store_create_job(
    db, "job_private", "owner", spec, 1L,
    access_token_hash = dsHPC:::.hash_job_capability("test-capability"))
  result <- dsHPC:::.build_job_result(db, "job_private")
  expect_true(result$ready)
  expect_true(private_mode(file.path(
    home, "artifacts", "job_private", "result")))
  expect_true(private_mode(file.path(
    home, "artifacts", "job_private", "result", "result.rds")))
  expect_true(private_mode(file.path(home, "dshpc.sqlite")))
})

test_that("persistent runner definitions are atomic and group-private", {
  skip_on_os("windows")
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home))

  path <- trusted_hpc_call(dsHPC::register_dshpc_runner, list(
    name = "private_runner",
    command = "python",
    args_template = character(0),
    allowed_params = character(0),
    resources = list(memory_mb = 128L, cpu_slots = 1L)))
  expect_true(private_mode(path))
  expect_false(any(grepl("^\\.runner-", list.files(dirname(path)))))
})

test_that("published artifacts and operational files are group-private", {
  skip_on_os("windows")
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- make_test_spec()
  dsHPC:::.store_create_job(db, "job_publish", "owner", spec, 1L)

  source <- file.path(
    dsHPC:::.ensure_step_dir("job_publish", 1L), "output")
  source_file <- file.path(source, "records.txt")
  writeLines("private", source_file)
  Sys.chmod(source_file, "0644", use_umask = FALSE)
  published <- dsHPC:::.publish_generic("job_publish", list(
    dataset_id = "dataset", asset_name = "asset", step_index = 1L),
    source, db)
  expect_true(private_mode(published$path))
  expect_true(private_mode(file.path(published$path, "records.txt")))

  step_dir <- dsHPC:::.ensure_step_dir("job_publish", 2L)
  dsHPC:::.backend_write_external_marker(step_dir, "external", "external-1")
  expect_true(private_mode(file.path(step_dir, "external_backend.json")))

  dsHPC:::.worker_write_health()
  expect_true(private_mode(file.path(home, "worker.health")))
})

test_that("staged input manifests and copied files are group-private", {
  skip_on_os("windows")
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home))

  source <- file.path(home, "source.txt")
  target <- file.path(home, "artifacts", "copied", "source.txt")
  writeLines("private", source)
  Sys.chmod(source, "0644", use_umask = FALSE)
  dsHPC:::.copy_input_tree(source, target)
  expect_true(private_mode(target))

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- make_test_spec(2L)
  dsHPC:::.store_create_job(db, "job_stage", "owner", spec, 2L)
  source_step <- dsHPC:::.ensure_step_dir("job_stage", 1L)
  writeLines("private", file.path(source_step, "output", "values.txt"))
  dsHPC:::.store_update_step(db, "job_stage", 1L,
    output_ref = file.path("artifacts", "job_stage", "step_001", "output"))
  target_step <- dsHPC:::.ensure_step_dir("job_stage", 2L)
  input_root <- dsHPC:::.stage_step_inputs(db, "job_stage",
    list(list(step = 1L, name = "source")), target_step)
  expect_true(private_mode(file.path(input_root, "inputs.json")))
})

test_that("generated worker scripts set a private umask", {
  entrypoint_path <- system.file("docker", "worker-entrypoint.sh",
    package = "dsHPC")
  if (!nzchar(entrypoint_path)) {
    entrypoint_path <- testthat::test_path(
      "..", "..", "inst", "docker", "worker-entrypoint.sh")
  }
  entrypoint <- readLines(entrypoint_path, warn = FALSE)
  expect_true(any(grepl("^umask 0007$", entrypoint)))
  expect_false(any(grepl("chmod 0777", entrypoint, fixed = TRUE)))

  configure_path <- testthat::test_path("..", "..", "configure")
  if (file.exists(configure_path)) {
    configure <- readLines(configure_path, warn = FALSE)
    expect_true(any(grepl("^umask 0007$", configure)))
    expect_false(any(grepl("chmod 0777", configure, fixed = TRUE)))
  }

  systemd_path <- system.file("systemd", "dshpc-worker.service",
    package = "dsHPC")
  if (!nzchar(systemd_path)) {
    systemd_path <- testthat::test_path(
      "..", "..", "inst", "systemd", "dshpc-worker.service")
  }
  systemd <- readLines(systemd_path, warn = FALSE)
  expect_true(any(grepl("^UMask=0007$", systemd)))
})

test_that("legacy permissions are remediated without following symlinks", {
  skip_on_os("windows")
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home))
  external <- tempfile("dshpc_external_")
  dir.create(external)
  on.exit(unlink(external, recursive = TRUE), add = TRUE)

  legacy_dir <- file.path(home, "artifacts", "legacy", "nested")
  dir.create(legacy_dir, recursive = TRUE)
  legacy_file <- file.path(legacy_dir, "result.rds")
  writeLines("legacy", legacy_file)
  external_file <- file.path(external, "outside.txt")
  writeLines("outside", external_file)
  Sys.chmod(c(home, dirname(legacy_dir), legacy_dir), "0755",
            use_umask = FALSE)
  Sys.chmod(c(legacy_file, external_file), "0644", use_umask = FALSE)
  Sys.chmod(external, "0755", use_umask = FALSE)

  unlink(file.path(home, "publish"), recursive = TRUE)
  expect_true(file.symlink(external, file.path(home, "publish")))
  expect_true(file.symlink(
    external_file, file.path(legacy_dir, "outside-file")))
  expect_true(file.symlink(
    external, file.path(home, "artifacts", "outside-directory")))

  lib <- dirname(find.package("dsHPC"))
  version <- as.character(utils::packageVersion("dsHPC", lib.loc = lib))
  dsHPC:::.onLoad(lib, "dsHPC")

  expect_equal(bitwAnd(as.integer(file.info(legacy_dir)$mode), 511L),
               strtoi("770", base = 8L))
  expect_equal(bitwAnd(as.integer(file.info(legacy_file)$mode), 511L),
               strtoi("660", base = 8L))
  expect_equal(bitwAnd(as.integer(file.info(external)$mode), 511L),
               strtoi("755", base = 8L))
  expect_equal(bitwAnd(as.integer(file.info(external_file)$mode), 511L),
               strtoi("644", base = 8L))

  marker <- file.path(home, paste0(".permissions-remediated-", version))
  expect_true(file.exists(marker))
  expect_equal(bitwAnd(as.integer(file.info(marker)$mode), 511L),
               strtoi("660", base = 8L))

  Sys.chmod(legacy_file, "0644", use_umask = FALSE)
  dsHPC:::.onLoad(lib, "dsHPC")
  expect_equal(bitwAnd(as.integer(file.info(legacy_file)$mode), 511L),
               strtoi("644", base = 8L))
})
