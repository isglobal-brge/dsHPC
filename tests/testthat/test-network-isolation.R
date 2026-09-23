network_test_prepared <- function(home) {
  list(command = "/bin/sh", args = c("-c", "printf '%s' \"$DSHPC_CFG_VALUE\""),
    step_dir = home, output_dir = file.path(home, "output"),
    env_vars = c(DSHPC_CFG_VALUE = "literal $value; with spaces"))
}

test_that("Docker and Podman builders retain their isolated network default", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  withr::local_options(dshpc.home = home)
  settings <- dsHPC:::.dshpc_settings()
  settings$container_run_as_current_user <- FALSE
  prepared <- network_test_prepared(home)
  container <- list(image = "fixture:latest", pull = "never")
  for (name in c("docker", "podman")) {
    runtime <- list(name = name, command = paste0("/usr/bin/", name))
    command <- dsHPC:::.backend_docker_like_command(runtime, container,
      prepared, settings)
    expect_match(command, "'--network' 'none'", fixed = TRUE)
    expect_match(command, "'run' '--rm'", fixed = TRUE)
    settings$container_network <- "host"
    command <- dsHPC:::.backend_docker_like_command(runtime, container,
      prepared, settings)
    expect_match(command, "'--network' 'host'", fixed = TRUE)
    settings$container_network <- "none"
  }
})

test_that("Apptainer and Singularity builders verify isolated execution", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  withr::local_options(dshpc.home = home)
  settings <- dsHPC:::.dshpc_settings()
  prepared <- network_test_prepared(home)
  container <- list(image = "fixture.sif")
  for (name in c("apptainer", "singularity")) {
    runtime <- list(name = name, command = paste0("/usr/bin/", name))
    command <- dsHPC:::.backend_apptainer_command(runtime, container,
      prepared, settings)
    expect_match(command, "'--containall' '--net' '--network' 'none'", fixed = TRUE)
    expect_match(command, "/proc/self/ns/net", fixed = TRUE)
    expect_match(command, "/proc/net/dev", fixed = TRUE)
    expect_match(command, "'DSHPC_CFG_VALUE=literal $value; with spaces'", fixed = TRUE)
    expect_match(command, "No network-enabled fallback was attempted", fixed = TRUE)
    settings$container_network <- "host"
    host <- dsHPC:::.backend_apptainer_command(runtime, container, prepared, settings)
    expect_false(grepl("--net", host, fixed = TRUE))
    expect_false(grepl("dshpc-network-guard", host, fixed = TRUE))
    settings$container_network <- "none"
    for (extra in list(c("--network", "bridge"), "--network=host", "--net=false",
                       "--netns-path=/proc/1/ns/net", "-n")) {
      container$extra_args <- extra
      expect_error(dsHPC:::.backend_apptainer_command(runtime, container,
        prepared, settings), "cannot be overridden")
    }
    container$extra_args <- NULL
  }
})

test_that("Apptainer runtime refusal fails closed and preserves its exit status", {
  skip_on_os("windows")
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  withr::local_options(dshpc.home = home)
  bin <- file.path(home, "bin")
  dir.create(bin)
  runtime <- file.path(bin, "apptainer")
  readlink <- file.path(bin, "readlink")
  marker <- file.path(home, "runner-was-started")
  invocation <- file.path(home, "runtime-args")
  writeLines(c("#!/bin/sh", "echo 'net:[1]'"), readlink)
  writeLines(c("#!/bin/sh",
    paste0("printf '%s\\n' \"$@\" > ", shQuote(invocation)),
    "echo 'network namespaces are disabled' >&2", "exit 42"), runtime)
  Sys.chmod(c(runtime, readlink), "0755")
  prepared <- network_test_prepared(home)
  prepared$command <- "/usr/bin/touch"
  prepared$args <- marker
  prepared$env_vars <- c(PATH = paste(bin, Sys.getenv("PATH"), sep = .Platform$path.sep))
  prepared$runner_config <- list(container = list(image = "fixture.sif", runtime = "apptainer"))
  withr::local_envvar(c(PATH = prepared$env_vars[["PATH"]]))
  script <- file.path(home, "run_step.sh")
  dsHPC:::.backend_write_step_script(script, prepared)
  result <- processx::run("/bin/sh", script, error_on_status = FALSE)
  expect_equal(result$status, 42L)
  expect_match(result$stderr, "network isolation", fixed = TRUE)
  expect_match(result$stderr, "user/network namespaces", fixed = TRUE)
  expect_false(file.exists(marker))
  expect_equal(readLines(file.path(home, "exit_code")), "42")
  expect_equal(sum(readLines(invocation) == "exec"), 1L)
  unlink(invocation)
  writeLines(c("#!/bin/sh", "exit 1"), readlink)
  unsupported <- processx::run("/bin/sh", script, error_on_status = FALSE)
  expect_equal(unsupported$status, 125L)
  expect_match(unsupported$stderr, "network isolation", fixed = TRUE)
  expect_false(file.exists(invocation))
  expect_false(file.exists(marker))
})

test_that("a runtime silently ignoring isolation never executes the runner", {
  skip_on_os("windows")
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  withr::local_options(dshpc.home = home)
  bin <- file.path(home, "bin")
  dir.create(bin)
  runtime <- file.path(bin, "singularity")
  readlink <- file.path(bin, "readlink")
  marker <- file.path(home, "runner-was-started")
  writeLines(c("#!/bin/sh", "echo 'net:[1]'"), readlink)
  writeLines(c("#!/bin/sh",
    "while [ \"$#\" -gt 0 ] && [ \"$1\" != fixture.sif ]; do shift; done",
    "shift", "exec \"$@\""), runtime)
  Sys.chmod(c(runtime, readlink), "0755")
  prepared <- network_test_prepared(home)
  prepared$command <- "/usr/bin/touch"
  prepared$args <- marker
  prepared$env_vars <- c(PATH = paste(bin, Sys.getenv("PATH"), sep = .Platform$path.sep))
  prepared$runner_config <- list(container = list(image = "fixture.sif", runtime = "singularity"))
  withr::local_envvar(c(PATH = prepared$env_vars[["PATH"]]))
  script <- file.path(home, "run_step.sh")
  dsHPC:::.backend_write_step_script(script, prepared)
  result <- processx::run("/bin/sh", script, error_on_status = FALSE)
  expect_equal(result$status, 125L)
  expect_match(result$stderr, "refusing to execute runner", fixed = TRUE)
  expect_false(file.exists(marker))
  expect_equal(readLines(file.path(home, "exit_code")), "125")
})

test_that("network guard requires a distinct loopback-only namespace", {
  skip_on_os("windows")
  home <- setup_test_home()
  on.exit(cleanup_test_home(home))
  bin <- file.path(home, "bin")
  dir.create(bin)
  readlink <- file.path(bin, "readlink")
  writeLines(c("#!/bin/sh", "echo 'net:[2]'"), readlink)
  Sys.chmod(readlink, "0755")
  netdev <- file.path(home, "net-dev")
  guard <- dsHPC:::.backend_apptainer_network_guard()
  # Substitute the procfs fixture only in the test; production paths are fixed.
  guard <- gsub("/proc/net/dev", shQuote(netdev), guard, fixed = TRUE)
  script <- file.path(home, "guard.sh")
  writeLines(guard, script)
  env <- c(PATH = paste(bin, Sys.getenv("PATH"), sep = .Platform$path.sep))
  args <- c(script, "net:[1]", "/usr/bin/env", "DSHPC_CFG_VALUE=literal $value; with spaces",
    "/bin/sh", "-c", "printf '%s' \"$DSHPC_CFG_VALUE\"")
  writeLines(c("Inter-| Receive", "    lo:123456789 0 0 0"), netdev)
  allowed <- processx::run("/bin/sh", args, env = env, error_on_status = FALSE)
  expect_equal(allowed$status, 0L)
  expect_equal(allowed$stdout, "literal $value; with spaces")
  writeLines(c("    lo: 0 0 0 0", "  eth0:123456789 0 0 0"), netdev)
  connected <- processx::run("/bin/sh", args, env = env, error_on_status = FALSE)
  expect_equal(connected$status, 125L)
  expect_equal(connected$stdout, "")
  unlink(netdev)
  missing <- processx::run("/bin/sh", args, env = env, error_on_status = FALSE)
  expect_equal(missing$status, 125L)
  expect_match(missing$stderr, "cannot verify container network isolation", fixed = TRUE)
})
