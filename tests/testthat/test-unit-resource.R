.unit_test_command <- function() {
  command <- Sys.which("Rscript")
  if (!nzchar(command)) skip("Rscript executable is unavailable")
  normalizePath(unname(command), winslash = "/", mustWork = TRUE)
}

.unit_test_entry <- function(labels = "dsHPC", runners = character(0),
                             command = .unit_test_command(),
                             resource_pool_id = NULL,
                             runtime_revision = NULL) {
  entry <- list(
    type = "external",
    enabled = TRUE,
    allowed_labels = as.list(labels),
    allowed_runners = as.list(runners),
    config = list(
      external_submit_cmd = command,
      external_status_cmd = command,
      external_cancel_cmd = command))
  if (!is.null(runtime_revision)) {
    entry$config$runtime_revision <- runtime_revision
  }
  if (!is.null(resource_pool_id)) {
    entry$resource_pool_id <- resource_pool_id
  }
  entry
}

.unit_test_catalog <- function(units) {
  path <- tempfile("dshpc-units-", fileext = ".yml")
  yaml::write_yaml(list(schema_version = 1L, units = units), path)
  path
}

.unit_test_resource <- function(unit_id = "unit_alpha", secret = NULL,
                                url = NULL, format = NULL) {
  if (is.null(url)) url <- paste0("dshpc+unit://external/", unit_id)
  resourcer::newResource(
    name = "opaque-resource-name",
    url = url,
    secret = secret,
    format = format)
}

test_that("execution units seal an immutable runtime revision", {
  revision <- strrep("a", 64L)
  catalog <- .unit_test_catalog(list(unit_alpha = .unit_test_entry(
    runtime_revision = revision)))
  withr::local_options(list(dshpc.units_file = catalog))
  selection <- dsHPC:::.dshpc_unit_from_resource(.unit_test_resource())
  expect_identical(selection$config$runtime_revision, revision)

  spec <- list(.dshpc_unit = selection)
  expect_identical(dsHPC:::.dshpc_runtime_revision(spec), revision)
  spec$.dshpc_runtime_revision <- revision
  expect_true(dsHPC:::.dshpc_runtime_reuse_ready(spec))

  invalid <- .unit_test_entry(runtime_revision = "mutable-tag")
  invalid_catalog <- .unit_test_catalog(list(unit_alpha = invalid))
  withr::local_options(list(dshpc.units_file = invalid_catalog))
  expect_error(dsHPC:::.dshpc_unit_from_resource(.unit_test_resource()),
    "HPC unit resource is unavailable", fixed = TRUE)
})

.unit_test_dslite_config <- function() {
  config <- DSLite::defaultDSConfiguration()
  description <- system.file("DESCRIPTION", package = "dsHPC")
  raw <- read.dcf(description, fields = "AssignMethods")[1, 1]
  methods <- trimws(strsplit(raw, ",", fixed = TRUE)[[1]])
  config$AssignMethods <- data.frame(
    name = methods,
    value = paste0("dsHPC::", methods),
    package = "dsHPC",
    version = as.character(utils::packageVersion("dsHPC")),
    type = "assign",
    class = "function",
    stringsAsFactors = FALSE)
  config
}

test_that("execution-unit Resources resolve only administrator catalogue entries", {
  catalog <- .unit_test_catalog(list(
    unit_alpha = .unit_test_entry(labels = c("dsHPC", "dsImaging"))))
  withr::local_options(list(dshpc.units_file = catalog))

  selection <- dsHPC:::.dshpc_unit_from_resource(.unit_test_resource())
  expect_identical(selection$source, "resource")
  expect_identical(selection$unit_id, "unit_alpha")
  expect_identical(selection$resource_pool_id, "unit_alpha")
  expect_identical(selection$type, "external")
  expect_identical(selection$config$external_submit_cmd,
    .unit_test_command())
  expect_false("executor_backend" %in% names(selection$config))
  expect_match(selection$config_seal, "^[0-9a-f]{64}$")

  invalid_urls <- c(
    "dshpc+unit://local/unit_alpha",
    "dshpc+unit://external/unit_alpha?secret=x",
    "dshpc+unit://external/../unit_alpha",
    "https://example.org/unit_alpha")
  for (url in invalid_urls) {
    resource <- resourcer::newResource("opaque", url)
    expect_error(dsHPC:::.dshpc_unit_from_resource(resource),
      "HPC unit resource is unavailable", fixed = TRUE)
  }
  armadillo <- .unit_test_resource(
    secret = "temporary-armadillo-jwt",
    url = paste0("https://armadillo.test/storage/projects/hpcunits/",
      "rawfiles/markers%2Funit_alpha_marker.parquet"),
    format = "dshpc-unit:external/unit_alpha")
  expect_identical(
    dsHPC:::.dshpc_unit_from_resource(armadillo), selection)
  client <- dsHPC:::DsHpcUnitResourceClient$new(armadillo)
  expect_s3_class(resourcer::newResourceClient(armadillo),
    "DsHpcUnitResourceClient")
  retained <- client$getResource()
  expect_null(retained$secret)
  expect_null(retained$identity)
  expect_false(grepl("armadillo.test", retained$url, fixed = TRUE))
  expect_false(grepl("temporary-armadillo-jwt",
    jsonlite::toJSON(client$getUnitSelection(), auto_unbox = TRUE),
    fixed = TRUE))
  expect_error(dsHPC:::.dshpc_unit_from_resource(.unit_test_resource(
    url = "dshpc+unit://external/unit_alpha",
    format = "dshpc-unit:external/unit_beta")),
    "HPC unit resource is unavailable", fixed = TRUE)
  expect_error(dsHPC:::.dshpc_unit_from_resource(
    structure(c(unclass(.unit_test_resource()), list(extra = "x")),
      class = "resource")),
    "HPC unit resource is unavailable", fixed = TRUE)
  expect_error(dsHPC:::.dshpc_unit_from_resource(
    .unit_test_resource("unknown_unit")),
    "HPC unit resource is unavailable", fixed = TRUE)
})

test_that("unit snapshots are canonical, authorized, and durable", {
  catalog <- .unit_test_catalog(list(
    unit_alpha = .unit_test_entry(labels = "dsImaging",
      runners = "dsimaging_image_preprocess")))
  withr::local_options(list(dshpc.units_file = catalog))
  selection <- dsHPC:::.dshpc_unit_from_resource(.unit_test_resource())

  restored <- jsonlite::fromJSON(jsonlite::toJSON(selection,
    auto_unbox = TRUE, null = "null"), simplifyVector = FALSE)
  expect_identical(dsHPC:::.dshpc_validate_unit_snapshot(restored),
    selection)

  authorized <- list(label = "dsImaging_radiomics", steps = list(list(
    plane = "artifact", runner = "dsimaging_image_preprocess")))
  expect_silent(dsHPC:::.dshpc_validate_unit_snapshot(selection,
    spec = authorized))
  expect_error(dsHPC:::.dshpc_validate_unit_snapshot(selection,
    spec = list(label = "dsFlower", steps = list())),
    "HPC unit resource is unavailable", fixed = TRUE)
  expect_error(dsHPC:::.dshpc_validate_unit_snapshot(selection,
    spec = list(label = "dsImaging", steps = list(list(
      plane = "artifact", runner = "unapproved_runner")))),
    "HPC unit resource is unavailable", fixed = TRUE)

  changed <- selection
  changed$config$default_timeout_secs <- 1L
  expect_error(dsHPC:::.dshpc_validate_unit_snapshot(changed),
    "HPC unit resource is unavailable", fixed = TRUE)

  withr::local_options(list(dshpc.units_file = tempfile("removed-catalog-")))
  settings <- dsHPC:::.dshpc_settings_for_spec(c(authorized,
    list(.dshpc_unit = restored)), base_settings = list(
      executor_backend = "embedded", default_timeout_secs = 99L))
  expect_identical(settings$executor_backend, "external")
  expect_identical(settings$default_timeout_secs, 86400L)
})

test_that("unit references remain private to one DataSHIELD session", {
  catalog <- .unit_test_catalog(list(
    unit_alpha = .unit_test_entry(labels = "dsHPC")))
  withr::local_options(list(dshpc.units_file = catalog))

  owner <- new.env(parent = globalenv())
  assign("unit_resource",
    DsHpcUnitResourceClient$new(.unit_test_resource()), envir = owner)
  reference <- eval(quote(dsHPC::hpcUnitInitDS("unit_resource")), owner)
  assign("hpc_unit", reference, envir = owner)

  expect_true(dsHPC:::.dshpc_is_unit_reference(reference))
  expect_identical(names(reference), "capability")
  expect_identical(
    trusted_hpc_call(hpcUnitSelectionInternal, owner)$unit_id,
    "unit_alpha")
  expect_error(
    trusted_hpc_call(hpcUnitSelectionInternal, owner,
      default_label = "dsImaging"),
    "HPC unit resource is unavailable", fixed = TRUE)
  expect_error(eval(quote(dsHPC::hpcUnitInitDS("unit_resource")), owner),
    "already selected", fixed = TRUE)

  other <- new.env(parent = globalenv())
  assign("hpc_unit", reference, envir = other)
  expect_null(trusted_hpc_call(hpcUnitSelectionInternal, other))
  expect_error(eval(quote(dsHPC::hpcUnitDestroyDS("hpc_unit")), other),
    "HPC unit resource is unavailable", fixed = TRUE)

  returned <- eval(quote(dsHPC::hpcUnitDestroyDS("hpc_unit")), owner)
  expect_identical(returned, reference)
  expect_null(trusted_hpc_call(hpcUnitSelectionInternal, owner))
  expect_false(exists("hpc_unit", envir = owner, inherits = FALSE))

  # Simulate a lost assign response: DataSHIELD assigns the invisible return
  # value back to the same symbol and the retry must remain idempotent.
  assign("hpc_unit", returned, envir = owner)
  expect_silent(eval(quote(dsHPC::hpcUnitDestroyDS("hpc_unit")), owner))

  replacement <- eval(quote(dsHPC::hpcUnitInitDS("unit_resource")), owner)
  assign("hpc_unit", replacement, envir = owner)
  rm(list = "hpc_unit", envir = owner)
  expect_null(trusted_hpc_call(hpcUnitSelectionInternal, owner))
  replacement <- eval(quote(dsHPC::hpcUnitInitDS("unit_resource")), owner)
  assign("hpc_unit", replacement, envir = owner)
  expect_silent(eval(quote(dsHPC::hpcUnitDestroyDS("hpc_unit")), owner))
})

test_that("unit initialization requires a platform-resolved Resource client", {
  catalog <- .unit_test_catalog(list(
    unit_alpha = .unit_test_entry(labels = "dsHPC")))
  withr::local_options(list(dshpc.units_file = catalog))
  owner <- new.env(parent = globalenv())
  assign("raw_resource", .unit_test_resource(), envir = owner)

  expect_error(eval(quote(dsHPC::hpcUnitInitDS("raw_resource")), owner),
    "HPC unit resource is unavailable", fixed = TRUE)
})

test_that("submitted jobs persist the unit without Resource names or credentials", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  catalog <- .unit_test_catalog(list(
    unit_alpha = .unit_test_entry(labels = "dsHPC"),
    unit_beta = .unit_test_entry(labels = "dsHPC")))
  withr::local_options(list(dshpc.home = home, dshpc.units_file = catalog))

  alpha <- dsHPC:::.dshpc_unit_from_resource(
    .unit_test_resource("unit_alpha"))
  beta <- dsHPC:::.dshpc_unit_from_resource(
    .unit_test_resource("unit_beta"))
  spec <- list(.owner = "owner", label = "dsHPC_test",
    visibility = "global", steps = list(list(
      type = "emit", plane = "session", output_name = "values",
      value = 1:5)))

  first <- trusted_hpc_call(hpcSubmitInternal, spec,
    unit_selection = alpha)
  second <- trusted_hpc_call(hpcSubmitInternal, spec,
    unit_selection = beta)
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  stored <- dsHPC:::.store_get_spec(db, first$job_id)
  jobs <- DBI::dbGetQuery(db,
    "SELECT job_id, spec_hash FROM jobs WHERE job_id IN (?, ?)",
    params = list(first$job_id, second$job_id))

  expect_identical(stored$.dshpc_unit$unit_id, "unit_alpha")
  expect_identical(stored$.dshpc_unit$source, "resource")
  persisted <- as.character(jsonlite::toJSON(stored, auto_unbox = TRUE,
    null = "null"))
  expect_false(grepl("opaque-resource-name", persisted, fixed = TRUE))
  expect_false(grepl(
    "opaque-resource-name|temporary-armadillo-jwt|password|private_key",
    persisted, ignore.case = TRUE))
  expect_length(unique(jobs$spec_hash), 2L)
  expect_false(isTRUE(second$deduplicated))

  step <- list(type = "run", plane = "artifact", runner = "runner")
  expect_false(identical(
    dsHPC:::.step_cache_hash(step, execution_unit = alpha),
    dsHPC:::.step_cache_hash(step, execution_unit = beta)))
  expect_error(trusted_hpc_call(hpcSubmitInternal,
    c(spec, list(.dshpc_unit = alpha))), "reserved field", fixed = TRUE)
})

test_that("new jobs pin the effective site default", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(
    dshpc.home = home,
    dshpc.executor_backend = "embedded",
    dshpc.default_timeout_secs = 17L,
    dshpc.max_retries = 1L))
  spec <- list(.owner = "owner", label = "dsHPC_test",
    steps = list(list(type = "emit", plane = "session",
      output_name = "values", value = 1:5)))

  handle <- trusted_hpc_call(hpcSubmitInternal, spec)
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  stored <- dsHPC:::.store_get_spec(db, handle$job_id)
  expect_identical(stored$.dshpc_unit$unit_id, "site-default")
  expect_identical(stored$.dshpc_unit$source, "site_default")
  expect_identical(stored$.dshpc_unit$type, "embedded")
  expect_identical(stored$.dshpc_unit$config$default_timeout_secs, 17L)
  expect_identical(stored$.dshpc_unit$config$max_retries, 1L)

  changed <- dsHPC:::.dshpc_settings()
  changed$default_timeout_secs <- 999L
  changed$max_retries <- 9L
  restored <- dsHPC:::.dshpc_settings_for_spec(stored,
    base_settings = changed)
  expect_identical(restored$default_timeout_secs, 17L)
  expect_identical(restored$max_retries, 1L)
  expect_identical(restored$executor_backend, "embedded")
})

test_that("resource units are rechecked before new execution steps", {
  catalog <- .unit_test_catalog(list(
    unit_alpha = .unit_test_entry(labels = "dsHPC")))
  withr::local_options(list(dshpc.units_file = catalog))
  selection <- dsHPC:::.dshpc_unit_from_resource(.unit_test_resource())
  spec <- list(label = "dsHPC_test", steps = list(),
    .dshpc_unit = selection)
  expect_silent(dsHPC:::.dshpc_assert_unit_dispatchable(spec))

  changed <- .unit_test_entry(labels = "dsHPC")
  changed$config$default_timeout_secs <- 10L
  yaml::write_yaml(list(schema_version = 1L,
    units = list(unit_alpha = changed)), catalog)

  # The old snapshot remains readable for status/cancellation, but cannot
  # start another step after the administrator changed or revoked it.
  expect_silent(dsHPC:::.dshpc_validate_unit_snapshot(selection, spec))
  expect_error(dsHPC:::.dshpc_assert_unit_dispatchable(spec),
    "HPC unit resource is unavailable", fixed = TRUE)
})

test_that("revoked units fail queued and continuing jobs without stale leases", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  catalog <- .unit_test_catalog(list(
    unit_alpha = .unit_test_entry(labels = "dsHPC")))
  withr::local_options(list(
    dshpc.home = home, dshpc.units_file = catalog,
    dshpc.worker_autostart = FALSE))
  selection <- dsHPC:::.dshpc_unit_from_resource(.unit_test_resource())
  spec <- make_test_spec(2L)
  spec$.dshpc_unit <- selection
  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)

  dsHPC:::.store_create_job(db, "job_revoked_pending", "owner", spec, 2L)
  dsHPC:::.store_create_job(db, "job_revoked_running", "owner", spec, 2L)
  dsHPC:::.store_update_job(db, "job_revoked_running",
    state = "RUNNING", step_index = 1L)
  dsHPC:::.scheduler_acquire_leases(db, "job_revoked_running", list(
    plan = list(memory_mb = 1L, cpu_slots = 1L), gpu_devices = character(0)))

  changed <- .unit_test_entry(labels = "dsHPC")
  changed$config$default_timeout_secs <- 10L
  yaml::write_yaml(list(schema_version = 1L,
    units = list(unit_alpha = changed)), catalog)

  dsHPC:::.worker_dispatch(db)
  pending <- dsHPC:::.store_get_job(db, "job_revoked_pending")
  expect_identical(pending$state, "FAILED")
  expect_match(pending$error_message, "HPC unit resource is unavailable",
    fixed = TRUE)

  expect_false(dsHPC:::.executor_advance(
    db, "job_revoked_running"))
  running <- dsHPC:::.store_get_job(db, "job_revoked_running")
  expect_identical(running$state, "FAILED")
  expect_identical(running$error_message,
    "Execution unit became unavailable")
  leases <- DBI::dbGetQuery(db,
    "SELECT job_id FROM resource_leases WHERE job_id IN (?, ?)",
    params = list("job_revoked_pending", "job_revoked_running"))
  expect_equal(nrow(leases), 0L)
})

test_that("completed unit jobs remain readable after catalogue removal", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  catalog <- .unit_test_catalog(list(
    unit_alpha = .unit_test_entry(labels = "dsHPC")))
  withr::local_options(list(dshpc.home = home, dshpc.units_file = catalog))
  selection <- dsHPC:::.dshpc_unit_from_resource(.unit_test_resource())
  handle <- trusted_hpc_call(hpcSubmitInternal, make_test_spec(),
    unit_selection = selection)
  unlink(catalog)

  status <- trusted_hpc_call(hpcStatusInternal, handle,
    required_label = "dsHPC_test")
  expect_identical(status$state, "FINISHED")
  expect_true(status$is_done)
})

test_that("snapshot integrity does not depend on command availability", {
  command <- tempfile("dshpc-wrapper-")
  expect_true(file.copy(.unit_test_command(), command))
  Sys.chmod(command, "0755")
  catalog <- .unit_test_catalog(list(
    unit_alpha = .unit_test_entry(labels = "dsHPC", command = command)))
  withr::local_options(list(dshpc.units_file = catalog))
  selection <- dsHPC:::.dshpc_unit_from_resource(.unit_test_resource())
  spec <- list(label = "dsHPC_test", steps = list(),
    .dshpc_unit = selection)

  unlink(command)
  expect_silent(dsHPC:::.dshpc_validate_unit_snapshot(selection, spec))
  expect_silent(dsHPC:::.dshpc_settings_for_spec(spec))
  expect_error(dsHPC:::.dshpc_assert_unit_dispatchable(spec),
    "HPC unit resource is unavailable", fixed = TRUE)
})

test_that("unit aliases share only an explicitly configured physical pool", {
  catalog <- .unit_test_catalog(list(
    unit_alpha = .unit_test_entry(resource_pool_id = "cluster-a"),
    unit_beta = .unit_test_entry(resource_pool_id = "cluster-a")))
  withr::local_options(list(dshpc.units_file = catalog))
  alpha <- dsHPC:::.dshpc_unit_from_resource(
    .unit_test_resource("unit_alpha"))
  beta <- dsHPC:::.dshpc_unit_from_resource(
    .unit_test_resource("unit_beta"))
  alpha_spec <- list(label = "dsHPC_test", steps = list(),
    .dshpc_unit = alpha)
  beta_spec <- list(label = "dsHPC_test", steps = list(),
    .dshpc_unit = beta)

  expect_identical(dsHPC:::.dshpc_unit_key(alpha_spec),
    dsHPC:::.dshpc_unit_key(beta_spec))
  expect_false(identical(alpha$config_seal, beta$config_seal))
})

test_that("legacy and pinned site-default jobs share one scheduler pool", {
  withr::local_options(list(
    dshpc.executor_backend = "embedded",
    dshpc.site_default_pool_id = "shared-local"))
  legacy <- list(label = "dsHPC_test", steps = list())
  pinned <- legacy
  pinned$.dshpc_unit <- dsHPC:::.dshpc_site_default_snapshot("dsHPC_test")

  expect_identical(dsHPC:::.dshpc_unit_key(legacy),
    "embedded:shared-local")
  expect_identical(dsHPC:::.dshpc_unit_key(legacy),
    dsHPC:::.dshpc_unit_key(pinned))
  expect_identical(pinned$.dshpc_unit$resource_pool_id, "shared-local")
})

test_that("durable unit snapshots reject arbitrary persisted extra arguments", {
  entry <- .unit_test_entry()
  entry$config$container_extra_args <- list("--env", "TOKEN=value")
  catalog <- .unit_test_catalog(list(unit_alpha = entry))
  withr::local_options(list(dshpc.units_file = catalog))
  expect_error(dsHPC:::.dshpc_unit_from_resource(.unit_test_resource()),
    "HPC unit resource is unavailable", fixed = TRUE)

  withr::local_options(list(
    dshpc.executor_backend = "embedded",
    dshpc.container_extra_args = c("--env", "TOKEN=value")))
  expect_error(dsHPC:::.dshpc_site_default_snapshot("dsHPC"),
    "HPC unit resource is unavailable", fixed = TRUE)
})

test_that("DSLite resolves an opaque unit Resource without Opal naming", {
  skip_if_not_installed("DSLite")
  catalog <- .unit_test_catalog(list(
    unit_alpha = .unit_test_entry(labels = "dsHPC")))
  withr::local_options(list(dshpc.units_file = catalog))

  config <- .unit_test_dslite_config()
  server <- DSLite::newDSLiteServer(
    resources = list(unit_without_project_prefix = .unit_test_resource()),
    config = config, strict = TRUE)
  env <- environment()
  assign("unit_resource_server", server, envir = env)
  withr::local_options(list(datashield.env = env))
  connection <- DSI::dsConnect(DSLite::DSLite(), name = "site",
    url = "unit_resource_server")
  on.exit(DSI::dsDisconnect(connection), add = TRUE)

  DSI::dsFetch(DSI::dsAssignResource(connection, "unit_resource",
    "unit_without_project_prefix", async = FALSE))
  DSI::dsFetch(DSI::dsAssignExpr(connection, "hpc_unit",
    call("hpcUnitInitDS", "unit_resource"), async = FALSE))
  session <- server$getSession(connection@sid)
  expect_s3_class(session$unit_resource, "DsHpcUnitResourceClient")
  expect_true(dsHPC:::.dshpc_is_unit_reference(session$hpc_unit))
  expect_identical(
    trusted_hpc_call(hpcUnitSelectionInternal, session)$unit_id,
    "unit_alpha")

  DSI::dsFetch(DSI::dsAssignExpr(connection, "hpc_unit",
    call("hpcUnitDestroyDS", "hpc_unit"), async = FALSE))
  DSI::dsRmSymbol(connection, "hpc_unit")
  DSI::dsRmSymbol(connection, "unit_resource")
  expect_null(trusted_hpc_call(hpcUnitSelectionInternal, session))
})
