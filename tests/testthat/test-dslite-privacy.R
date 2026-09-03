.description_method_table <- function(field) {
  description <- system.file("DESCRIPTION", package = "dsHPC")
  raw <- read.dcf(description, fields = field)[1, 1]
  if (is.na(raw) || !nzchar(trimws(raw))) {
    return(data.frame(name = character(0), value = character(0),
      package = character(0), version = character(0), type = character(0),
      class = character(0), stringsAsFactors = FALSE))
  }
  entries <- trimws(strsplit(raw, ",", fixed = TRUE)[[1]])
  aliases <- grepl("=", entries, fixed = TRUE)
  names <- ifelse(aliases, sub("=.*$", "", entries), entries)
  values <- ifelse(aliases, sub("^[^=]*=", "", entries),
    paste0("dsHPC::", entries))
  data.frame(
    name = names,
    value = values,
    package = "dsHPC",
    version = as.character(utils::packageVersion("dsHPC")),
    type = if (identical(field, "AggregateMethods")) "aggregate" else "assign",
    class = "function",
    stringsAsFactors = FALSE
  )
}

.dshpc_dslite_config <- function() {
  config <- DSLite::defaultDSConfiguration()
  config$AggregateMethods <- .description_method_table("AggregateMethods")
  config$AssignMethods <- .description_method_table("AssignMethods")
  config
}

.dshpc_encode_test_arg <- function(x) {
  json <- as.character(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"))
  encoded <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(json)))
  encoded <- gsub("\\+", "-", encoded)
  encoded <- gsub("/", "_", encoded)
  paste0("B64:", sub("=+$", "", encoded))
}

.dslite_fetch <- function(connection, expression) {
  DSI::dsFetch(DSI::dsAggregate(connection, expression, async = FALSE))
}

test_that("DESCRIPTION exposes only capability-scoped analyst methods", {
  skip_if_not_installed("DSLite")

  config <- .dshpc_dslite_config()
  expect_true("hpcJobReferenceDS" %in% config$AggregateMethods$name)
  expect_false(any(config$AggregateMethods$name %in% c(
    "c", "list", "hpcListDS", "hpcStudioDS", "hpcSchedulerStatusDS")))
  expect_false(any(config$AggregateMethods$value %in%
    c("base::c", "base::list")))
  expect_false(any(config$AssignMethods$name %in% c(
    "hpcSubmitDS", "hpcLoadOutputDS")))

  server <- DSLite::newDSLiteServer(config = config, strict = TRUE)
  env <- environment()
  assign("allowlist_server", server, envir = env)
  withr::local_options(list(datashield.env = env))
  connection <- DSI::dsConnect(DSLite::DSLite(), name = "allowlist",
    url = "allowlist_server")
  on.exit(DSI::dsDisconnect(connection), add = TRUE)

  expect_error(.dslite_fetch(connection, quote(c(1, 2))),
    "does not allow expression: c", fixed = TRUE)
  expect_error(.dslite_fetch(connection, quote(list(1, 2))),
    "does not allow expression: list", fixed = TRUE)
})

test_that("DSLite rejects direct generic job submission and output loading", {
  skip_if_not_installed("DSLite")

  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, nfilter.subset = 3))
  on.exit(cleanup_test_home(home), add = TRUE)

  server <- DSLite::newDSLiteServer(config = .dshpc_dslite_config(),
    strict = TRUE)
  env <- environment()
  assign("privacy_server", server, envir = env)
  withr::local_options(list(datashield.env = env))
  owner <- DSI::dsConnect(DSLite::DSLite(), name = "owner",
    url = "privacy_server")
  on.exit(try(DSI::dsDisconnect(owner), silent = TRUE), add = TRUE)

  spec <- list(
    .owner = "owner",
    label = "privacy-test",
    steps = list(list(type = "emit", plane = "session",
      output_name = "values", value = as.list(seq_len(5L))))
  )
  expect_error(DSI::dsAssignExpr(owner, "job_handle",
    call("hpcSubmitDS", .dshpc_encode_test_arg(spec)), async = FALSE),
    "does not allow expression: hpcSubmitDS", fixed = TRUE)
  expect_error(DSI::dsAssignExpr(owner, "loaded_values",
    call("hpcLoadOutputDS", "job_handle", "values", FALSE,
      "privacy-test"), async = FALSE),
    "does not allow expression: hpcLoadOutputDS", fixed = TRUE)
  db <- dsHPC:::.db_connect()
  jobs_before <- DBI::dbGetQuery(db, "SELECT COUNT(*) AS n FROM jobs")$n
  dsHPC:::.db_close(db)
  expect_error(.dslite_fetch(owner,
    call("hpcStatusDS", call("hpcSubmitInternal",
      .dshpc_encode_test_arg(spec)))),
    "literal value or an assigned server symbol", fixed = TRUE)
  db <- dsHPC:::.db_connect()
  jobs_after <- DBI::dbGetQuery(db, "SELECT COUNT(*) AS n FROM jobs")$n
  dsHPC:::.db_close(db)
  expect_equal(jobs_after,
    jobs_before)
})

test_that("public methods reject evaluated argument expressions", {
  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home))
  on.exit(cleanup_test_home(home), add = TRUE)
  handle <- trusted_hpc_call(hpcSubmitInternal, list(
    .owner = "owner", label = "dsHPC_test", visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "values", value = 1:5))))

  marker <- FALSE
  expect_error(hpcStatusDS({
    marker <- TRUE
    handle
  }), "literal value or an assigned server symbol")
  expect_false(marker)
  expect_error(hpcStatusDS(base::identity(handle)),
    "literal value or an assigned server symbol")
  expect_error(hpcLogsDS(handle, {
    marker <- TRUE
    50L
  }), "literal value or an assigned server symbol")
  expect_false(marker)
})

test_that("retired DS methods fail under a persisted legacy allowlist", {
  skip_if_not_installed("DSLite")

  method_rows <- function(methods, type) {
    data.frame(
      name = methods,
      value = paste0("dsHPC::", methods),
      package = "dsHPC",
      version = as.character(utils::packageVersion("dsHPC")),
      type = type,
      class = "function",
      stringsAsFactors = FALSE
    )
  }

  config <- .dshpc_dslite_config()
  legacy_aggregates <- c("hpcListDS", "hpcStudioDS",
    "hpcSchedulerStatusDS")
  legacy_assigns <- c("hpcSubmitDS", "hpcLoadOutputDS")
  config$AggregateMethods <- rbind(config$AggregateMethods,
    method_rows(legacy_aggregates, "aggregate"))
  config$AssignMethods <- rbind(config$AssignMethods,
    method_rows(legacy_assigns, "assign"))

  server <- DSLite::newDSLiteServer(config = config, strict = TRUE)
  env <- environment()
  assign("legacy_server", server, envir = env)
  withr::local_options(list(datashield.env = env))
  connection <- DSI::dsConnect(DSLite::DSLite(), name = "legacy",
    url = "legacy_server")
  on.exit(DSI::dsDisconnect(connection), add = TRUE)

  for (method in legacy_aggregates) {
    expect_error(.dslite_fetch(connection,
      as.call(list(as.name(method)))), "was retired", fixed = TRUE)
  }
  expect_error(DSI::dsAssignExpr(connection, "retired_submit",
    call("hpcSubmitDS", "ignored"), async = FALSE),
    "was retired", fixed = TRUE)
  expect_error(DSI::dsAssignExpr(connection, "retired_load",
    call("hpcLoadOutputDS", "ignored", "ignored"), async = FALSE),
    "was retired", fixed = TRUE)
})

test_that("result reconstruction failures do not disclose server paths", {
  skip_if_not_installed("DSLite")

  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, nfilter.subset = 3))
  on.exit(cleanup_test_home(home), add = TRUE)

  handle <- trusted_hpc_call(hpcSubmitInternal, list(
    .owner = "owner",
    label = "privacy-test",
    visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "values", value = as.list(seq_len(5L))))
  ))
  bearer <- hpcJobReferenceDS(handle)
  result_dir <- file.path(home, "artifacts", handle$job_id, "result")
  unlink(result_dir, recursive = TRUE)
  writeLines("not a directory", result_dir)

  server <- DSLite::newDSLiteServer(config = .dshpc_dslite_config(),
    strict = TRUE)
  env <- environment()
  assign("result_error_server", server, envir = env)
  withr::local_options(list(datashield.env = env))
  connection <- DSI::dsConnect(DSLite::DSLite(), name = "result_error",
    url = "result_error_server")
  on.exit(try(DSI::dsDisconnect(connection), silent = TRUE), add = TRUE)

  warnings <- character(0)
  error <- tryCatch(
    withCallingHandlers(
      .dslite_fetch(connection, call("hpcResultDS", bearer)),
      warning = function(w) {
        warnings <<- c(warnings, conditionMessage(w))
        invokeRestart("muffleWarning")
      }
    ),
    error = function(e) conditionMessage(e)
  )

  expect_length(warnings, 0L)
  expect_match(error, "Job result is unavailable.", fixed = TRUE)
  expect_false(grepl(home, error, fixed = TRUE))
  expect_false(grepl(handle$job_id, error, fixed = TRUE))
  expect_false(grepl(bearer, error, fixed = TRUE))
})

test_that("private handle symbols never resolve across DSLite sessions", {
  skip_if_not_installed("DSLite")

  home <- setup_test_home()
  withr::local_options(list(dshpc.home = home, nfilter.subset = 3))
  on.exit(cleanup_test_home(home), add = TRUE)
  handle <- trusted_hpc_call(hpcSubmitInternal, list(
    .owner = "owner",
    label = "privacy-test",
    visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "values", value = as.list(seq_len(5L))))
  ))

  server <- DSLite::newDSLiteServer(config = .dshpc_dslite_config(),
    strict = TRUE)
  env <- environment()
  assign("isolation_server", server, envir = env)
  withr::local_options(list(datashield.env = env))
  owner <- DSI::dsConnect(DSLite::DSLite(), name = "owner",
    url = "isolation_server")
  other <- DSI::dsConnect(DSLite::DSLite(), name = "other",
    url = "isolation_server")
  on.exit(try(DSI::dsDisconnect(owner), silent = TRUE), add = TRUE)
  on.exit(try(DSI::dsDisconnect(other), silent = TRUE), add = TRUE)

  assign("private_handle", handle, envir = server$getSession(owner@sid))
  leaked_name <- paste0("dshpc_global_handle_", Sys.getpid())
  assign(leaked_name, handle, envir = .GlobalEnv)
  on.exit(rm(list = leaked_name, envir = .GlobalEnv), add = TRUE)

  expect_equal(.dslite_fetch(owner,
    call("hpcStatusDS", "private_handle"))$state, "FINISHED")
  expect_error(.dslite_fetch(other,
    call("hpcStatusDS", "private_handle")),
    "Job not found or access denied", fixed = TRUE)
  expect_error(.dslite_fetch(other,
    call("hpcStatusDS", leaked_name)),
    "Job not found or access denied", fixed = TRUE)

  bearer <- hpcJobReferenceDS(handle)
  expect_equal(.dslite_fetch(other, call("hpcStatusDS", bearer))$state,
    "FINISHED")
})
