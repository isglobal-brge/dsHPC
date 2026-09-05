shared_test_spec <- function(value = 1:5, visibility = "global",
                             type = "emit",
                             reuse_fingerprint = strrep("a", 64L)) {
  spec <- list(.owner = "untrusted-display-owner", label = "dsHPC_test",
    visibility = visibility, steps = list(list(type = type, plane = "session",
      output_name = "values", value = value)))
  if (!is.null(reuse_fingerprint)) {
    spec$reuse_fingerprint <- reuse_fingerprint
  }
  spec
}

test_that("tracking mutation and resolution APIs require a package caller", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))
  expect_error(hpcTrackingCreateInternal(), "trusted server packages",
    fixed = TRUE)
  expect_error(hpcTrackingFinishInternal(
    "trk_00000000-0000-4000-8000-000000000000"),
    "trusted server packages", fixed = TRUE)
  expect_error(hpcTrackingResolveOutputInternal(
    "trk_00000000-0000-4000-8000-000000000000", "value"),
    "trusted server packages", fixed = TRUE)
})

test_that("tracking roots use only neutral closed-vocabulary kinds", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  imaging <- trusted_hpc_call(hpcTrackingCreateInternal,
    "same-domain-key", kind = "imaging")
  analysis <- trusted_hpc_call(hpcTrackingCreateInternal,
    "same-domain-key", kind = "analysis")

  expect_identical(imaging$kind, "imaging")
  expect_identical(analysis$kind, "analysis")
  expect_false(identical(imaging$tracking_id, analysis$tracking_id))
  expect_setequal(hpcTrackingListDS()$items$kind,
    c("analysis", "imaging"))
  expect_error(trusted_hpc_call(hpcTrackingCreateInternal,
    "same-domain-key", kind = "private-cohort-name"),
    "Tracking kind is invalid", fixed = TRUE)
})

test_that("shared public methods reject evaluated argument expressions", {
  marker <- FALSE
  for (fn in list(hpcTrackingStatusDS, hpcTrackingResultDS,
                  hpcTrackingOutputsDS)) {
    expect_error(fn({
      marker <- TRUE
      "trk_00000000-0000-4000-8000-000000000000"
    }), "literal value or an assigned server symbol")
    expect_false(marker)
  }
  expect_error(hpcTrackingAssignOutputDS(
    "trk_00000000-0000-4000-8000-000000000000", {
      marker <- TRUE
      "value"
    }), "literal value or an assigned server symbol")
  expect_false(marker)
})

test_that("global submissions create one root and reuse active or completed work", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  first <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())
  capabilities <- hpcCapabilitiesDS()
  expect_identical(capabilities$shared_tracking, "root_v1")
  expect_identical(capabilities$shared_results, "safe_v1")
  expect_identical(capabilities$reusable_outputs, "opaque_ref_v1")
  expect_identical(capabilities$queue_visibility, "shared")
  second <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())
  expect_match(first$tracking_id, "^trk_")
  expect_identical(second$tracking_id, first$tracking_id)
  expect_identical(second$job_id, first$job_id)
  expect_true(second$reused)
  expect_null(second$.dshpc_capability)
  expect_equal(hpcStatusDS(first)$state, "FINISHED")

  db <- dsHPC:::.db_connect()
  expect_equal(DBI::dbGetQuery(db, "SELECT COUNT(*) AS n FROM jobs")$n, 1)
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM tracking_roots")$n, 1)
  dsHPC:::.store_update_job(db, first$job_id, state = "RUNNING")
  dsHPC:::.db_close(db)

  active <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())
  expect_identical(active$job_id, first$job_id)
  expect_identical(active$tracking_id, first$tracking_id)
  expect_null(active$.dshpc_capability)
})

test_that("jobs are reused only with a trusted immutable identity", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))
  spec <- shared_test_spec(reuse_fingerprint = NULL)

  first <- trusted_hpc_call(hpcSubmitInternal, spec)
  second <- trusted_hpc_call(hpcSubmitInternal, spec)
  expect_false(identical(second$job_id, first$job_id))
  expect_false(identical(second$tracking_id, first$tracking_id))

  db <- dsHPC:::.db_connect()
  dsHPC:::.store_update_job(db, second$job_id, state = "RUNNING",
    finished_at = NA_character_)
  dsHPC:::.db_close(db)
  active <- trusted_hpc_call(hpcSubmitInternal, spec)
  expect_false(identical(active$job_id, second$job_id))
  expect_false(identical(active$tracking_id, second$tracking_id))
})

test_that("a fingerprint cannot enable reuse without a sealed runtime", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared", dshpc.runtime_revision = NULL,
    default.dshpc.runtime_revision = NULL))
  withr::local_envvar(c(DSHPC_RUNTIME_REVISION = NA_character_))

  first <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())
  second <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())

  expect_false(identical(second$job_id, first$job_id))
  expect_false(identical(second$tracking_id, first$tracking_id))
  expect_false(isTRUE(second$reused))
  expect_error(trusted_hpc_call(hpcRuntimeIdentityInternal),
    "runtime revision is not configured", fixed = TRUE)
})

test_that("an interrupted implicit root is recovered without becoming visible", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  spec <- dsHPC:::.validate_job_spec(shared_test_spec())
  spec$job_id <- NULL
  spec$.dshpc_unit <- dsHPC:::.dshpc_site_default_snapshot(spec$label)
  spec_hash <- dsHPC:::.dshpc_whole_job_hash(spec, "dsHPC")

  db <- dsHPC:::.db_connect()
  orphan <- dsHPC:::.tracking_create(db, "dsHPC", reuse_key = spec_hash,
    implicit = TRUE)
  expect_identical(DBI::dbGetQuery(db,
    "SELECT lifecycle FROM tracking_roots WHERE tracking_id = ?",
    params = list(orphan$tracking_id))$lifecycle, "CREATING")
  dsHPC:::.db_close(db)
  expect_equal(nrow(hpcTrackingListDS()$items), 0L)

  recovered <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())
  expect_identical(recovered$tracking_id, orphan$tracking_id)
  db <- dsHPC:::.db_connect()
  expect_identical(DBI::dbGetQuery(db,
    "SELECT lifecycle FROM tracking_roots WHERE tracking_id = ?",
    params = list(orphan$tracking_id))$lifecycle, "OPEN")
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM tracking_jobs WHERE tracking_id = ?",
    params = list(orphan$tracking_id))$n, 1)
  dsHPC:::.db_close(db)
  expect_equal(nrow(hpcTrackingListDS()$items), 1L)
})

test_that("a failed implicit execution is resubmitted under a new root", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  spec <- shared_test_spec(type = "not_a_session_runner")
  first <- trusted_hpc_call(hpcSubmitInternal, spec)
  expect_equal(first$state, "FAILED")
  second <- trusted_hpc_call(hpcSubmitInternal, spec)
  expect_equal(second$state, "FAILED")
  expect_false(identical(first$job_id, second$job_id))
  expect_false(identical(first$tracking_id, second$tracking_id))

  page <- hpcTrackingListDS()
  expect_equal(nrow(page$items), 2L)
  expect_true(all(page$items$state == "terminal"))
})

test_that("a failed implicit job never publishes a partial reusable output", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  spec <- shared_test_spec()
  spec$steps <- list(
    list(type = "emit", plane = "session", output_name = "partial",
      value = 1:5),
    list(type = "not_a_session_runner", plane = "session"))
  failed <- trusted_hpc_call(hpcSubmitInternal, spec)
  tracking_id <- failed$tracking_id
  expect_identical(failed$state, "FAILED")
  expect_identical(hpcTrackingStatusDS(tracking_id)$state, "terminal")
  expect_equal(nrow(hpcTrackingOutputsDS(tracking_id)), 0L)
  expect_false(hpcTrackingResultDS(tracking_id)$ready)

  retry <- trusted_hpc_call(hpcSubmitInternal, spec)
  expect_false(identical(retry$tracking_id, tracking_id))
})

test_that("an explicit primary is idempotent and can retry after failure", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  root <- trusted_hpc_call(hpcTrackingCreateInternal, "direct-analysis")
  failed_spec <- shared_test_spec(type = "not_a_session_runner")
  first <- trusted_hpc_call(hpcSubmitInternal, failed_spec,
    tracking_id = root$tracking_id, tracking_role = "primary")
  expect_equal(first$state, "FAILED")
  tracking_id <- root$tracking_id
  expect_identical(hpcTrackingStatusDS(tracking_id)$state, "running")

  retry <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = root$tracking_id, tracking_role = "primary")
  expect_false(identical(retry$job_id, first$job_id))
  attached <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = root$tracking_id, tracking_role = "primary")
  expect_identical(attached$job_id, retry$job_id)
  expect_null(attached$.dshpc_capability)
  expect_error(trusted_hpc_call(hpcSubmitInternal,
    shared_test_spec(value = 99), tracking_id = root$tracking_id,
    tracking_role = "primary"), "specification does not match", fixed = TRUE)

  db <- dsHPC:::.db_connect()
  roles <- DBI::dbGetQuery(db,
    "SELECT role FROM tracking_jobs WHERE tracking_id = ?",
    params = list(root$tracking_id))
  dsHPC:::.db_close(db)
  expect_equal(sum(roles$role == "primary"), 2L)
})

test_that("explicit primary submissions require an immutable identity", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))
  root <- trusted_hpc_call(hpcTrackingCreateInternal, "explicit-no-fingerprint")
  spec <- shared_test_spec(reuse_fingerprint = NULL)

  expect_error(trusted_hpc_call(hpcSubmitInternal, spec,
    tracking_id = root$tracking_id, tracking_role = "primary"),
    "immutable fingerprint", fixed = TRUE)

  db <- dsHPC:::.db_connect()
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM tracking_jobs WHERE tracking_id = ?",
    params = list(root$tracking_id))$n, 0)
  dsHPC:::.db_close(db)
})

test_that("explicit session effects are reconstructed instead of reused", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))
  root <- trusted_hpc_call(hpcTrackingCreateInternal, "explicit-session-effect")
  spec <- shared_test_spec()
  spec$steps[[1L]] <- list(type = "assign_expr", plane = "session",
    expr = "1 + 1", symbol = "assigned_value")

  first <- trusted_hpc_call(hpcSubmitInternal, spec,
    tracking_id = root$tracking_id, tracking_role = "primary")
  second <- trusted_hpc_call(hpcSubmitInternal, spec,
    tracking_id = root$tracking_id, tracking_role = "primary")
  expect_false(identical(first$job_id, second$job_id))
  expect_false(isTRUE(second$reused))
  expect_match(second$.dshpc_capability, "^cap_")
})

test_that("implicit reuse cannot attach to an explicit workflow primary", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  root <- trusted_hpc_call(hpcTrackingCreateInternal, "explicit-boundary")
  explicit <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = root$tracking_id, tracking_role = "primary")
  implicit <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())

  expect_false(identical(implicit$job_id, explicit$job_id))
  expect_false(identical(implicit$tracking_id, root$tracking_id))
})

test_that("an explicit root cannot mix primary and collection children", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  primary_root <- trusted_hpc_call(hpcTrackingCreateInternal, "mode-primary")
  trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = primary_root$tracking_id, tracking_role = "primary")
  expect_error(trusted_hpc_call(hpcSubmitInternal,
    shared_test_spec(value = 6:10, visibility = "private"),
    tracking_id = primary_root$tracking_id, tracking_role = "child"),
    "execution mode does not match", fixed = TRUE)

  child_root <- trusted_hpc_call(hpcTrackingCreateInternal, "mode-child")
  trusted_hpc_call(hpcSubmitInternal,
    shared_test_spec(visibility = "private"),
    tracking_id = child_root$tracking_id, tracking_role = "child")
  expect_error(trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = child_root$tracking_id, tracking_role = "primary"),
    "execution mode does not match", fixed = TRUE)

  db <- dsHPC:::.db_connect()
  DBI::dbExecute(db, "DELETE FROM tracking_jobs WHERE tracking_id = ?",
    params = list(child_root$tracking_id))
  mode <- DBI::dbGetQuery(db,
    "SELECT execution_mode FROM tracking_roots WHERE tracking_id = ?",
    params = list(child_root$tracking_id))$execution_mode
  dsHPC:::.db_close(db)
  expect_identical(mode, "child")
  expect_error(trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = child_root$tracking_id, tracking_role = "primary"),
    "execution mode does not match", fixed = TRUE)
})

test_that("cache opt-out prevents whole-job reuse", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))
  spec <- shared_test_spec()
  spec$steps[[1]]$cache <- FALSE

  first <- trusted_hpc_call(hpcSubmitInternal, spec)
  second <- trusted_hpc_call(hpcSubmitInternal, spec)

  expect_false(identical(second$job_id, first$job_id))
  expect_false(identical(second$tracking_id, first$tracking_id))
})

test_that("session operations with caller-visible effects are not reused", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))
  spec <- shared_test_spec()
  spec$steps[[1L]] <- list(type = "assign_expr", plane = "session",
    expr = "1 + 1", symbol = "assigned_value")

  first <- trusted_hpc_call(hpcSubmitInternal, spec)
  second <- trusted_hpc_call(hpcSubmitInternal, spec)

  expect_false(identical(second$job_id, first$job_id))
  expect_false(identical(second$tracking_id, first$tracking_id))
})

test_that("an explicit workflow cannot claim success without knowledge", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))
  root <- trusted_hpc_call(hpcTrackingCreateInternal, "empty-success")
  expect_error(trusted_hpc_call(hpcTrackingFinishInternal,
    root$tracking_id, TRUE), "must publish a reusable output", fixed = TRUE)
  expect_silent(trusted_hpc_call(hpcTrackingFinishInternal,
    root$tracking_id, FALSE))
  retry <- trusted_hpc_call(hpcTrackingCreateInternal, "empty-success")
  expect_false(retry$reused)
  expect_false(identical(retry$tracking_id, root$tracking_id))
})

test_that("a published primary seals only after execution is terminal", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))
  root <- trusted_hpc_call(hpcTrackingCreateInternal, "primary-publication")
  job <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = root$tracking_id, tracking_role = "primary")

  db <- dsHPC:::.db_connect()
  dsHPC:::.store_update_job(db, job$job_id, state = "RUNNING",
    finished_at = NA_character_)
  dsHPC:::.db_close(db)
  expect_silent(trusted_hpc_call(hpcTrackingPublishReferenceInternal,
    root$tracking_id, "knowledge", "asset_primary"))
  expect_silent(trusted_hpc_call(hpcTrackingPublishReferenceInternal,
    root$tracking_id, "knowledge", "asset_primary"))
  expect_error(trusted_hpc_call(hpcTrackingPublishReferenceInternal,
    root$tracking_id, "knowledge", "asset_other"),
    "already published", fixed = TRUE)
  expect_identical(
    trusted_hpc_call(hpcTrackingStatusInternal, root$tracking_id)$state,
    "running")

  db <- dsHPC:::.db_connect()
  dsHPC:::.store_update_job(db, job$job_id, state = "FINISHED",
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
  dsHPC:::.db_close(db)
  expect_silent(trusted_hpc_call(hpcTrackingPublishOutputInternal,
    root$tracking_id, job, "values", public_name = "values_copy",
    classification = "server_reusable"))
  expect_identical(
    trusted_hpc_call(hpcTrackingStatusInternal, root$tracking_id)$state,
    "running")
  expect_silent(trusted_hpc_call(hpcTrackingPublishReferenceInternal,
    root$tracking_id, "new_knowledge", "asset_other"))
  expect_silent(trusted_hpc_call(hpcTrackingFinishInternal,
    root$tracking_id, TRUE))
  expect_identical(
    trusted_hpc_call(hpcTrackingStatusInternal, root$tracking_id)$state,
    "terminal")
  expect_silent(trusted_hpc_call(hpcTrackingPublishReferenceInternal,
    root$tracking_id, "knowledge", "asset_primary"))
  expect_silent(trusted_hpc_call(hpcTrackingPublishOutputInternal,
    root$tracking_id, job, "values", public_name = "values_copy",
    classification = "server_reusable"))
  expect_error(trusted_hpc_call(hpcTrackingPublishReferenceInternal,
    root$tracking_id, "late_knowledge", "asset_late"),
    "already complete", fixed = TRUE)
  tracking_id <- root$tracking_id
  public_outputs <- hpcTrackingOutputsDS(tracking_id)
  expect_identical(public_outputs$name, "output_001")
  expect_true(all(public_outputs$kind == "server_object"))
  expect_false(grepl("knowledge|values_copy",
    paste(capture.output(str(public_outputs)), collapse = "\n")))
  reference <- hpcTrackingAssignOutputDS(tracking_id, "output_001")
  resolved <- trusted_hpc_call(hpcTrackingResolveOutputInternal, reference)
  expect_identical(resolved$output_name, "knowledge")
  expect_identical(resolved$reference, "asset_primary")
})

test_that("a self-contained primary durably requests terminal reconciliation", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  root <- trusted_hpc_call(hpcTrackingCreateInternal, "auto-finalize")
  job <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = root$tracking_id, tracking_role = "primary",
    tracking_finalize = TRUE)
  expect_identical(job$state, "FINISHED")
  expect_silent(trusted_hpc_call(hpcTrackingPublishOutputInternal,
    root$tracking_id, job, "values", public_name = "primary_output",
    classification = "server_reusable"))
  tracking_id <- root$tracking_id
  expect_identical(hpcTrackingStatusDS(tracking_id)$state, "terminal")

  db <- dsHPC:::.db_connect()
  stored <- dsHPC:::.tracking_get_root(db, root$tracking_id)
  dsHPC:::.db_close(db)
  expect_identical(as.integer(stored$finish_requested), 1L)
  expect_identical(as.integer(stored$success), 1L)

  expect_error(trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_finalize = TRUE), "explicit primary", fixed = TRUE)
})

test_that("a successful finalizing retry supersedes an earlier failed primary", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  root <- trusted_hpc_call(hpcTrackingCreateInternal, "retry-finalize")
  failed <- trusted_hpc_call(hpcSubmitInternal,
    shared_test_spec(type = "not_a_session_runner"),
    tracking_id = root$tracking_id, tracking_role = "primary")
  expect_identical(failed$state, "FAILED")

  final <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = root$tracking_id, tracking_role = "primary",
    tracking_finalize = TRUE)
  expect_silent(trusted_hpc_call(hpcTrackingPublishOutputInternal,
    root$tracking_id, final, "values", public_name = "primary_output",
    classification = "server_reusable"))
  tracking_id <- root$tracking_id
  expect_identical(hpcTrackingStatusDS(tracking_id)$state, "terminal")

  db <- dsHPC:::.db_connect()
  stored <- dsHPC:::.tracking_get_root(db, tracking_id)
  dsHPC:::.db_close(db)
  expect_identical(as.character(stored$finalizing_job_id), final$job_id)
  expect_identical(as.integer(stored$success), 1L)
})

test_that("reattaching an existing primary can durably request finalization", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  active_root <- trusted_hpc_call(hpcTrackingCreateInternal,
    "active-finalize")
  active <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = active_root$tracking_id, tracking_role = "primary")
  db <- dsHPC:::.db_connect()
  dsHPC:::.store_update_job(db, active$job_id, state = "RUNNING",
    finished_at = NA_character_)
  dsHPC:::.db_close(db)
  attached <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = active_root$tracking_id, tracking_role = "primary",
    tracking_finalize = TRUE)
  expect_identical(attached$job_id, active$job_id)
  db <- dsHPC:::.db_connect()
  marked <- dsHPC:::.tracking_get_root(db, active_root$tracking_id)
  dsHPC:::.db_close(db)
  expect_identical(as.character(marked$finalizing_job_id), active$job_id)

  completed_root <- trusted_hpc_call(hpcTrackingCreateInternal,
    "completed-finalize")
  completed <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = completed_root$tracking_id, tracking_role = "primary")
  recovered <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec(),
    tracking_id = completed_root$tracking_id, tracking_role = "primary",
    tracking_finalize = TRUE)
  expect_identical(recovered$job_id, completed$job_id)
  expect_null(recovered$.dshpc_capability)
  expect_silent(trusted_hpc_call(hpcTrackingPublishOutputInternal,
    completed_root$tracking_id, recovered$job_id, "values",
    public_name = "primary_output", classification = "server_reusable"))
  tracking_id <- completed_root$tracking_id
  expect_identical(hpcTrackingStatusDS(tracking_id)$state, "terminal")
})

test_that("collection execution cardinality is hidden behind one root", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  root <- trusted_hpc_call(hpcTrackingCreateInternal, "collection-seven")
  handles <- lapply(seq_len(7L), function(i) {
    trusted_hpc_call(hpcSubmitInternal,
      shared_test_spec(value = rep(i, 5L), visibility = "private"),
      tracking_id = root$tracking_id)
  })
  page <- hpcTrackingListDS()
  expect_named(page, c("items", "next_cursor", "has_more", "schema"))
  expect_equal(nrow(page$items), 1L)
  expect_identical(page$items$tracking_id, root$tracking_id)
  expect_named(page$items, c("tracking_id", "state", "is_done", "kind"))
  expect_false(any(c("owner", "label", "name", "progress", "submitted_at",
    "total_steps") %in% names(page$items)))

  db <- dsHPC:::.db_connect()
  children <- DBI::dbGetQuery(db,
    "SELECT job_id FROM tracking_jobs WHERE tracking_id = ?",
    params = list(root$tracking_id))
  dsHPC:::.db_close(db)
  expect_equal(nrow(children), 7L)
  expect_equal(length(handles), 7L)
})

test_that("shared results cross only the closed client-safe boundary", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared", nfilter.subset = 3))

  handle <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())
  tracking_id <- handle$tracking_id
  safe_path <- file.path(home, "artifacts", handle$job_id, "summary.rds")
  unsafe_path <- file.path(home, "artifacts", handle$job_id, "secret.rds")
  identifiers_path <- file.path(home, "artifacts", handle$job_id,
    "identifiers.rds")
  saveRDS(list(n_samples = 4L), safe_path)
  saveRDS(list(patient = "secret"), unsafe_path)
  saveRDS(data.frame(patient = c("alice", "bob", "carol")),
    identifiers_path)
  db <- dsHPC:::.db_connect()
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "approved", "summary",
    safe_path, safe_for_client = TRUE)
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "private_table",
    "emit_value", unsafe_path, reuse_class = "server_reusable")
  dsHPC:::.db_register_output(db, handle$job_id, 1L,
    "patient_identifiers", "aggregate_result", identifiers_path,
    safe_for_client = TRUE)
  projected <- dsHPC:::.tracking_public_output_entries(db, tracking_id)
  summary_alias <- projected$public_name[projected$name == "approved"]
  server_alias <- projected$public_name[projected$reuse_class ==
    "server_reusable"]
  expect_identical(server_alias, "output_001")
  expect_identical(summary_alias, "output_002")
  dsHPC:::.db_close(db)

  result <- hpcTrackingResultDS(tracking_id)
  expect_true(result$ready)
  expect_equal(vapply(result$summaries, `[[`, character(1), "name"),
    summary_alias)
  expect_false(grepl("secret|patient|private_table|approved|values",
    paste(capture.output(str(result)), collapse = "\n")))
  raw_job_id <- handle$job_id
  expect_error(hpcResultDS(raw_job_id), "access denied", fixed = TRUE)

  outputs <- hpcTrackingOutputsDS(tracking_id)
  expect_identical(outputs$name, projected$public_name)
  expect_true(all(grepl("^output_[0-9]{3}$", outputs$name)))
  expect_true(all(outputs$kind %in% c("summary", "server_object")))
  expect_false(grepl("patient|private_table|approved|values|emit_value",
    paste(capture.output(str(outputs)), collapse = "\n")))
  expect_lte(nrow(outputs), 2L)
  ref <- hpcTrackingAssignOutputDS(tracking_id, server_alias)
  expect_s3_class(ref, "dshpc_output_reference")
  expect_named(ref, c("tracking_id", "output_name", "kind", "classification"))
  expect_false(any(c("path", "value", "provider", "reference", "job_id") %in%
    names(ref)))
  loaded <- trusted_hpc_call(hpcTrackingResolveOutputInternal, ref)
  expect_identical(as.integer(loaded), 1:5)
  expect_identical(attr(loaded, "dshpc.provenance")$classification,
    "server_reusable")
  private <- trusted_hpc_call(hpcTrackingResolveOutputInternal,
    tracking_id, "private_table")
  expect_identical(private$patient, "secret")
})

test_that("a summary-only root keeps the fixed output_002 alias", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared", nfilter.subset = 3))

  handle <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())
  summary_path <- file.path(home, "artifacts", handle$job_id,
    "summary-only.rds")
  saveRDS(list(n_samples = 4L), summary_path)
  db <- dsHPC:::.db_connect()
  DBI::dbExecute(db, "DELETE FROM outputs WHERE job_id = ?",
    params = list(handle$job_id))
  dsHPC:::.db_register_output(db, handle$job_id, 1L, "approved", "summary",
    summary_path, safe_for_client = TRUE)
  dsHPC:::.db_close(db)

  tracking_id <- handle$tracking_id
  outputs <- hpcTrackingOutputsDS(tracking_id)
  expect_identical(outputs$name, "output_002")
  expect_identical(outputs$kind, "summary")
  expect_identical(outputs$classification, "client_safe")
  result <- hpcTrackingResultDS(tracking_id)
  expect_identical(result$summaries[[1L]]$name, "output_002")
  expect_error(hpcTrackingAssignOutputDS(tracking_id, "output_001"),
    "Reusable output not found", fixed = TRUE)
})

test_that("a result without a client-safe summary keeps its fixed schema", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  handle <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())
  tracking_id <- handle$tracking_id
  result <- hpcTrackingResultDS(tracking_id)
  expect_identical(names(result),
    c("ready", "summaries", "available_outputs"))
  expect_true(result$ready)
  expect_identical(result$summaries, list())
  expect_identical(result$available_outputs, list())
  expect_identical(hpcTrackingOutputsDS(tracking_id)$name,
    "output_001")
})

test_that("DSLite assigns only an opaque reusable reference", {
  skip_if_not_installed("DSLite")
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))
  handle <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())
  tracking_id <- handle$tracking_id

  server <- DSLite::newDSLiteServer(config = .dshpc_dslite_config(),
    strict = TRUE)
  env <- environment()
  assign("tracking_server", server, envir = env)
  withr::local_options(list(datashield.env = env))
  first <- DSI::dsConnect(DSLite::DSLite(), name = "first",
    url = "tracking_server")
  second <- DSI::dsConnect(DSLite::DSLite(), name = "second",
    url = "tracking_server")
  on.exit(try(DSI::dsDisconnect(first), silent = TRUE), add = TRUE)
  on.exit(try(DSI::dsDisconnect(second), silent = TRUE), add = TRUE)

  expect_silent(DSI::dsAssignExpr(first, "shared_values",
    call("hpcTrackingAssignOutputDS", tracking_id, "output_001"),
    async = FALSE))
  assigned <- get("shared_values", envir = server$getSession(first@sid),
    inherits = FALSE)
  expect_s3_class(assigned, "dshpc_output_reference")
  expect_false(any(c("path", "value", "provider", "reference", "job_id") %in%
    names(assigned)))
  status <- DSI::dsFetch(DSI::dsAggregate(second,
    call("hpcTrackingStatusDS", tracking_id), async = FALSE))
  expect_identical(status$state, "terminal")
  expect_error(DSI::dsFetch(DSI::dsAggregate(second,
    call("hpcTrackingAssignOutputDS", tracking_id, "output_001"),
    async = FALSE)),
    "does not allow expression", fixed = TRUE)
})

test_that("private jobs and scoped deployments stay capability-only", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))
  private <- trusted_hpc_call(hpcSubmitInternal,
    shared_test_spec(visibility = "private"))
  expect_null(private$tracking_id)
  private_id <- private$job_id
  expect_error(hpcTrackingStatusDS(private_id), "Tracked job not found",
    fixed = TRUE)
  expect_equal(hpcStatusDS(private)$state, "FINISHED")

  withr::local_options(list(dshpc.queue_visibility = "scoped"))
  scoped_first <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())
  scoped_second <- trusted_hpc_call(hpcSubmitInternal, shared_test_spec())
  expect_false(identical(scoped_first$job_id, scoped_second$job_id))
  expect_null(scoped_first$tracking_id)
  expect_null(scoped_second$tracking_id)
  expect_true(scoped_second$deduplicated)
  expect_match(scoped_first$.dshpc_capability, "^cap_")
  expect_match(scoped_second$.dshpc_capability, "^cap_")
  expect_identical(trusted_hpc_call(hpcLoadOutputInternal, scoped_second,
    "values", required_label = "dsHPC_test"), 1:5)
  expect_error(hpcTrackingListDS(), "Shared job tracking is disabled",
    fixed = TRUE)
  expect_error(hpcTrackingStatusDS("trk_00000000-0000-4000-8000-000000000000"),
    "Shared job tracking is disabled", fixed = TRUE)

  scoped_root <- trusted_hpc_call(hpcTrackingCreateInternal, "scoped-output")
  trusted_hpc_call(hpcTrackingPublishReferenceInternal,
    scoped_root$tracking_id, "knowledge", "asset_scoped")
  trusted_hpc_call(hpcTrackingFinishInternal, scoped_root$tracking_id, TRUE)
  resolved <- trusted_hpc_call(hpcTrackingResolveOutputInternal,
    scoped_root$tracking_id, "knowledge")
  expect_s3_class(resolved, "dshpc_domain_output_reference")
  expect_identical(resolved$reference, "asset_scoped")
})

test_that("tracking pages allow deterministic recovery without a raw count", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))
  roots <- lapply(seq_len(3L), function(i) {
    trusted_hpc_call(hpcTrackingCreateInternal, paste0("page-", i))
  })

  first <- hpcTrackingListDS(limit = 2L)
  expect_equal(nrow(first$items), 2L)
  expect_true(first$has_more)
  expect_match(first$next_cursor, "^cur_")
  cursor <- first$next_cursor
  second <- hpcTrackingListDS(limit = 2L, cursor = cursor)
  expect_equal(nrow(second$items), 1L)
  expect_false(second$has_more)
  expect_null(second$next_cursor)
  expect_length(intersect(first$items$tracking_id,
    second$items$tracking_id), 0L)
  expect_setequal(c(first$items$tracking_id, second$items$tracking_id),
    vapply(roots, `[[`, character(1), "tracking_id"))
  expect_false(any(c("count", "total", "timestamp") %in% names(first)))
})

test_that("tracking pagination rejects ambiguous bounds and cursors", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared"))

  for (value in list(0, 501, 1.5, Inf, NA_real_, c(1, 2), "10")) {
    expect_error(hpcTrackingListDS(limit = value),
      "one whole number", fixed = TRUE)
  }
  for (value in list("", NA_character_, character(0), c("bad", "bad"))) {
    expect_error(hpcTrackingListDS(cursor = value),
      "Tracking cursor is invalid", fixed = TRUE)
  }
})

test_that("GC removes unpublished children but keeps referenced knowledge", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared", dshpc.job_expiry_hours = 0))

  root <- trusted_hpc_call(hpcTrackingCreateInternal, "gc-collection")
  db <- dsHPC:::.db_connect()
  stale_root <- dsHPC:::.tracking_create(db, "dsHPC", "stale-root",
    implicit = TRUE)$tracking_id
  DBI::dbExecute(db,
    "UPDATE tracking_roots SET created_at = ? WHERE tracking_id = ?",
    params = list("2000-01-01T00:00:00.000Z", stale_root))
  dsHPC:::.db_close(db)
  kept <- trusted_hpc_call(hpcSubmitInternal,
    shared_test_spec(visibility = "private"), tracking_id = root$tracking_id)
  discarded <- trusted_hpc_call(hpcSubmitInternal,
    shared_test_spec(value = 6:10, visibility = "private"),
    tracking_id = root$tracking_id)
  trusted_hpc_call(hpcTrackingPublishOutputInternal, root$tracking_id, kept,
    "values", public_name = "features", classification = "server_reusable")
  trusted_hpc_call(hpcTrackingFinishInternal, root$tracking_id, TRUE)

  db <- dsHPC:::.db_connect()
  old <- format(Sys.time() - 3600, "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  dsHPC:::.store_update_job(db, kept$job_id, finished_at = old)
  dsHPC:::.store_update_job(db, discarded$job_id, finished_at = old)
  DBI::dbExecute(db,
    "INSERT INTO resource_leases
       (job_id, resource, amount, acquired_at) VALUES (?, ?, ?, ?)",
    params = list(discarded$job_id, "cpu", 1, old))
  dsHPC:::.worker_gc(db)
  expect_false(is.null(dsHPC:::.store_get_job(db, kept$job_id)))
  expect_null(dsHPC:::.store_get_job(db, discarded$job_id))
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM resource_leases WHERE job_id = ?",
    params = list(discarded$job_id))$n, 0)
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM tracking_roots WHERE tracking_id = ?",
    params = list(root$tracking_id))$n, 1)
  expect_equal(DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM tracking_roots WHERE tracking_id = ?",
    params = list(stale_root))$n, 0)
  dsHPC:::.db_close(db)

  tracking_id <- root$tracking_id
  output_name <- hpcTrackingOutputsDS(tracking_id)$name[[1L]]
  reference <- hpcTrackingAssignOutputDS(tracking_id, output_name)
  value <- trusted_hpc_call(hpcTrackingResolveOutputInternal, reference)
  expect_identical(as.integer(value), 1:5)
})

test_that("GC removes failed implicit execution but retains terminal tracking", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared", dshpc.job_expiry_hours = 0))

  failed <- trusted_hpc_call(hpcSubmitInternal,
    shared_test_spec(type = "not_a_session_runner"))
  tracking_id <- failed$tracking_id
  db <- dsHPC:::.db_connect()
  dsHPC:::.store_update_job(db, failed$job_id,
    finished_at = "2000-01-01T00:00:00.000Z")
  dsHPC:::.worker_gc(db)
  expect_null(dsHPC:::.store_get_job(db, failed$job_id))
  root <- dsHPC:::.tracking_get_root(db, tracking_id)
  expect_identical(root$lifecycle, "SEALED")
  expect_identical(as.integer(root$success), 0L)
  dsHPC:::.db_close(db)

  expect_identical(hpcTrackingStatusDS(tracking_id)$state, "terminal")
  result <- hpcTrackingResultDS(tracking_id)
  expect_false(result$ready)
  expect_identical(result$error, "Job execution failed.")
})

test_that("GC does not retain a successful implicit job without knowledge", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared", dshpc.job_expiry_hours = 0))
  spec <- shared_test_spec()
  spec$steps[[1L]] <- list(type = "assign_expr", plane = "session",
    expr = "1 + 1", symbol = "assigned_value")

  completed <- trusted_hpc_call(hpcSubmitInternal, spec)
  tracking_id <- completed$tracking_id
  db <- dsHPC:::.db_connect()
  dsHPC:::.store_update_job(db, completed$job_id,
    finished_at = "2000-01-01T00:00:00.000Z")
  dsHPC:::.worker_gc(db)
  expect_null(dsHPC:::.store_get_job(db, completed$job_id))
  root <- dsHPC:::.tracking_get_root(db, tracking_id)
  expect_identical(root$lifecycle, "SEALED")
  expect_identical(as.integer(root$success), 1L)
  dsHPC:::.db_close(db)

  expect_identical(hpcTrackingStatusDS(tracking_id)$state, "terminal")
  expect_equal(nrow(hpcTrackingOutputsDS(tracking_id)), 0L)
})

test_that("GC seals an expired incomplete primary before removing it", {
  home <- setup_test_home()
  on.exit(cleanup_test_home(home), add = TRUE)
  withr::local_options(list(dshpc.home = home,
    dshpc.queue_visibility = "shared", dshpc.job_expiry_hours = 0))

  root <- trusted_hpc_call(hpcTrackingCreateInternal, "expired-primary")
  failed <- trusted_hpc_call(hpcSubmitInternal,
    shared_test_spec(type = "not_a_session_runner"),
    tracking_id = root$tracking_id, tracking_role = "primary")
  db <- dsHPC:::.db_connect()
  dsHPC:::.store_update_job(db, failed$job_id,
    finished_at = "2000-01-01T00:00:00.000Z")
  dsHPC:::.worker_gc(db)
  expect_null(dsHPC:::.store_get_job(db, failed$job_id))
  sealed <- dsHPC:::.tracking_get_root(db, root$tracking_id)
  expect_identical(sealed$lifecycle, "SEALED")
  expect_identical(as.integer(sealed$success), 0L)
  dsHPC:::.db_close(db)

  tracking_id <- root$tracking_id
  expect_identical(hpcTrackingStatusDS(tracking_id)$state, "terminal")
  retry <- trusted_hpc_call(hpcTrackingCreateInternal, "expired-primary")
  expect_false(retry$reused)
  expect_false(identical(retry$tracking_id, tracking_id))
})
