# Module: Durable HPC execution units
#
# An analyst selects a Resource, but executable configuration remains in an
# administrator-owned catalogue.  The selected, non-secret configuration is
# sealed into the durable job specification so a worker never depends on the
# lifetime of the DataSHIELD session.

.DSHPC_UNIT_SCHEMA_VERSION <- 1L
.DSHPC_UNIT_TYPES <- c("slurm", "external", "kubernetes")
.DSHPC_RUNTIME_TYPES <- c("embedded", .DSHPC_UNIT_TYPES)

.dshpc_units_file <- function() {
  value <- .dshpc_option("units_file",
    Sys.getenv("DSHPC_UNITS_FILE", unset = ""))
  value <- as.character(value %||% "")[1L]
  if (is.na(value) || !nzchar(value) || nchar(value, type = "bytes") > 4096L) {
    stop("HPC unit catalogue is not configured.", call. = FALSE)
  }
  path.expand(value)
}

.dshpc_unit_fail <- function() {
  condition <- structure(
    list(message = "HPC unit resource is unavailable.", call = NULL),
    class = c("dshpc_unit_unavailable", "error", "condition"))
  stop(condition)
}

.dshpc_unit_named_list <- function(x, allowed, required = character(0)) {
  if (!is.list(x) || inherits(x, "data.frame")) .dshpc_unit_fail()
  nms <- names(x)
  if (length(x) > 0L && (is.null(nms) || anyNA(nms) || any(!nzchar(nms)) ||
      anyDuplicated(nms))) .dshpc_unit_fail()
  if (length(setdiff(nms %||% character(0), allowed)) > 0L ||
      length(setdiff(required, nms %||% character(0))) > 0L) {
    .dshpc_unit_fail()
  }
  x
}

.dshpc_unit_scalar_string <- function(x, pattern = NULL, allow_empty = FALSE,
                                       max_bytes = 1024L) {
  ok <- is.character(x) && length(x) == 1L && !is.na(x) &&
    nchar(x, type = "bytes") <= max_bytes && !grepl("[\r\n]", x) &&
    (isTRUE(allow_empty) || nzchar(x))
  if (ok && !is.null(pattern)) ok <- grepl(pattern, x, perl = TRUE)
  if (!isTRUE(ok)) .dshpc_unit_fail()
  x
}

.dshpc_unit_string_vector <- function(x, pattern, allow_empty = FALSE,
                                       max_items = 256L,
                                       max_bytes = 256L) {
  values <- as.character(unlist(x %||% list(), use.names = FALSE))
  if (length(values) > max_items || anyNA(values) || anyDuplicated(values) ||
      any(nchar(values, type = "bytes") > max_bytes) ||
      any(grepl("[\r\n]", values)) ||
      (!isTRUE(allow_empty) && length(values) == 0L) ||
      any(!grepl(pattern, values, perl = TRUE))) {
    .dshpc_unit_fail()
  }
  sort(unique(values), method = "radix")
}

.dshpc_unit_bool <- function(x) {
  if (!is.logical(x) || length(x) != 1L || is.na(x)) .dshpc_unit_fail()
  isTRUE(x)
}

.dshpc_unit_integer <- function(x, minimum = 0L, maximum = .Machine$integer.max) {
  value <- suppressWarnings(as.numeric(x))
  if (length(value) != 1L || is.na(value) || !is.finite(value) ||
      value != floor(value) || value < minimum || value > maximum) {
    .dshpc_unit_fail()
  }
  as.integer(value)
}

.dshpc_unit_command <- function(x, required = FALSE,
                                require_available = TRUE) {
  if (is.null(x) && !isTRUE(required)) return(NULL)
  value <- .dshpc_unit_scalar_string(x, max_bytes = 2048L)
  if (!startsWith(value, "/") || grepl("(^|/)\\.\\.(/|$)", value)) {
    .dshpc_unit_fail()
  }
  if (!isTRUE(require_available)) return(value)
  if (!file.exists(value) || dir.exists(value) ||
      file.access(value, mode = 1L) != 0L) .dshpc_unit_fail()
  normalizePath(value, winslash = "/", mustWork = TRUE)
}

.dshpc_unit_optional_string <- function(x, pattern = NULL,
                                         max_bytes = 1024L) {
  if (is.null(x)) return(NULL)
  .dshpc_unit_scalar_string(x, pattern = pattern, allow_empty = TRUE,
    max_bytes = max_bytes)
}

.dshpc_unit_paths <- function(x) {
  if (is.null(x)) return(NULL)
  if (!is.list(x) || length(x) == 0L || length(x) > 32L) .dshpc_unit_fail()
  rows <- lapply(x, function(item) {
    item <- .dshpc_unit_named_list(item,
      c("local", "backend"), c("local", "backend"))
    local <- .dshpc_unit_scalar_string(item$local, max_bytes = 2048L)
    backend <- .dshpc_unit_scalar_string(item$backend, max_bytes = 2048L)
    if (!startsWith(local, "/") || !startsWith(backend, "/") ||
        grepl("(^|/)\\.\\.(/|$)", local) ||
        grepl("(^|/)\\.\\.(/|$)", backend)) .dshpc_unit_fail()
    trim_root <- function(path) if (identical(path, "/")) "/" else
      sub("/+$", "", path)
    list(local = trim_root(local), backend = trim_root(backend))
  })
  local <- vapply(rows, `[[`, character(1), "local")
  backend <- vapply(rows, `[[`, character(1), "backend")
  if (anyDuplicated(local) || anyDuplicated(backend)) .dshpc_unit_fail()
  rows[order(local, method = "radix")]
}

.dshpc_unit_config_names <- function(type) {
  common <- c(
    "default_timeout_secs", "max_retries",
    "external_enforce_local_resources",
    "external_enforce_runner_concurrency",
    "backend_path_mappings", "backend_gpu_count",
    "backend_request_optional_gpus", "backend_capabilities_cmd",
    "backend_capabilities_ttl_secs", "container_runtime",
    "container_pull", "container_network",
    "container_run_as_current_user")
  backend <- switch(type,
    slurm = c("slurm_sbatch", "slurm_squeue", "slurm_sacct",
      "slurm_scancel", "slurm_sinfo", "slurm_partition", "slurm_account",
      "slurm_qos", "slurm_time",
      "slurm_request_optional_gpus"),
    external = c("external_submit_cmd", "external_status_cmd",
      "external_cancel_cmd"),
    kubernetes = c("kubernetes_kubectl", "kubernetes_context",
      "kubernetes_namespace", "kubernetes_service_account",
      "kubernetes_image", "kubernetes_image_pull_policy",
      "kubernetes_pvc", "kubernetes_mount_path",
      "kubernetes_backoff_limit",
      "kubernetes_ttl_seconds_after_finished"),
    character(0))
  c(common, backend)
}

.dshpc_unit_runtime_defaults <- function(type, schema_version = 1L) {
  if (!identical(as.integer(schema_version), 1L) ||
      !type %in% .DSHPC_RUNTIME_TYPES) .dshpc_unit_fail()
  common <- list(
    default_timeout_secs = 86400L,
    max_retries = 3L,
    external_enforce_local_resources = FALSE,
    external_enforce_runner_concurrency = FALSE,
    backend_path_mappings = NULL,
    backend_gpu_count = "auto",
    backend_request_optional_gpus = "auto",
    backend_capabilities_cmd = NULL,
    backend_capabilities_ttl_secs = 30L,
    container_runtime = "auto",
    container_pull = "missing",
    container_network = "none",
    container_extra_args = NULL,
    container_run_as_current_user = FALSE)
  backend <- switch(type,
    embedded = list(),
    slurm = list(
      slurm_sbatch = NULL, slurm_squeue = NULL, slurm_sacct = NULL,
      slurm_scancel = NULL, slurm_sinfo = NULL, slurm_partition = "",
      slurm_account = "", slurm_qos = "", slurm_time = "",
      slurm_extra_args = NULL, slurm_request_optional_gpus = FALSE),
    external = list(
      external_submit_cmd = NULL, external_status_cmd = NULL,
      external_cancel_cmd = NULL),
    kubernetes = list(
      kubernetes_kubectl = NULL, kubernetes_context = "",
      kubernetes_namespace = "default", kubernetes_service_account = "",
      kubernetes_image = "", kubernetes_image_pull_policy = "IfNotPresent",
      kubernetes_pvc = NULL, kubernetes_mount_path = "",
      kubernetes_backoff_limit = 0L,
      kubernetes_ttl_seconds_after_finished = 86400L))
  c(common, backend)
}

.dshpc_normalize_unit_config <- function(config, type,
                                          require_available = TRUE) {
  allowed <- .dshpc_unit_config_names(type)
  config <- .dshpc_unit_named_list(config %||% list(), allowed)
  out <- list()

  command_names <- intersect(names(config), c(
    "backend_capabilities_cmd", "slurm_sbatch", "slurm_squeue",
    "slurm_sacct", "slurm_scancel", "slurm_sinfo",
    "external_submit_cmd", "external_status_cmd", "external_cancel_cmd",
    "kubernetes_kubectl"))
  for (name in command_names) {
    out[[name]] <- .dshpc_unit_command(config[[name]],
      require_available = require_available)
  }

  if (identical(type, "slurm")) {
    if (is.null(out$slurm_sbatch) || is.null(out$slurm_scancel) ||
        (is.null(out$slurm_squeue) && is.null(out$slurm_sacct))) {
      .dshpc_unit_fail()
    }
  }
  if (identical(type, "external") &&
      (is.null(out$external_submit_cmd) || is.null(out$external_status_cmd))) {
    .dshpc_unit_fail()
  }
  if (identical(type, "kubernetes") && is.null(out$kubernetes_kubectl)) {
    .dshpc_unit_fail()
  }

  bool_names <- intersect(names(config), c(
    "external_enforce_local_resources",
    "external_enforce_runner_concurrency", "container_run_as_current_user",
    "slurm_request_optional_gpus"))
  for (name in bool_names) out[[name]] <- .dshpc_unit_bool(config[[name]])

  integer_specs <- list(
    default_timeout_secs = c(0L, .Machine$integer.max),
    max_retries = c(0L, 100L),
    backend_capabilities_ttl_secs = c(0L, 86400L),
    kubernetes_backoff_limit = c(0L, 100L),
    kubernetes_ttl_seconds_after_finished = c(0L, .Machine$integer.max))
  for (name in intersect(names(config), names(integer_specs))) {
    bounds <- integer_specs[[name]]
    out[[name]] <- .dshpc_unit_integer(config[[name]], bounds[[1]], bounds[[2]])
  }

  if ("backend_gpu_count" %in% names(config)) {
    value <- config$backend_gpu_count
    if (is.character(value) && length(value) == 1L &&
        identical(tolower(value), "auto")) {
      out$backend_gpu_count <- "auto"
    } else {
      out$backend_gpu_count <- .dshpc_unit_integer(value, 0L, 4096L)
    }
  }
  if ("backend_request_optional_gpus" %in% names(config)) {
    value <- tolower(.dshpc_unit_scalar_string(
      as.character(config$backend_request_optional_gpus),
      pattern = "^(auto|always|never)$", max_bytes = 16L))
    out$backend_request_optional_gpus <- value
  }

  if ("backend_path_mappings" %in% names(config)) {
    out$backend_path_mappings <- .dshpc_unit_paths(config$backend_path_mappings)
  }
  string_patterns <- list(
    slurm_partition = "^[A-Za-z0-9._-]*$",
    slurm_account = "^[A-Za-z0-9._-]*$",
    slurm_qos = "^[A-Za-z0-9._-]*$",
    slurm_time = "^[0-9:-]*$",
    container_runtime = "^(auto|docker|podman|apptainer|singularity|none)$",
    container_pull = "^(missing|always|never)$",
    container_network = "^[A-Za-z0-9._-]*$",
    kubernetes_context = "^[A-Za-z0-9._:/-]*$",
    kubernetes_namespace = "^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$",
    kubernetes_service_account = "^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$",
    kubernetes_image = "^[A-Za-z0-9][A-Za-z0-9._/@:-]*$",
    kubernetes_image_pull_policy = "^(Always|IfNotPresent|Never)$",
    kubernetes_pvc = "^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$",
    kubernetes_mount_path = "^/[^\r\n]*$")
  for (name in intersect(names(config), names(string_patterns))) {
    out[[name]] <- .dshpc_unit_optional_string(config[[name]],
      pattern = string_patterns[[name]], max_bytes = 1024L)
  }
  if (identical(type, "kubernetes") &&
      (is.null(out$kubernetes_pvc) || !nzchar(out$kubernetes_pvc))) {
    .dshpc_unit_fail()
  }

  # Every accepted input must have been normalized above. This assertion keeps
  # additions to the allowlist from silently bypassing type checks.
  if (length(setdiff(names(config), names(out))) > 0L) .dshpc_unit_fail()
  out[sort(names(out), method = "radix")]
}

.dshpc_unit_seal <- function(source, unit_id, resource_pool_id, type, config,
                              allowed_labels, allowed_runners) {
  payload <- .canonicalise_spec(list(
    schema_version = .DSHPC_UNIT_SCHEMA_VERSION,
    source = source,
    unit_id = unit_id,
    resource_pool_id = resource_pool_id,
    type = type,
    config = config,
    allowed_labels = as.list(allowed_labels),
    allowed_runners = as.list(allowed_runners)))
  digest::digest(as.character(jsonlite::toJSON(payload, auto_unbox = TRUE,
    null = "null")), algo = "sha256", serialize = FALSE)
}

.dshpc_validate_unit_snapshot <- function(snapshot, spec = NULL) {
  snapshot <- .dshpc_unit_named_list(snapshot,
    c("schema_version", "source", "unit_id", "resource_pool_id", "type",
      "config_seal", "config", "allowed_labels", "allowed_runners"),
    c("schema_version", "source", "unit_id", "resource_pool_id", "type",
      "config_seal", "config", "allowed_labels", "allowed_runners"))
  schema_version <- .dshpc_unit_integer(snapshot$schema_version, 1L, 1L)
  source <- .dshpc_unit_scalar_string(snapshot$source,
    pattern = "^(resource|site_default)$", max_bytes = 16L)
  unit_id <- .dshpc_unit_scalar_string(snapshot$unit_id,
    pattern = "^[a-z][a-z0-9._-]{0,63}$", max_bytes = 64L)
  resource_pool_id <- .dshpc_unit_scalar_string(snapshot$resource_pool_id,
    pattern = "^[a-z][a-z0-9._-]{0,63}$", max_bytes = 64L)
  type <- .dshpc_unit_scalar_string(snapshot$type,
    pattern = "^(embedded|slurm|external|kubernetes)$", max_bytes = 16L)
  if ((identical(source, "resource") &&
       (identical(type, "embedded") || identical(unit_id, "site-default"))) ||
      (identical(source, "site_default") &&
       !identical(unit_id, "site-default"))) .dshpc_unit_fail()
  config <- .dshpc_normalize_unit_config(snapshot$config, type,
    require_available = FALSE)
  labels <- .dshpc_unit_string_vector(snapshot$allowed_labels,
    "^[A-Za-z][A-Za-z0-9._-]{0,127}$", max_items = 64L,
    max_bytes = 128L)
  runners <- .dshpc_unit_string_vector(snapshot$allowed_runners,
    "^[A-Za-z0-9_]+$", allow_empty = TRUE, max_items = 512L,
    max_bytes = 128L)
  seal <- .dshpc_unit_scalar_string(snapshot$config_seal,
    pattern = "^[0-9a-f]{64}$", max_bytes = 64L)
  expected <- .dshpc_unit_seal(source, unit_id, resource_pool_id, type,
    config, labels, runners)
  if (!identical(seal, expected)) .dshpc_unit_fail()

  out <- list(schema_version = schema_version, source = source,
    unit_id = unit_id, resource_pool_id = resource_pool_id,
    type = type, config_seal = seal, config = config,
    allowed_labels = as.list(labels), allowed_runners = as.list(runners))
  if (!is.null(spec)) .dshpc_authorize_unit_spec(out, spec)
  out
}

.dshpc_authorize_unit_spec <- function(snapshot, spec) {
  label <- as.character(spec$label %||% "")[1L]
  labels <- as.character(unlist(snapshot$allowed_labels, use.names = FALSE))
  if (!nzchar(label) || !any(vapply(labels, function(package) {
      .dshpc_label_matches_package(label, package)
    }, logical(1)))) .dshpc_unit_fail()

  allowed <- as.character(unlist(snapshot$allowed_runners,
    use.names = FALSE))
  if (length(allowed) > 0L) {
    runners <- unique(vapply(spec$steps %||% list(), function(step) {
      as.character(step$runner %||% "")[1L]
    }, character(1)))
    runners <- runners[nzchar(runners)]
    if (length(setdiff(runners, allowed)) > 0L) .dshpc_unit_fail()
  }
  invisible(TRUE)
}

.dshpc_load_units <- function() {
  path <- .dshpc_units_file()
  if (!file.exists(path) || dir.exists(path) || .dshpc_path_is_symlink(path) ||
      is.na(file.info(path)$size) || file.info(path)$size > 1048576) {
    .dshpc_unit_fail()
  }
  raw <- tryCatch(yaml::yaml.load_file(path, eval.expr = FALSE),
    error = function(e) .dshpc_unit_fail())
  raw <- .dshpc_unit_named_list(raw, c("schema_version", "units"),
    c("schema_version", "units"))
  if (!identical(.dshpc_unit_integer(raw$schema_version, 1L, 1L), 1L)) {
    .dshpc_unit_fail()
  }
  units <- raw$units
  if (!is.list(units) || length(units) == 0L || is.null(names(units)) ||
      anyNA(names(units)) || anyDuplicated(names(units)) ||
      any(!grepl("^[a-z][a-z0-9._-]{0,63}$", names(units)))) {
    .dshpc_unit_fail()
  }
  units
}

.dshpc_catalogue_unit <- function(unit_id, type) {
  units <- .dshpc_load_units()
  entry <- units[[unit_id]]
  if (is.null(entry)) .dshpc_unit_fail()
  entry <- .dshpc_unit_named_list(entry,
    c("type", "enabled", "resource_pool_id", "allowed_labels",
      "allowed_runners", "config"),
    c("type", "enabled", "allowed_labels", "config"))
  entry_type <- .dshpc_unit_scalar_string(entry$type,
    pattern = "^(slurm|external|kubernetes)$", max_bytes = 16L)
  if (!identical(entry_type, type) ||
      ("enabled" %in% names(entry) && !.dshpc_unit_bool(entry$enabled))) {
    .dshpc_unit_fail()
  }
  labels <- .dshpc_unit_string_vector(entry$allowed_labels,
    "^[A-Za-z][A-Za-z0-9._-]{0,127}$", max_items = 64L,
    max_bytes = 128L)
  runners <- .dshpc_unit_string_vector(entry$allowed_runners %||% list(),
    "^[A-Za-z0-9_]+$", allow_empty = TRUE, max_items = 512L,
    max_bytes = 128L)
  resource_pool_id <- .dshpc_unit_scalar_string(
    entry$resource_pool_id %||% unit_id,
    pattern = "^[a-z][a-z0-9._-]{0,63}$", max_bytes = 64L)
  config <- .dshpc_normalize_unit_config(entry$config, type)
  seal <- .dshpc_unit_seal("resource", unit_id, resource_pool_id, type,
    config, labels, runners)
  .dshpc_validate_unit_snapshot(list(
    schema_version = .DSHPC_UNIT_SCHEMA_VERSION,
    source = "resource", unit_id = unit_id,
    resource_pool_id = resource_pool_id,
    type = type,
    config_seal = seal,
    config = config,
    allowed_labels = as.list(labels),
    allowed_runners = as.list(runners)))
}

.dshpc_parse_unit_url <- function(url) {
  url <- .dshpc_unit_scalar_string(url, max_bytes = 256L)
  match <- regexec(
    "^dshpc\\+unit://(slurm|external|kubernetes)/([a-z][a-z0-9._-]{0,63})$",
    url, perl = TRUE)
  parts <- regmatches(url, match)[[1L]]
  if (length(parts) != 3L) .dshpc_unit_fail()
  list(type = parts[[2L]], unit_id = parts[[3L]])
}

.dshpc_parse_unit_locator <- function(locator) {
  locator <- .dshpc_unit_scalar_string(locator, max_bytes = 256L)
  if (startsWith(locator, "dshpc-unit:")) {
    locator <- sub("^dshpc-unit:", "dshpc+unit://", locator)
  }
  .dshpc_parse_unit_url(locator)
}

.dshpc_unit_locator <- function(resource) {
  url <- .dshpc_unit_scalar_string(resource$url, max_bytes = 2048L)
  format <- resource$format
  if (!is.null(format)) {
    format <- .dshpc_unit_scalar_string(as.character(format),
      allow_empty = TRUE, max_bytes = 256L)
  }
  format_locator <- if (!is.null(format) && nzchar(format) &&
      startsWith(format, "dshpc-unit:")) {
    sub("^dshpc-unit:", "dshpc+unit://", format)
  } else character(0)
  locators <- c(
    if (startsWith(url, "dshpc+unit://")) url else character(0),
    format_locator)
  locators <- unique(locators)
  if (length(locators) != 1L) .dshpc_unit_fail()
  locators[[1L]]
}

.dshpc_unit_from_resource <- function(resource) {
  if (!inherits(resource, c("resource", "Resource")) || !is.list(resource)) {
    .dshpc_unit_fail()
  }
  resource <- .dshpc_unit_named_list(resource,
    c("name", "url", "identity", "secret", "format"), c("name", "url"))
  .dshpc_unit_scalar_string(resource$name, max_bytes = 256L)
  # Connection credentials belong to the durable worker identity, never to a
  # session Resource. Armadillo may inject a short-lived access JWT in
  # `secret`; validate its shape, then deliberately ignore it.
  if (!is.null(resource$identity) &&
      (length(resource$identity) != 1L || is.na(resource$identity) ||
       nzchar(as.character(resource$identity)))) .dshpc_unit_fail()
  if (!is.null(resource$secret)) {
    .dshpc_unit_scalar_string(as.character(resource$secret),
      allow_empty = TRUE, max_bytes = 65536L)
  }
  parsed <- .dshpc_parse_unit_locator(.dshpc_unit_locator(resource))
  .dshpc_catalogue_unit(parsed$unit_id, parsed$type)
}

.dshpc_sanitized_unit_resource <- function(selection) {
  selection <- .dshpc_validate_unit_snapshot(selection)
  resourcer::newResource(
    name = "dsHPC execution unit",
    url = paste0("dshpc+unit://", selection$type, "/", selection$unit_id))
}

.dshpc_assert_unit_commands_available <- function(snapshot) {
  snapshot <- .dshpc_validate_unit_snapshot(snapshot)
  command_names <- intersect(names(snapshot$config), c(
    "backend_capabilities_cmd", "slurm_sbatch", "slurm_squeue",
    "slurm_sacct", "slurm_scancel", "slurm_sinfo",
    "external_submit_cmd", "external_status_cmd", "external_cancel_cmd",
    "kubernetes_kubectl"))
  for (name in command_names) {
    .dshpc_unit_command(snapshot$config[[name]], require_available = TRUE)
  }
  invisible(TRUE)
}

.dshpc_assert_unit_dispatchable <- function(spec) {
  snapshot <- spec$.dshpc_unit %||% NULL
  if (is.null(snapshot)) return(invisible(TRUE))
  snapshot <- .dshpc_validate_unit_snapshot(snapshot, spec = spec)
  .dshpc_assert_unit_commands_available(snapshot)
  if (identical(snapshot$source, "resource")) {
    current <- .dshpc_catalogue_unit(snapshot$unit_id, snapshot$type)
    if (!identical(current$config_seal, snapshot$config_seal)) {
      .dshpc_unit_fail()
    }
  }
  invisible(TRUE)
}

.dshpc_settings_for_spec <- function(spec,
                                      base_settings = .dshpc_settings()) {
  snapshot <- spec$.dshpc_unit %||% NULL
  if (is.null(snapshot)) return(base_settings)
  snapshot <- .dshpc_validate_unit_snapshot(snapshot, spec = spec)
  overlay <- .dshpc_unit_runtime_defaults(snapshot$type,
    snapshot$schema_version)
  for (name in names(snapshot$config)) {
    overlay[name] <- snapshot$config[name]
  }
  overlay$executor_backend <- snapshot$type
  out <- base_settings
  for (name in names(overlay)) out[name] <- overlay[name]
  out
}

.dshpc_unit_key <- function(spec, settings = .dshpc_settings_for_spec(spec)) {
  snapshot <- spec$.dshpc_unit %||% NULL
  if (is.null(snapshot)) {
    return(paste(.executor_backend_name(settings),
      .dshpc_site_default_pool_id(settings), sep = ":"))
  }
  snapshot <- .dshpc_validate_unit_snapshot(snapshot, spec = spec)
  paste(snapshot$type, snapshot$resource_pool_id, sep = ":")
}

.dshpc_site_default_pool_id <- function(settings = .dshpc_settings()) {
  .dshpc_unit_scalar_string(
    settings$site_default_pool_id %||% "site-default",
    pattern = "^[a-z][a-z0-9._-]{0,63}$", max_bytes = 64L)
}

.dshpc_pin_configured_command <- function(value, fallback = "",
                                           required = FALSE) {
  parts <- .backend_command_parts(value)
  if (length(parts$args) > 0L) .dshpc_unit_fail()
  command <- .backend_resolve_cmd(parts$command, fallback)
  if (!nzchar(command)) {
    if (isTRUE(required)) .dshpc_unit_fail()
    return(NULL)
  }
  .dshpc_unit_command(command, required = required)
}

.dshpc_unit_config_from_settings <- function(settings, type) {
  if (!is.list(settings) || !type %in% .DSHPC_RUNTIME_TYPES) {
    .dshpc_unit_fail()
  }
  if ((identical(type, "slurm") &&
       length(settings$slurm_extra_args %||% character(0)) > 0L) ||
      length(settings$container_extra_args %||% character(0)) > 0L) {
    .dshpc_unit_fail()
  }
  allowed_names <- .dshpc_unit_config_names(type)
  config <- settings[intersect(allowed_names, names(settings))]

  # Empty option values mean "not configured". The schema-versioned runtime
  # defaults explicitly clear them instead of inheriting another unit's value.
  for (name in names(config)) {
    value <- config[[name]]
    if (is.null(value) || length(value) == 0L ||
        (is.character(value) && length(value) == 1L &&
         (is.na(value) || !nzchar(value)))) {
      config[[name]] <- NULL
    }
  }

  mappings <- .backend_path_mappings(settings)
  config$backend_path_mappings <- if (nrow(mappings) == 0L) NULL else
    lapply(seq_len(nrow(mappings)), function(i) list(
      local = mappings$local[[i]], backend = mappings$backend[[i]]))

  command_specs <- list(
    backend_capabilities_cmd = list(fallback = "", required = FALSE))
  if (identical(type, "slurm")) {
    command_specs <- c(command_specs, list(
      slurm_sbatch = list(fallback = "sbatch", required = TRUE),
      slurm_squeue = list(fallback = "squeue", required = FALSE),
      slurm_sacct = list(fallback = "sacct", required = FALSE),
      slurm_scancel = list(fallback = "scancel", required = TRUE),
      slurm_sinfo = list(fallback = "sinfo", required = FALSE)))
  } else if (identical(type, "external")) {
    command_specs <- c(command_specs, list(
      external_submit_cmd = list(fallback = "", required = TRUE),
      external_status_cmd = list(fallback = "", required = TRUE),
      external_cancel_cmd = list(fallback = "", required = FALSE)))
  } else if (identical(type, "kubernetes")) {
    command_specs <- c(command_specs, list(
      kubernetes_kubectl = list(fallback = "kubectl", required = TRUE)))
  }
  for (name in names(command_specs)) {
    value <- settings[[name]] %||% ""
    command <- .dshpc_pin_configured_command(value,
      command_specs[[name]]$fallback, command_specs[[name]]$required)
    if (is.null(command)) config[[name]] <- NULL else
      config[[name]] <- command
  }
  .dshpc_normalize_unit_config(config, type)
}

.dshpc_site_default_snapshot <- function(label,
                                          settings = .dshpc_settings()) {
  label <- .dshpc_unit_scalar_string(label,
    pattern = "^[A-Za-z][A-Za-z0-9._-]{0,127}$", max_bytes = 128L)
  type <- .executor_backend_name(settings)
  if (!type %in% .DSHPC_RUNTIME_TYPES) .dshpc_unit_fail()
  config <- .dshpc_unit_config_from_settings(settings, type)
  resource_pool_id <- .dshpc_site_default_pool_id(settings)
  runners <- character(0)
  seal <- .dshpc_unit_seal("site_default", "site-default", resource_pool_id,
    type, config, label, runners)
  .dshpc_validate_unit_snapshot(list(
    schema_version = .DSHPC_UNIT_SCHEMA_VERSION,
    source = "site_default", unit_id = "site-default",
    resource_pool_id = resource_pool_id, type = type, config_seal = seal,
    config = config, allowed_labels = list(label),
    allowed_runners = list()))
}
