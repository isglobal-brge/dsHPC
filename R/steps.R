# Module: Step Constructors
# Generic steps only. Domain-specific DSLs live in upstream packages.

# --- Session plane ---

#' @export
ds_step_assign_table <- function(table, symbol) {
  .make_step("assign_table", plane = "session", table = table, symbol = symbol)
}

#' @export
ds_step_assign_resource <- function(resource, symbol) {
  .make_step("assign_resource", plane = "session", resource = resource, symbol = symbol)
}

#' @export
ds_step_assign_expr <- function(expr, symbol) {
  .make_step("assign_expr", plane = "session", expr = expr, symbol = symbol)
}

#' @export
ds_step_aggregate <- function(expr) {
  .make_step("aggregate", plane = "session", expr = expr)
}

#' @export
ds_step_emit <- function(output_name, value = NULL) {
  .make_step("emit", plane = "session", output_name = output_name, value = value)
}

# --- Artifact plane ---

#' @export
ds_step_resolve_dataset <- function(dataset_id) {
  .make_step("resolve_dataset", plane = "session", dataset_id = dataset_id)
}

#' @export
ds_step_stage_tabular <- function(resource, columns = NULL, format = "parquet") {
  .make_step("stage_tabular", plane = "artifact", runner = "stage_parquet",
             resource = resource, columns = columns, format = format)
}

#' @export
ds_step_run_artifact <- function(runner, config = list(), input_from = NULL) {
  .make_step("run_artifact", plane = "artifact", runner = runner,
             config = config, input_from = input_from)
}

#' @export
ds_step_publish_asset <- function(target_dataset, asset_name, asset_type = "derived") {
  .make_step("publish_asset", plane = "session",
             dataset_id = target_dataset, asset_name = asset_name,
             asset_type = asset_type)
}

#' @export
ds_step_publish_dataset <- function(dataset_id, title, modality) {
  .make_step("publish_dataset", plane = "session",
             dataset_id = dataset_id, title = title, modality = modality)
}

#' @export
ds_step_safe_summary <- function() {
  .make_step("safe_summary", plane = "session")
}

# --- Internal ---

#' @keywords internal
.make_step <- function(type, plane = "session", ...) {
  step <- list(type = type, plane = plane, ...)
  class(step) <- c("dsjobs_step", "list")
  step
}

#' @export
print.dsjobs_step <- function(x, ...) {
  cat("dsjobs_step\n")
  cat("  Type: ", x$type, "\n")
  cat("  Plane:", x$plane, "\n")
  if (!is.null(x$runner)) cat("  Runner:", x$runner, "\n")
  if (!is.null(x$dataset_id)) cat("  Dataset:", x$dataset_id, "\n")
  invisible(x)
}
