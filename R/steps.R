# Module: Step Constructors
# All step constructors return a dsjobs_step S3 object.

# --- Session-plane steps ---

#' Create an assign-table step
#'
#' @param table Character; the table/symbol name.
#' @param symbol Character; symbol to assign to.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_assign_table <- function(table, symbol) {
  .make_step("assign_table", plane = "session",
             table = table, symbol = symbol)
}

#' Create an assign-resource step
#'
#' @param resource Character; the resource name.
#' @param symbol Character; symbol to assign to.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_assign_resource <- function(resource, symbol) {
  .make_step("assign_resource", plane = "session",
             resource = resource, symbol = symbol)
}

#' Create an assign-expression step
#'
#' @param expr Character; R expression to evaluate.
#' @param symbol Character; symbol to assign to.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_assign_expr <- function(expr, symbol) {
  .make_step("assign_expr", plane = "session",
             expr = expr, symbol = symbol)
}

#' Create an aggregate step
#'
#' @param expr Character; R expression to aggregate.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_aggregate <- function(expr) {
  .make_step("aggregate", plane = "session", expr = expr)
}

#' Create a checkpoint step
#'
#' @param workspace_name Character; name for the checkpoint.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_checkpoint <- function(workspace_name = "default") {
  .make_step("checkpoint", plane = "session",
             workspace_name = workspace_name)
}

#' Create a restore-checkpoint step
#'
#' @param workspace_name Character; name of checkpoint to restore.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_restore_checkpoint <- function(workspace_name = "default") {
  .make_step("restore_checkpoint", plane = "session",
             workspace_name = workspace_name)
}

#' Create an emit step (output a value)
#'
#' @param output_name Character; name for the output.
#' @param value Any R object to emit.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_emit <- function(output_name, value = NULL) {
  .make_step("emit", plane = "session",
             output_name = output_name, value = value)
}

# --- Artifact-plane steps ---

#' Create a resolve-dataset step
#'
#' @param dataset_id Character; the dataset identifier.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_resolve_dataset <- function(dataset_id) {
  .make_step("resolve_dataset", plane = "session",
             dataset_id = dataset_id)
}

#' Create a stage-tabular step
#'
#' @param resource Character; resource name or symbol.
#' @param columns Character vector; columns to select.
#' @param format Character; output format ("parquet" or "csv").
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_stage_tabular <- function(resource, columns = NULL,
                                   format = "parquet") {
  .make_step("stage_tabular", plane = "artifact",
             runner = "stage_parquet",
             resource = resource, columns = columns, format = format)
}

#' Create a run-artifact step
#'
#' @param runner Character; name of the runner (e.g. "pyradiomics").
#' @param config Named list; runner-specific configuration.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_run_artifact <- function(runner, config = list()) {
  .make_step("run_artifact", plane = "artifact",
             runner = runner, config = config)
}

#' Create a publish-asset step
#'
#' @param target_dataset Character; target dataset identifier.
#' @param asset_name Character; name for the published asset.
#' @param asset_type Character; type of asset.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_publish_asset <- function(target_dataset, asset_name,
                                   asset_type = "derived") {
  .make_step("publish_asset", plane = "session",
             dataset_id = target_dataset, asset_name = asset_name,
             asset_type = asset_type)
}

#' Create a publish-dataset step
#'
#' @param dataset_id Character; new dataset identifier.
#' @param title Character; human-readable title.
#' @param modality Character; imaging modality.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_publish_dataset <- function(dataset_id, title, modality) {
  .make_step("publish_dataset", plane = "session",
             dataset_id = dataset_id, title = title, modality = modality)
}

#' Create a safe-summary step
#'
#' @param output Named list or NULL; output to summarize.
#' @return A \code{dsjobs_step} object.
#' @export
ds_step_safe_summary <- function(output = NULL) {
  .make_step("safe_summary", plane = "session", output = output)
}

# --- Internal ---

#' Construct a dsjobs_step S3 object
#' @keywords internal
.make_step <- function(type, plane = "session", ...) {
  step <- list(type = type, plane = plane, ...)
  class(step) <- c("dsjobs_step", "list")
  step
}

#' Print a dsjobs_step
#' @param x A dsjobs_step object.
#' @param ... Additional arguments (ignored).
#' @export
print.dsjobs_step <- function(x, ...) {
  cat("dsjobs_step\n")
  cat("  Type: ", x$type, "\n")
  cat("  Plane:", x$plane, "\n")
  if (!is.null(x$runner)) cat("  Runner:", x$runner, "\n")
  if (!is.null(x$dataset_id)) cat("  Dataset:", x$dataset_id, "\n")
  invisible(x)
}
