# Module: Step Constructors

#' dsHPC Step Constructors
#'
#' Internal builders for server-side packages composing dsHPC job specs.
#'
#' @param table,resource,expr DataSHIELD table/resource/expression payload.
#' @param symbol Server-side symbol name.
#' @param output_name Output name.
#' @param value Small value to emit server-side.
#' @param dataset_id Dataset identifier.
#' @param columns Optional columns for tabular staging.
#' @param format Staging format.
#' @param runner Allowlisted artifact runner name.
#' @param config Runner configuration list.
#' @param inputs Optional upstream step or DAG node inputs.
#' @param target_dataset Dataset receiving a published asset.
#' @param asset_name Asset name.
#' @param asset_type Asset kind.
#' @param publish_kind Publisher plugin kind.
#' @param title Dataset title.
#' @param modality Dataset modality.
#' @return A `dshpc_step`.
#' @name ds_steps
#' @keywords internal
NULL

#' @rdname ds_steps
ds_step_assign_table <- function(table, symbol) {
  .make_step("assign_table", plane = "session", table = table, symbol = symbol)
}

#' @rdname ds_steps
ds_step_assign_resource <- function(resource, symbol) {
  .make_step("assign_resource", plane = "session", resource = resource,
             symbol = symbol)
}

#' @rdname ds_steps
ds_step_assign_expr <- function(expr, symbol) {
  .make_step("assign_expr", plane = "session", expr = expr, symbol = symbol)
}

#' @rdname ds_steps
ds_step_aggregate <- function(expr) {
  .make_step("aggregate", plane = "session", expr = expr)
}

#' @rdname ds_steps
ds_step_emit <- function(output_name, value = NULL) {
  .make_step("emit", plane = "session", output_name = output_name, value = value)
}

#' @rdname ds_steps
ds_step_resolve_dataset <- function(dataset_id) {
  .make_step("resolve_dataset", plane = "session", dataset_id = dataset_id)
}

#' @rdname ds_steps
ds_step_stage_tabular <- function(resource, columns = NULL, format = "parquet") {
  .make_step("stage_tabular", plane = "artifact", runner = "stage_parquet",
             resource = resource, columns = columns, format = format)
}

#' @rdname ds_steps
ds_step_run_artifact <- function(runner, config = list(), inputs = NULL) {
  .make_step("run_artifact", plane = "artifact", runner = runner,
             config = config, inputs = inputs)
}

#' @rdname ds_steps
ds_step_publish_asset <- function(target_dataset, asset_name,
                                  asset_type = "derived",
                                  publish_kind = "generic") {
  .make_step("publish_asset", plane = "session", dataset_id = target_dataset,
             asset_name = asset_name, asset_type = asset_type,
             publish_kind = publish_kind)
}

#' @rdname ds_steps
ds_step_publish_dataset <- function(dataset_id, title, modality,
                                    publish_kind = "generic") {
  .make_step("publish_dataset", plane = "session", dataset_id = dataset_id,
             title = title, modality = modality, publish_kind = publish_kind)
}

#' @rdname ds_steps
ds_step_safe_summary <- function() {
  .make_step("safe_summary", plane = "session")
}

#' @keywords internal
.make_step <- function(type, plane = "session", ...) {
  step <- list(type = type, plane = plane, ...)
  class(step) <- c("dshpc_step", "list")
  step
}

#' @export
print.dshpc_step <- function(x, ...) {
  cat("dshpc_step\n  Type:", x$type, "\n  Plane:", x$plane, "\n")
  if (!is.null(x$runner)) cat("  Runner:", x$runner, "\n")
  invisible(x)
}
