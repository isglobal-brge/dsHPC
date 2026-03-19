# Module: Radiomics DSL
# Sugar for composing radiomics extraction jobs.

#' Extract radiomics features from a dataset
#'
#' Composes: resolve_dataset + pyradiomics runner + safe_summary +
#' optionally publish_asset.
#'
#' @param conns DSI connections object.
#' @param dataset_id Character; the dataset identifier.
#' @param settings Named list; pyradiomics settings (optional).
#' @param publish Logical or named list; if TRUE, publishes as
#'   \code{<dataset_id>.radiomics}. If a named list, uses
#'   \code{asset_name} and \code{asset_type} fields.
#' @param symbol Character; job handle symbol (default auto-generated).
#' @return A \code{dsjobs_result} with submission status.
#' @export
ds.radiomics.extract <- function(conns, dataset_id, settings = list(),
                                  publish = FALSE, symbol = NULL) {
  steps <- list(
    ds_step_resolve_dataset(dataset_id),
    ds_step_run_artifact(
      runner = "pyradiomics",
      config = settings
    ),
    ds_step_safe_summary()
  )

  # Add publish step if requested
  pub_config <- NULL
  if (isTRUE(publish)) {
    steps <- c(steps, list(
      ds_step_publish_asset(dataset_id, "radiomics", "radiomics")
    ))
    pub_config <- list(
      dataset_id = dataset_id,
      asset_name = "radiomics",
      asset_type = "radiomics"
    )
  } else if (is.list(publish)) {
    asset_name <- publish$asset_name %||% "radiomics"
    asset_type <- publish$asset_type %||% "radiomics"
    steps <- c(steps, list(
      ds_step_publish_asset(dataset_id, asset_name, asset_type)
    ))
    pub_config <- list(
      dataset_id = dataset_id,
      asset_name = asset_name,
      asset_type = asset_type
    )
  }

  job <- ds_job(
    steps = steps,
    publish = pub_config,
    resource_class = "cpu_heavy"
  )

  ds.jobs.submit(conns, job, symbol = symbol)
}
