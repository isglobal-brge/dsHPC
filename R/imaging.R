# Module: Imaging Preprocessing DSL
# Sugar for composing imaging preprocessing jobs.

#' Preprocess an imaging dataset
#'
#' Composes: resolve_dataset + image_preprocess runner + safe_summary +
#' optionally publish_dataset.
#'
#' @param conns DSI connections object.
#' @param dataset_id Character; source dataset identifier.
#' @param operations Named list; preprocessing operations
#'   (e.g. resize, normalize, augment).
#' @param publish Logical or named list; if TRUE, publishes as a new dataset.
#'   If a named list, uses \code{dataset_id}, \code{title}, \code{modality}.
#' @param symbol Character; job handle symbol (default auto-generated).
#' @return A \code{dsjobs_result} with submission status.
#' @export
ds.imaging.preprocess <- function(conns, dataset_id, operations = list(),
                                   publish = FALSE, symbol = NULL) {
  steps <- list(
    ds_step_resolve_dataset(dataset_id),
    ds_step_run_artifact(
      runner = "image_preprocess",
      config = operations
    ),
    ds_step_safe_summary()
  )

  # Add publish step if requested
  pub_config <- NULL
  if (isTRUE(publish)) {
    new_id <- paste0(dataset_id, ".preprocessed")
    steps <- c(steps, list(
      ds_step_publish_dataset(new_id, paste("Preprocessed", dataset_id),
                               "preprocessed")
    ))
    pub_config <- list(
      dataset_id = new_id,
      asset_name = "preprocessed",
      asset_type = "preprocessed"
    )
  } else if (is.list(publish)) {
    new_id <- publish$dataset_id %||% paste0(dataset_id, ".preprocessed")
    title <- publish$title %||% paste("Preprocessed", dataset_id)
    modality <- publish$modality %||% "preprocessed"
    steps <- c(steps, list(
      ds_step_publish_dataset(new_id, title, modality)
    ))
    pub_config <- list(
      dataset_id = new_id,
      asset_name = "preprocessed",
      asset_type = modality
    )
  }

  job <- ds_job(
    steps = steps,
    publish = pub_config,
    resource_class = "cpu_heavy"
  )

  ds.jobs.submit(conns, job, symbol = symbol)
}
