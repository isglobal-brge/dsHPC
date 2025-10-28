#' Submit a meta-job with multiple input files
#'
#' @param config API configuration
#' @param file_inputs Named list of file hashes (e.g., list(primary="hash1", reference="hash2"))
#' @param method_chain List of method specifications
#'
#' @return Response from API with meta_job_id
#' @export
submit_meta_job_multi <- function(config, file_inputs, method_chain) {
  if (!is.list(file_inputs) || is.null(names(file_inputs))) {
    stop("file_inputs must be a named list")
  }
  
  # Sort file_inputs by name for consistency
  sorted_inputs <- file_inputs[order(names(file_inputs))]
  
  body <- list(
    initial_file_inputs = sorted_inputs,
    method_chain = method_chain
  )
  
  response <- api_post(config, "/submit-meta-job", body = body)
  return(response)
}

#' Execute a processing chain with multiple input files
#'
#' @param config API configuration
#' @param file_inputs Named list of file hashes
#' @param method_chain List of method specifications
#' @param timeout Maximum time to wait (default: NA)
#' @param interval Polling interval (default: 5)
#' @param parse_json Whether to parse output as JSON (default: TRUE)
#'
#' @return Final output of the chain
#' @export
execute_processing_chain_multi <- function(config, file_inputs, method_chain,
                                          timeout = NA, interval = 5, parse_json = TRUE) {
  # Validate all files exist
  for (name in names(file_inputs)) {
    hash <- file_inputs[[name]]
    if (!hash_exists(config, hash)) {
      stop(sprintf("File '%s' with hash %s not found in database", name, hash))
    }
  }
  
  # Submit meta-job
  meta_response <- submit_meta_job_multi(config, file_inputs, method_chain)
  meta_job_id <- meta_response$meta_job_id
  
  # Wait for results
  result <- wait_for_meta_job_results(config, meta_job_id, timeout, interval, parse_json)
  
  return(result)
}

