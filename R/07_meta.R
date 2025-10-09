#' Submit a meta-job with chained processing steps
#'
#' @param config API configuration created by create_api_config
#' @param content Initial content to process (raw vector, character, or other object)
#' @param method_chain List of method specifications, each with method_name and parameters
#'
#' @return A list with meta-job information including meta_job_id and status
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' content <- readRDS("lung_image.rds")
#' 
#' # Define processing chain
#' chain <- list(
#'   list(
#'     method_name = "lung_mask",
#'     parameters = list()
#'   ),
#'   list(
#'     method_name = "extract_radiomics",
#'     parameters = list(feature_set = "all")
#'   )
#' )
#' 
#' meta_job <- submit_meta_job(config, content, chain)
#' }
submit_meta_job <- function(config, content, method_chain) {
  # Calculate content hash
  file_hash <- hash_content(content)
  
  # Validate method_chain
  if (!is.list(method_chain) || length(method_chain) == 0) {
    stop("method_chain must be a non-empty list")
  }
  
  # Process and validate each step in the chain
  processed_chain <- list()
  for (i in seq_along(method_chain)) {
    step <- method_chain[[i]]
    
    if (!is.list(step)) {
      stop(paste0("Step ", i, " in method_chain must be a list"))
    }
    
    if (is.null(step$method_name) || !is.character(step$method_name)) {
      stop(paste0("Step ", i, " must have a 'method_name' character field"))
    }
    
    # Ensure parameters is a list (default to empty list if not provided)
    if (is.null(step$parameters)) {
      step$parameters <- list()
    } else if (!is.list(step$parameters)) {
      stop(paste0("Step ", i, " parameters must be a list"))
    }
    
    # Sort parameters for consistency
    sorted_params <- sort_parameters(step$parameters)
    
    processed_chain[[i]] <- list(
      method_name = step$method_name,
      parameters = sorted_params
    )
  }
  
  # Create request body
  body <- list(
    initial_file_hash = file_hash,
    method_chain = processed_chain
  )
  
  # Make API call
  response <- api_post(config, "/submit-meta-job", body = body)
  
  return(response)
}

#' Get the status of a meta-job
#'
#' @param config API configuration created by create_api_config
#' @param meta_job_id ID of the meta-job to check
#'
#' @return A list with detailed meta-job status including chain progress
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' status <- get_meta_job_status(config, "meta-job-uuid-here")
#' }
get_meta_job_status <- function(config, meta_job_id) {
  if (!is.character(meta_job_id) || length(meta_job_id) != 1) {
    stop("meta_job_id must be a single character string")
  }
  
  # Make API call
  response <- api_get(config, paste0("/meta-job/", meta_job_id))
  
  return(response)
}

#' Wait for a meta-job to complete and return results
#'
#' @param config API configuration created by create_api_config
#' @param meta_job_id ID of the meta-job to wait for
#' @param timeout Maximum time to wait in seconds (default: 600)
#' @param interval Polling interval in seconds (default: 5)
#' @param parse_json Whether to parse the final output as JSON (default: TRUE)
#'
#' @return The final output of the meta-job chain if completed within timeout
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' results <- wait_for_meta_job_results(config, meta_job_id, timeout = 900)
#' }
wait_for_meta_job_results <- function(config, meta_job_id, timeout = 600, interval = 5, parse_json = TRUE) {
  if (!is.character(meta_job_id) || length(meta_job_id) != 1) {
    stop("meta_job_id must be a single character string")
  }
  
  # Ensure interval is not too small
  interval <- max(interval, 1)
  
  # Start timer
  start_time <- Sys.time()
  
  # Poll for completion
  while(TRUE) {
    # Check for timeout
    elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
    if (elapsed > timeout) {
      stop(paste0("Timeout waiting for meta-job to complete. Current status: ", 
                  ifelse(is.null(status_info$status), "unknown", status_info$status)))
    }
    
    # Get current status
    status_info <- get_meta_job_status(config, meta_job_id)
    
    # Check if completed
    if (!is.null(status_info$status) && status_info$status == "completed") {
      # Get the final output
      output <- status_info$final_output
      
      # Parse JSON if requested
      if (parse_json && !is.null(output) && output != "") {
        tryCatch({
          output <- jsonlite::fromJSON(output)
        }, error = function(e) {
          warning("Failed to parse output as JSON: ", e$message)
          # Return the raw output instead
        })
      }
      
      return(output)
    }
    
    # Check for error state
    if (!is.null(status_info$status) && status_info$status == "failed") {
      stop(paste0("Meta-job failed with error: ", 
                  ifelse(is.null(status_info$error), "unknown error", status_info$error)))
    }
    
    # Wait before polling again
    Sys.sleep(interval)
  }
}

#' Execute a processing chain and wait for results
#'
#' This is a convenience function that uploads content, submits a meta-job,
#' and waits for the results in one call.
#'
#' @param config API configuration created by create_api_config
#' @param content Content to process (raw vector, character, or other object)
#' @param method_chain List of method specifications, each with method_name and parameters
#' @param upload_filename Name to use when uploading the content (default: "input_data")
#' @param timeout Maximum time to wait in seconds (default: 600)
#' @param interval Polling interval in seconds (default: 5)
#' @param parse_json Whether to parse the final output as JSON (default: TRUE)
#'
#' @return The final output of the processing chain
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' content <- readRDS("lung_image.rds")
#' 
#' # Define processing chain
#' chain <- list(
#'   list(
#'     method_name = "lung_mask",
#'     parameters = list()
#'   ),
#'   list(
#'     method_name = "extract_radiomics",
#'     parameters = list(feature_set = "all")
#'   )
#' )
#' 
#' results <- execute_processing_chain(config, content, chain)
#' }
execute_processing_chain <- function(config, content, method_chain, 
                                   upload_filename = "input_data",
                                   timeout = 600, interval = 5, parse_json = TRUE) {
  # Upload the content first
  message("Uploading content...")
  upload_success <- upload_file(config, content, upload_filename)
  
  if (!upload_success) {
    stop("Failed to upload content")
  }
  
  # Submit meta-job
  message("Submitting meta-job with ", length(method_chain), " steps...")
  meta_job_response <- submit_meta_job(config, content, method_chain)
  
  if (is.null(meta_job_response$meta_job_id)) {
    stop("Failed to submit meta-job: ", 
         ifelse(is.null(meta_job_response$message), "unknown error", meta_job_response$message))
  }
  
  meta_job_id <- meta_job_response$meta_job_id
  message("Meta-job submitted with ID: ", meta_job_id)
  
  # Wait for results
  message("Waiting for processing to complete...")
  results <- wait_for_meta_job_results(config, meta_job_id, timeout = timeout, 
                                      interval = interval, parse_json = parse_json)
  
  message("Processing complete!")
  return(results)
}

#' Get cached steps from a meta-job
#'
#' Returns information about which steps in a meta-job used cached results
#'
#' @param config API configuration created by create_api_config
#' @param meta_job_id ID of the meta-job to check
#'
#' @return A data frame with step information including cache usage
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' cache_info <- get_meta_job_cache_info(config, "meta-job-uuid-here")
#' }
get_meta_job_cache_info <- function(config, meta_job_id) {
  # Get full status
  status_info <- get_meta_job_status(config, meta_job_id)
  
  if (is.null(status_info$chain)) {
    return(data.frame())
  }
  
  # Extract cache information from chain
  cache_info <- do.call(rbind, lapply(status_info$chain, function(step) {
    data.frame(
      step = step$step,
      method_name = step$method_name,
      cached = ifelse(is.null(step$cached), FALSE, step$cached),
      status = ifelse(is.null(step$status), "pending", step$status),
      job_id = ifelse(is.null(step$job_id), NA, step$job_id),
      stringsAsFactors = FALSE
    )
  }))
  
  return(cache_info)
}
