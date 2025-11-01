# Internal helper: NULL-coalescing operator
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

#' Submit a meta-job with chained processing steps
#'
#' @param config API configuration created by create_api_config
#' @param content Initial content to process (raw vector, character, or other object)
#' @param method_chain List of method specifications, each with method_name and parameters
#'
#' @return A list with meta-job information including meta_job_hash and status
#' @export
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
  
  # Call the _by_hash version
  return(submit_meta_job_by_hash(config, file_hash, method_chain))
}

#' Get the status of a meta-job
#'
#' @param config API configuration created by create_api_config
#' @param meta_job_hash ID of the meta-job to check
#'
#' @return A list with detailed meta-job status including chain progress
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' status <- get_meta_job_status(config, "meta-job-uuid-here")
#' }
get_meta_job_status <- function(config, meta_job_hash) {
  if (!is.character(meta_job_hash) || length(meta_job_hash) != 1) {
    stop("meta_job_hash must be a single character string")
  }
  
  # Make API call
  response <- api_get(config, paste0("/meta-job/", meta_job_hash))
  
  return(response)
}

#' Wait for a meta-job to complete and return results
#'
#' @param config API configuration created by create_api_config
#' @param meta_job_hash ID of the meta-job to wait for
#' @param timeout Maximum time to wait in seconds (default: NA for no timeout)
#' @param interval Polling interval in seconds (default: 5)
#' @param parse_json Whether to parse the final output as JSON (default: TRUE)
#'
#' @return The final output of the meta-job chain if completed within timeout
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' results <- wait_for_meta_job_results(config, meta_job_hash, timeout = 900)
#' }
wait_for_meta_job_results <- function(config, meta_job_hash, timeout = NA, interval = 5, parse_json = TRUE) {
  if (!is.character(meta_job_hash) || length(meta_job_hash) != 1) {
    stop("meta_job_hash must be a single character string")
  }
  
  # Ensure interval is not too small
  interval <- max(interval, 1)
  
  # Start timer
  start_time <- Sys.time()
  
  # Track last displayed step
  last_step_displayed <- NULL
  step_start_time <- Sys.time()
  
  # Poll for completion
  while(TRUE) {
    # Check for timeout (only if specified)
    if (!is.na(timeout)) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
      if (elapsed > timeout) {
        stop(paste0("Timeout waiting for meta-job to complete after ", timeout, " seconds. Current status: ", 
                    ifelse(is.null(status_info$status), "unknown", status_info$status)))
      }
    }
    
    # Get current status
    status_info <- get_meta_job_status(config, meta_job_hash)
    
    # Display current step info during processing (if available)
    if (!is.null(status_info$current_step_info)) {
      step_info <- status_info$current_step_info
      
      # Get total steps - chain might be a dataframe or list
      total_steps <- if (is.data.frame(status_info$chain)) {
        nrow(status_info$chain)
      } else {
        length(status_info$chain)
      }
      
      # Create unique identifier for this step state
      step_id <- paste0(step_info$step_number, "_", step_info$job_hash, "_", step_info$job_status)
      
      timestamp <- format(Sys.time(), "%H:%M:%S")
      
      if (is.null(last_step_displayed) || last_step_displayed != step_id) {
        # Step/status changed - show full info
        status_msg <- sprintf("[%s] Step %d/%d: %s [%s]%s",
                             timestamp,
                             step_info$step_number,
                             total_steps,
                             step_info$method_name,
                             step_info$status_description,
                             if(step_info$is_resubmitted) " (resubmitted)" else "")
        
        cat(status_msg, "\n")
        last_step_displayed <- step_id
        step_start_time <- Sys.time()
      } else {
        # Same step/status - show heartbeat with elapsed time
        step_elapsed <- as.numeric(difftime(Sys.time(), step_start_time, units = "secs"))
        status_msg <- sprintf("[%s] Step %d/%d: %s [%s] (%.0fs)",
                             timestamp,
                             step_info$step_number,
                             total_steps,
                             step_info$method_name,
                             step_info$status_description,
                             step_elapsed)
        
        cat(status_msg, "\n")
      }
    }
    
    # Check if completed
    if (!is.null(status_info$status) && status_info$status == "completed") {
      # Add newline after progress display
      cat("\n")
      
      # The server includes the final output directly in the meta-job response
      # No need to make a separate query - the chain is handled entirely server-side
      output <- status_info$final_output
      
      if (is.null(output)) {
        stop("Meta-job completed but no final_output provided by server")
      }
      
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
      # Add newline after progress display
      cat("\n")
      
      # Enhanced error message with step information
      error_msg <- "Meta-job failed"
      
      if (!is.null(status_info$current_step_info)) {
        step_info <- status_info$current_step_info
        error_msg <- sprintf("Meta-job failed at step %d (%s): %s",
                            step_info$step_number,
                            step_info$method_name,
                            status_info$error %||% "Unknown error")
        
        # Add job details if available
        if (!is.null(step_info$job_status)) {
          error_msg <- paste0(error_msg, 
                             sprintf("\n  Job status: %s - %s",
                                    step_info$job_status,
                                    step_info$status_description))
        }
        if (!is.null(step_info$job_hash)) {
          error_msg <- paste0(error_msg, sprintf("\n  Job Hash: %s", step_info$job_hash))
        }
      } else {
        error_msg <- paste0(error_msg, ": ", status_info$error %||% "Unknown error")
      }
      
      stop(error_msg)
    }
    
    # Wait before polling again
    Sys.sleep(interval)
  }
}

#' Wait for a meta-job to complete (alias for wait_for_meta_job_results)
#'
#' @param config API configuration
#' @param meta_job_hash Meta-job hash
#' @param max_wait Maximum time to wait in seconds (default: NA for no timeout)
#' @param interval Polling interval in seconds (default: 5)
#' @param parse_json Whether to parse the final output as JSON (default: TRUE)
#'
#' @return Complete meta-job information including final output
#' @export
wait_for_meta_job <- function(config, meta_job_hash, max_wait = NA, interval = 5, parse_json = TRUE) {
  # Get meta-job info
  info <- wait_for_meta_job_results(config, meta_job_hash, timeout = max_wait, interval = interval, parse_json = parse_json)
  
  # Return full status object for consistency with wait_for_pipeline
  status <- get_meta_job_status(config, meta_job_hash)
  status$final_output <- info  # Replace with parsed output
  
  return(status)
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
#' @param timeout Maximum time to wait in seconds (default: NA for no timeout)
#' @param interval Polling interval in seconds (default: 5)
#' @param parse_json Whether to parse the final output as JSON (default: TRUE)
#'
#' @return The final output of the processing chain
#' @export
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
                                   timeout = NA, interval = 5, parse_json = TRUE) {
  # Upload the content first - this returns the hash of the uploaded content
  message("Uploading content...")
  file_hash <- upload_file(config, content, upload_filename)
  
  # The hash returned is the hash of the complete content (before chunking)
  # - For files: hash of the complete file
  # - For objects: hash of the complete serialized object
  
  # Call the _by_hash version with the hash from upload
  return(execute_processing_chain_by_hash(config, file_hash, method_chain, 
                                          timeout, interval, parse_json))
}

#' Get cached steps from a meta-job
#'
#' Returns information about which steps in a meta-job used cached results
#'
#' @param config API configuration created by create_api_config
#' @param meta_job_hash ID of the meta-job to check
#'
#' @return A data frame with step information including cache usage
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' cache_info <- get_meta_job_cache_info(config, "meta-job-uuid-here")
#' }
get_meta_job_cache_info <- function(config, meta_job_hash) {
  # Get full status
  status_info <- get_meta_job_status(config, meta_job_hash)
  
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
      job_hash = ifelse(is.null(step$job_hash), NA, step$job_hash),
      stringsAsFactors = FALSE
    )
  }))
  
  return(cache_info)
}

#' Submit a meta-job with chained processing steps by hash
#'
#' @param config API configuration created by create_api_config
#' @param file_hash Hash of the initial content (as returned by hash_content)
#' @param method_chain List of method specifications, each with method_name and parameters
#'
#' @return A list with meta-job information including meta_job_hash and status
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' file_hash <- "abc123..."
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
#' meta_job <- submit_meta_job_by_hash(config, file_hash, chain)
#' }
submit_meta_job_by_hash <- function(config, file_hash = NULL, method_chain) {
  # Validate file_hash (can be NULL for params-only jobs)
  if (!is.null(file_hash) && (!is.character(file_hash) || length(file_hash) != 1)) {
    stop("file_hash must be NULL or a single character string")
  }
  
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
    method_chain = processed_chain
  )
  
  # Only include initial_file_hash if provided (not NULL)
  if (!is.null(file_hash)) {
    body$initial_file_hash <- file_hash
  }
  
  # Make API call
  response <- api_post(config, "/submit-meta-job", body = body)
  
  return(response)
}

#' Execute a processing chain and wait for results by hash
#'
#' This is a convenience function that submits a meta-job using a pre-computed hash
#' and waits for the results in one call. Use this when you've already uploaded
#' the content and have its hash, or pass NULL for params-only jobs.
#'
#' @param config API configuration created by create_api_config
#' @param file_hash Hash of the initial content (as returned by hash_content), or NULL for params-only jobs
#' @param method_chain List of method specifications, each with method_name and parameters
#' @param timeout Maximum time to wait in seconds (default: 600)
#' @param interval Polling interval in seconds (default: 5)
#' @param parse_json Whether to parse the final output as JSON (default: TRUE)
#'
#' @return The final output of the processing chain
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' 
#' # Upload content and get hash
#' content <- readRDS("lung_image.rds")
#' file_hash <- hash_content(content)
#' upload_file(config, content, "lung_image.rds")
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
#' # Execute using hash directly
#' results <- execute_processing_chain_by_hash(config, file_hash, chain)
#' }
execute_processing_chain_by_hash <- function(config, file_hash = NULL, method_chain, 
                                            timeout = NA, interval = 5, parse_json = TRUE) {
  # Validate file_hash (can be NULL for params-only jobs)
  if (!is.null(file_hash) && (!is.character(file_hash) || length(file_hash) != 1)) {
    stop("file_hash must be NULL or a single character string")
  }
  
  # Submit meta-job
  message("Submitting meta-job with ", length(method_chain), " steps...")
  meta_job_response <- submit_meta_job_by_hash(config, file_hash, method_chain)
  
  if (is.null(meta_job_response$meta_job_hash)) {
    stop("Failed to submit meta-job: ", 
         ifelse(is.null(meta_job_response$message), "unknown error", meta_job_response$message))
  }
  
  meta_job_hash <- meta_job_response$meta_job_hash
  message("Meta-job submitted with ID: ", meta_job_hash)
  
  # Wait for results
  message("Waiting for processing to complete...")
  results <- wait_for_meta_job_results(config, meta_job_hash, timeout = timeout, 
                                      interval = interval, parse_json = parse_json)
  
  message("Processing complete!")
  return(results)
}
