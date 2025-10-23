# Internal helper: NULL-coalescing operator
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

#' Query a job execution
#'
#' @param config API configuration created by create_api_config
#' @param content Content to process (raw vector, character, or other object)
#' @param method_name Name of the method to execute
#' @param parameters Named list of parameters for the method
#' @param validate_parameters Whether to validate parameters against method specification (default: TRUE)
#'
#' @return A list with job information including status, output, and error details
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' content <- "Hello, World!"
#' job <- query_job(config, content, "analyze_data", list(parameter1 = "value1"))
#' }
query_job <- function(config, content, method_name, parameters = list(), validate_parameters = TRUE) {
  # Calculate content hash
  file_hash <- hash_content(content)
  
  # Call the _by_hash version
  return(query_job_by_hash(config, file_hash, method_name, parameters, validate_parameters))
}

#' Get the status of a job
#'
#' @param config API configuration created by create_api_config
#' @param file_path Path to the file processed
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#'
#' @return The status of the job as a string
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' status <- get_job_status(config, "image.jpg", "count_black_pixels", list(threshold = 30))
#' }
get_job_status <- function(config, file_path, method_name, parameters = list()) {
  # Calculate hash and call _by_hash version
  file_hash <- hash_content(file_path)
  return(get_job_status_by_hash(config, file_hash, method_name, parameters))
}

#' Check if a job succeeded
#'
#' @param config API configuration created by create_api_config
#' @param file_path Path to the file processed
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#'
#' @return TRUE if the job completed successfully, FALSE otherwise
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' if (job_succeeded(config, "image.jpg", "count_black_pixels", list(threshold = 30))) {
#'   print("Job completed successfully")
#' }
#' }
job_succeeded <- function(config, file_path, method_name, parameters = list()) {
  # Calculate hash and call _by_hash version
  file_hash <- hash_content(file_path)
  return(job_succeeded_by_hash(config, file_hash, method_name, parameters))
}

#' Get the output of a completed job
#'
#' @param config API configuration created by create_api_config
#' @param file_path Path to the file processed
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#' @param parse_json Whether to parse the output as JSON (default: TRUE)
#'
#' @return The job output, parsed as JSON if requested
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' output <- get_job_output(config, "image.jpg", "count_black_pixels", list(threshold = 30))
#' }
get_job_output <- function(config, file_path, method_name, parameters = list(), parse_json = TRUE) {
  # Calculate hash and call _by_hash version
  file_hash <- hash_content(file_path)
  return(get_job_output_by_hash(config, file_hash, method_name, parameters, parse_json))
}

#' Wait for a job to complete and return results
#'
#' @param config API configuration created by create_api_config
#' @param content Content to process (raw vector, character, or other object)
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#' @param timeout Maximum time to wait in seconds (default: NA for no timeout)
#' @param interval Polling interval in seconds (default: 5)
#' @param parse_json Whether to parse the output as JSON (default: TRUE)
#' @param validate_parameters Whether to validate parameters against method specification (default: TRUE)
#'
#' @return The job output if completed within timeout, otherwise throws an error
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' content <- "Hello, World!"
#' results <- wait_for_job_results(config, content, "analyze_text", list(parameter1 = "value1"))
#' }
wait_for_job_results <- function(config, content, method_name, parameters = list(), 
                                 timeout = NA, interval = 5, parse_json = TRUE,
                                 validate_parameters = TRUE) {
  # Calculate content hash once for efficiency
  file_hash <- hash_content(content)
  
  # Call the _by_hash version
  return(wait_for_job_results_by_hash(config, file_hash, method_name, parameters, 
                                      timeout, interval, parse_json, validate_parameters))
}

#' Query a job execution by hash
#'
#' @param config API configuration created by create_api_config
#' @param file_hash Hash of the content (as returned by hash_content)
#' @param method_name Name of the method to execute
#' @param parameters Named list of parameters for the method
#' @param validate_parameters Whether to validate parameters against method specification (default: TRUE)
#'
#' @return A list with job information including status, output, and error details
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' file_hash <- "abc123..."
#' job <- query_job_by_hash(config, file_hash, "analyze_data", list(parameter1 = "value1"))
#' }
query_job_by_hash <- function(config, file_hash, method_name, parameters = list(), validate_parameters = TRUE) {
  # Validate the inputs
  if (!is.character(file_hash) || length(file_hash) != 1) {
    stop("file_hash must be a single character string")
  }
  
  if (!is.character(method_name) || length(method_name) != 1) {
    stop("method_name must be a single character string")
  }
  
  if (!is.list(parameters)) {
    stop("parameters must be a named list")
  }
  
  # Sort parameters alphabetically to ensure consistent ordering
  sorted_params <- sort_parameters(parameters)
  
  # Validate parameters against method specification if requested
  if (validate_parameters) {
    # Get all available methods
    methods <- get_methods(config)
    
    # Validate parameters against method specification (use original parameters for validation)
    validate_method_parameters(method_name, parameters, methods)
  }
  
  # Create request body with sorted parameters
  body <- list(
    file_hash = file_hash,
    method_name = method_name,
    parameters = sorted_params
  )
  
  # Make API call
  response <- api_post(config, "/query-job", body = body)
  
  return(response)
}

#' Get the status of a job by hash
#'
#' @param config API configuration created by create_api_config
#' @param file_hash Hash of the content (as returned by hash_content)
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#'
#' @return The status of the job as a string
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' file_hash <- "abc123..."
#' status <- get_job_status_by_hash(config, file_hash, "count_black_pixels", list(threshold = 30))
#' }
get_job_status_by_hash <- function(config, file_hash, method_name, parameters = list()) {
  # Query the job
  job_info <- query_job_by_hash(config, file_hash, method_name, parameters, validate_parameters = FALSE)
  
  # Return just the status
  return(job_info$status)
}

#' Check if a job succeeded by hash
#'
#' @param config API configuration created by create_api_config
#' @param file_hash Hash of the content (as returned by hash_content)
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#'
#' @return TRUE if the job completed successfully, FALSE otherwise
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' file_hash <- "abc123..."
#' if (job_succeeded_by_hash(config, file_hash, "count_black_pixels", list(threshold = 30))) {
#'   print("Job completed successfully")
#' }
#' }
job_succeeded_by_hash <- function(config, file_hash, method_name, parameters = list()) {
  # Query the job
  job_info <- query_job_by_hash(config, file_hash, method_name, parameters, validate_parameters = FALSE)
  
  # Check if status is "CD" (completed)
  return(!is.null(job_info$status) && job_info$status == "CD")
}

#' Get the output of a completed job by hash
#'
#' @param config API configuration created by create_api_config
#' @param file_hash Hash of the content (as returned by hash_content)
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#' @param parse_json Whether to parse the output as JSON (default: TRUE)
#'
#' @return The job output, parsed as JSON if requested
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' file_hash <- "abc123..."
#' output <- get_job_output_by_hash(config, file_hash, "count_black_pixels", list(threshold = 30))
#' }
get_job_output_by_hash <- function(config, file_hash, method_name, parameters = list(), parse_json = TRUE) {
  # Query the job
  job_info <- query_job_by_hash(config, file_hash, method_name, parameters, validate_parameters = FALSE)
  
  # Check if job is completed
  if (is.null(job_info$status) || job_info$status != "CD") {
    stop(paste0("Cannot get output: job is not completed. Current status: ", 
                ifelse(is.null(job_info$status), "unknown", job_info$status)))
  }
  
  # Get the output
  output <- job_info$output
  
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

#' Wait for a job to complete and return results by hash
#'
#' @param config API configuration created by create_api_config
#' @param file_hash Hash of the content (as returned by hash_content)
#' @param method_name Name of the method executed
#' @param parameters Parameters used for the method
#' @param timeout Maximum time to wait in seconds (default: NA for no timeout)
#' @param interval Polling interval in seconds (default: 5)
#' @param parse_json Whether to parse the output as JSON (default: TRUE)
#' @param validate_parameters Whether to validate parameters against method specification (default: TRUE)
#'
#' @return The job output if completed within timeout, otherwise throws an error
#' @export
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 9000, "please_change_me")
#' file_hash <- "abc123..."
#' results <- wait_for_job_results_by_hash(config, file_hash, "analyze_text", list(parameter1 = "value1"))
#' }
wait_for_job_results_by_hash <- function(config, file_hash, method_name, parameters = list(), 
                                         timeout = NA, interval = 5, parse_json = TRUE,
                                         validate_parameters = TRUE) {
  # Validate the hash input
  if (!is.character(file_hash) || length(file_hash) != 1) {
    stop("file_hash must be a single character string")
  }
  
  # Ensure interval is not too small
  interval <- max(interval, 1)
  
  # Start timer
  start_time <- Sys.time()
  
  # Sort parameters alphabetically to ensure consistent ordering
  sorted_params <- sort_parameters(parameters)
  
  # Validate parameters on first call
  if (validate_parameters) {
    # Get all available methods
    methods <- get_methods(config)
    
    # Validate parameters against method specification (use original parameters for validation)
    validate_method_parameters(method_name, parameters, methods)
  }
  
  # Create request body with sorted parameters
  body <- list(
    file_hash = file_hash,
    method_name = method_name,
    parameters = sorted_params
  )
  
  # Query the job initially
  job_info <- api_post(config, "/query-job", body = body)
  
  # Track last displayed status
  last_status_displayed <- NULL
  status_start_time <- Sys.time()
  
  # Define in-progress statuses (job is still being processed)
  IN_PROGRESS_STATUSES <- c("PD", "R", "CG", "CF", "S", "ST")
  
  # Loop while job is in progress (exit on any terminal state)
  while(is.null(job_info$status) || (job_info$status %in% IN_PROGRESS_STATUSES)) {
    # Check for timeout (only if specified)
    if (!is.na(timeout)) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
      if (elapsed > timeout) {
        stop(paste0("Timeout waiting for job to complete after ", timeout, " seconds. Current status: ", 
                    ifelse(is.null(job_info$status), "unknown", job_info$status)))
      }
    }
    
    current_status <- job_info$status
    timestamp <- format(Sys.time(), "%H:%M:%S")
    
    # Check if status changed
    if (!is.null(current_status) && (is.null(last_status_displayed) || last_status_displayed != current_status)) {
      # Status changed - show full info
      status_desc <- job_info$status_detail %||% current_status
      cat(sprintf("[%s] Job: %s [%s]\n", timestamp, current_status, status_desc))
      last_status_displayed <- current_status
      status_start_time <- Sys.time()
    } else if (!is.null(current_status)) {
      # Status didn't change - show heartbeat with elapsed time
      status_elapsed <- as.numeric(difftime(Sys.time(), status_start_time, units = "secs"))
      status_desc <- job_info$status_detail %||% current_status
      cat(sprintf("[%s] Job: %s [%s] (%.0fs)\n", timestamp, current_status, status_desc, status_elapsed))
    }
    
    # Wait before polling again
    Sys.sleep(interval)
    
    # Poll job status without validation
    job_info <- api_post(config, "/query-job", body = body)
  }
  
  # Job reached terminal state - check if it succeeded or failed
  if (job_info$status == "CD") {
    # Job completed successfully
    output <- job_info$output
    
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
  } else {
    # Job failed or was cancelled
    error_details <- job_info$error_details %||% job_info$message %||% "No error details available"
    stop(paste0("Job terminated with status ", job_info$status, ": ", error_details))
  }
}
