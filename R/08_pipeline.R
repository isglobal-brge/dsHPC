# Pipeline (DAG) Orchestration Functions

#' Submit a pipeline (DAG of meta-jobs)
#'
#' @param config API configuration created by create_api_config
#' @param pipeline_definition List with 'nodes'
#'
#' @return Pipeline response with pipeline_id
#' @export
#'
#' @examples
#' \dontrun{
#' pipeline <- list(
#'   nodes = list(
#'     node_1 = list(
#'       chain = list(list(method_name = "concat", parameters = list(a = "Hello", b = "World"))),
#'       dependencies = list()
#'     ),
#'     node_2 = list(
#'       chain = list(list(method_name = "concat", parameters = list(a = "$ref:node_1", b = "!"))),
#'       dependencies = list("node_1")
#'     )
#'   )
#' )
#' 
#' response <- submit_pipeline(config, pipeline)
#' }
submit_pipeline <- function(config, pipeline_definition) {
  if (!is.list(pipeline_definition) || is.null(pipeline_definition$nodes)) {
    stop("pipeline_definition must be a list with 'nodes'")
  }
  
  # Submit via API (use internal function)
  response <- dsHPC:::api_post(config, "/pipeline/submit", body = pipeline_definition)
  
  return(response)
}


#' Get pipeline status
#'
#' @param config API configuration
#' @param pipeline_id Pipeline ID
#'
#' @return Pipeline status information
#' @export
get_pipeline_status <- function(config, pipeline_id) {
  if (!is.character(pipeline_id) || length(pipeline_id) != 1) {
    stop("pipeline_id must be a single character string")
  }
  
  status <- dsHPC:::api_get(config, paste0("/pipeline/", pipeline_id))
  
  return(status)
}


#' Wait for pipeline to complete
#'
#' @param config API configuration
#' @param pipeline_id Pipeline ID to wait for
#' @param timeout Maximum time to wait in seconds (default: NA for no timeout)
#' @param interval Polling interval in seconds (default: 5)
#' @param verbose Show progress updates (default: TRUE)
#'
#' @return Final pipeline status
#' @export
wait_for_pipeline <- function(config, pipeline_id, timeout = NA, interval = 5, verbose = TRUE) {
  if (!is.character(pipeline_id) || length(pipeline_id) != 1) {
    stop("pipeline_id must be a single character string")
  }
  
  interval <- max(interval, 1)
  start_time <- Sys.time()
  
  # Track last displayed status
  last_status_str <- NULL
  
  while(TRUE) {
    # Check for timeout
    if (!is.na(timeout)) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
      if (elapsed > timeout) {
        stop(paste0("Timeout waiting for pipeline after ", timeout, " seconds"))
      }
    }
    
    # Get status
    status <- get_pipeline_status(config, pipeline_id)
    
    # Display progress if verbose
    if (verbose) {
      # Create status string
      status_str <- sprintf("[%s] Pipeline: %s | Progress: %.1f%% (%d/%d nodes) | Running: %d | Failed: %d",
                           format(Sys.time(), "%H:%M:%S"),
                           status$status,
                           status$progress_percentage,
                           status$completed_nodes,
                           status$total_nodes,
                           status$running_nodes,
                           status$failed_nodes)
      
      # Only print if changed
      if (is.null(last_status_str) || last_status_str != status_str) {
        cat(status_str, "\n")
        last_status_str <- status_str
      }
    }
    
    # Check if done
    if (status$status %in% c("completed", "failed", "cancelled")) {
      if (verbose) cat("\n")
      
      if (status$status == "failed") {
        # Build error message with failed nodes
        failed_nodes <- Filter(function(n) n$status == "failed", status$nodes)
        error_msg <- sprintf("Pipeline failed. %d nodes failed:\n", length(failed_nodes))
        for (node in failed_nodes) {
          error_msg <- paste0(error_msg, sprintf("  - %s: %s\n", 
                                                 node$node_id, 
                                                 node$error %||% "Unknown error"))
        }
        stop(error_msg)
      }
      
      return(status)
    }
    
    Sys.sleep(interval)
  }
}


#' Execute pipeline and wait for results
#'
#' @param config API configuration
#' @param pipeline_definition Pipeline definition
#' @param timeout Maximum time to wait (default: NA)
#' @param interval Polling interval (default: 5)
#' @param verbose Show progress (default: TRUE)
#'
#' @return Final output from pipeline
#' @export
#'
#' @examples
#' \dontrun{
#' pipeline <- list(
#'   nodes = list(
#'     node_1 = list(
#'       chain = list(list(method_name = "concat", parameters = list(a = "A", b = "B"))),
#'       dependencies = list()
#'     ),
#'     node_2 = list(
#'       chain = list(list(method_name = "concat", parameters = list(a = "C", b = "D"))),
#'       dependencies = list()
#'     ),
#'     node_3 = list(
#'       chain = list(list(method_name = "concat", parameters = list(a = "$ref:node_1", b = "$ref:node_2"))),
#'       dependencies = list("node_1", "node_2")
#'     )
#'   )
#' )
#' 
#' result <- execute_pipeline(config, pipeline)
#' }
execute_pipeline <- function(config, pipeline_definition, timeout = NA, interval = 5, verbose = TRUE) {
  # Submit pipeline
  if (verbose) {
    message(sprintf("Submitting pipeline with %d nodes...", length(pipeline_definition$nodes)))
  }
  
  response <- submit_pipeline(config, pipeline_definition)
  pipeline_id <- response$pipeline_id
  
  if (verbose) {
    message(sprintf("Pipeline ID: %s", pipeline_id))
    message("")
  }
  
  # Wait for completion
  final_status <- wait_for_pipeline(config, pipeline_id, timeout, interval, verbose)
  
  # Return final output
  return(final_status$final_output)
}


# Helper for creating pipeline nodes
#' Create a pipeline node definition
#'
#' @param chain List of method steps
#' @param dependencies Vector of node IDs this depends on
#' @param input_file_hash Optional file hash for root nodes
#'
#' @return Node definition
#' @export
create_pipeline_node <- function(chain, dependencies = character(0), input_file_hash = NULL) {
  node <- list(
    chain = chain,
    dependencies = as.list(dependencies)
  )
  
  if (!is.null(input_file_hash)) {
    node$input_file_hash <- input_file_hash
  }
  
  return(node)
}

