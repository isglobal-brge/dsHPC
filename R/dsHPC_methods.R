#' @title DataSHIELD Integration Functions
#' @description This file contains functions for integrating dsHPC with the DataSHIELD framework.

#' @title Initialize dsHPC for DataSHIELD
#' @description Initializes the dsHPC system for use in a DataSHIELD environment.
#' @details This function should be called when the dsHPC package is loaded in a DataSHIELD
#' environment. It initializes the database connection and checks for available job schedulers.
#' @param db_path Optional character string specifying the path to the SQLite database file.
#' @param scheduler Character string specifying the job scheduler to use.
#' @return NULL invisibly.
#' @export
dsHPC.ds.init <- function(db_path = NULL, scheduler = "slurm") {
  # Check if running in DataSHIELD environment
  if (!is_datashield_env()) {
    stop("This function can only be called in a DataSHIELD environment.")
  }
  
  # Initialize the dsHPC system
  dsHPC.init(db_path, scheduler)
  
  return(invisible(NULL))
}

#' @title Submit a function execution as a job in DataSHIELD environment
#' @description DataSHIELD-compatible version of dsHPC.submit.
#' @param func_name Character string with the name of the function to execute.
#' @param args List of arguments to pass to the function.
#' @param package Character string with the package name containing the function.
#' @param required_packages Character vector with names of additional packages to load.
#' @param object_hash Optional character string with a hash identifying a data object.
#' @param use_cache Logical indicating whether to check for cached results.
#' @param slurm_opts List of options for Slurm job submission.
#' @return A list containing job information.
#' @export
dsHPC.ds.submit <- function(func_name, args, package = NULL, required_packages = NULL,
                          object_hash = NULL, use_cache = TRUE, slurm_opts = list()) {
  # Check if running in DataSHIELD environment
  if (!is_datashield_env()) {
    stop("This function can only be called in a DataSHIELD environment.")
  }
  
  # Submit the job
  job <- dsHPC.submit(
    func_name = func_name,
    args = args,
    package = package,
    object_hash = object_hash,
    required_packages = required_packages,
    use_cache = use_cache,
    slurm_opts = slurm_opts
  )
  
  # For DataSHIELD, return a sanitized version without the result
  sanitized_job <- list(
    job_id = job$job_id,
    job_hash = job$job_hash,
    status = job$status
  )
  
  return(sanitized_job)
}

#' @title Get job status in DataSHIELD environment
#' @description DataSHIELD-compatible version of dsHPC.status.
#' @param job_id Character string with the job ID.
#' @return A list containing job status information.
#' @export
dsHPC.ds.status <- function(job_id) {
  # Check if running in DataSHIELD environment
  if (!is_datashield_env()) {
    stop("This function can only be called in a DataSHIELD environment.")
  }
  
  # Get job status
  status <- dsHPC.status(job_id, return_result = FALSE)
  
  # For DataSHIELD, return a sanitized version
  sanitized_status <- list(
    job_id = status$job_id,
    status = status$status
  )
  
  return(sanitized_status)
}

#' @title List jobs in DataSHIELD environment
#' @description DataSHIELD-compatible version of dsHPC.list_jobs.
#' @param status_filter Optional character vector with statuses to filter by.
#' @return A data frame containing job information.
#' @export
dsHPC.ds.list_jobs <- function(status_filter = NULL) {
  # Check if running in DataSHIELD environment
  if (!is_datashield_env()) {
    stop("This function can only be called in a DataSHIELD environment.")
  }
  
  # List jobs
  jobs <- dsHPC.list_jobs(status_filter = status_filter)
  
  # For DataSHIELD, return only essential columns
  sanitized_jobs <- jobs[, c("job_id", "function_name", "status", "submission_time", "completion_time")]
  
  return(sanitized_jobs)
}

#' @title Cancel a job in DataSHIELD environment
#' @description DataSHIELD-compatible version of dsHPC.cancel.
#' @param job_id Character string with the job ID.
#' @return Logical indicating whether the cancellation was successful.
#' @export
dsHPC.ds.cancel <- function(job_id) {
  # Check if running in DataSHIELD environment
  if (!is_datashield_env()) {
    stop("This function can only be called in a DataSHIELD environment.")
  }
  
  # Cancel job
  result <- dsHPC.cancel(job_id)
  
  return(result)
}

#' @title Get job result in DataSHIELD environment
#' @description DataSHIELD-compatible version of dsHPC.result.
#' @param job_id Character string with the job ID.
#' @param wait_for_completion Logical indicating whether to wait for job completion.
#' @param timeout Integer specifying the maximum time to wait in seconds.
#' @return The job result, or NULL if not available.
#' @export
dsHPC.ds.result <- function(job_id, wait_for_completion = FALSE, timeout = 300) {
  # Check if running in DataSHIELD environment
  if (!is_datashield_env()) {
    stop("This function can only be called in a DataSHIELD environment.")
  }
  
  # Get job result
  result <- dsHPC.result(job_id, wait_for_completion = wait_for_completion, timeout = timeout)
  
  return(result)
}

#' @title DataSHIELD Function Wrapping Utility
#' @description Creates a DataSHIELD-compatible wrapped version of a function for execution in HPC.
#' @param func Function to wrap.
#' @param package Character string with the package name containing the function.
#' @param required_packages Character vector with names of additional packages to load.
#' @param slurm_opts List of options for Slurm job submission.
#' @param default_wait Logical indicating whether to wait for completion by default.
#' @return A function that will execute the original function via HPC.
#' @export
dsHPC.wrap_function <- function(func, package = NULL, required_packages = NULL,
                               slurm_opts = list(), default_wait = TRUE) {
  
  # Get function name
  func_name <- as.character(substitute(func))
  
  # Create wrapper function
  wrapper <- function(...) {
    # Check if dsHPC is initialized
    config <- getOption("dsHPC.config")
    if (is.null(config)) {
      stop("dsHPC has not been initialized. Call dsHPC.init() first.")
    }
    
    # Capture arguments
    args <- list(...)
    
    # Submit job
    job <- dsHPC.submit(
      func_name = func_name,
      args = args,
      package = package,
      required_packages = required_packages,
      use_cache = TRUE,
      slurm_opts = slurm_opts
    )
    
    # If result is already available from cache, return it
    if (job$status == "CACHED" && !is.null(job$result)) {
      return(job$result)
    }
    
    # If default_wait is TRUE, wait for completion
    if (default_wait) {
      result <- dsHPC.result(job$job_id, wait_for_completion = TRUE)
      return(result)
    } else {
      # Just return job information
      return(job)
    }
  }
  
  return(wrapper)
} 