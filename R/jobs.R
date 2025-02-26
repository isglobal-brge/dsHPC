#' @title Submit Job by Function
#' @description Submit a job by providing a function and arguments.
#' @param func Function to run in the job.
#' @param args List of arguments to pass to the function.
#' @param slurm_opts Optional list with Slurm-specific options (partition, memory, time, cpus).
#' @param use_cache Logical indicating whether to reuse cached results.
#' @param required_packages Character vector with additional packages to load.
#' @return List with job information.
#' @examples
#' \dontrun{
#' # Initialize dsHPC first
#' dsHPC.init()
#' 
#' # Submit a simple job
#' job <- dsHPC.submit(mean, args = list(x = 1:10))
#' 
#' # Check job status
#' status <- dsHPC.status(job$job_id)
#' 
#' # Get result when job is complete
#' if (status$status == "COMPLETED") {
#'   result <- dsHPC.result(job$job_id)
#'   print(result)
#' }
#' }
#' @export
dsHPC.submit <- function(func, args = list(), slurm_opts = list(), 
                          use_cache = TRUE, required_packages = NULL) {
  # Validate input
  if (!is.function(func)) {
    stop("func must be a function")
  }
  
  if (!is.list(args)) {
    stop("args must be a list")
  }
  
  # Get configuration
  config <- getOption("dsHPC.config")
  if (is.null(config)) {
    stop("dsHPC has not been initialized. Call dsHPC.init() first.")
  }
  
  # Create unique function name
  func_name <- sprintf("dsHPC_func_%s", uuid::UUIDgenerate(use.time = TRUE))
  
  # Calculate a hash for the function and arguments
  func_str <- deparse(func)
  job_hash <- create_job_hash(func_str, args)
  
  # Check if result is already cached
  if (use_cache) {
    cached_result <- get_cached_result(config$connection, job_hash)
    if (!is.null(cached_result)) {
      message("Using cached result")
      return(list(
        job_id = uuid::UUIDgenerate(),
        job_hash = job_hash,
        status = "CACHED",
        result = cached_result
      ))
    }
  }
  
  # Generate job ID
  job_id <- uuid::UUIDgenerate()
  
  # Store function and job info
  assign(func_name, func, envir = parent.frame())
  store_job_info(config$connection, job_id, job_hash, func_name, args, status = "SUBMITTED")
  
  # Execute job locally if no scheduler or in simulation mode
  if (!config$scheduler_available || config$simulation_mode) {
    tryCatch({
      # Load required packages
      if (!is.null(required_packages)) {
        for (pkg in required_packages) {
          if (!requireNamespace(pkg, quietly = TRUE)) {
            stop(paste("Required package not available:", pkg))
          }
          library(pkg, character.only = TRUE)
        }
      }
      
      # Execute function with provided arguments
      result <- do.call(func, args)
      
      # Always store the result in the cache
      store_job_result(config$connection, job_hash, result)
      
      # Update job status
      update_job_status(config$connection, job_id, "COMPLETED", completed = TRUE)
      
      return(list(
        job_id = job_id,
        job_hash = job_hash,
        status = "COMPLETED",
        result = result
      ))
    }, error = function(e) {
      update_job_status(config$connection, job_id, "FAILED", 
                        error_message = as.character(e), completed = TRUE)
      stop(paste("Job execution failed:", e))
    })
  } else {
    # Create job script if using a scheduler
    job_script <- create_job_script(func_name, args, job_id, config$db_path, 
                                  required_packages, slurm_opts)
    
    # Submit job to scheduler (e.g., Slurm)
    job_script_path <- file.path(config$scripts_dir, paste0(job_id, ".R"))
    writeLines(job_script, job_script_path)
    
    if (config$scheduler == "slurm") {
      # Prepare Slurm command
      slurm_cmd <- create_slurm_command(job_id, job_script_path, slurm_opts, config)
      
      # Submit job to Slurm
      submit_result <- system(slurm_cmd, intern = TRUE)
      slurm_id <- extract_slurm_id(submit_result)
      
      # Update job info with Slurm job ID
      update_job_scheduler_id(config$connection, job_id, slurm_id)
      
      return(list(
        job_id = job_id,
        job_hash = job_hash,
        status = "SUBMITTED",
        scheduler_id = slurm_id
      ))
    } else {
      stop(paste("Unsupported scheduler:", config$scheduler))
    }
  }
}

#' @title Submit Job by Function Name
#' @description Submit a job by providing a function name and arguments.
#' @param func_name Character string with the name of the function to run.
#' @param args List of arguments to pass to the function.
#' @param object_hash Optional pre-computed hash for the function and arguments.
#' @param slurm_opts Optional list with Slurm-specific options (partition, memory, time, cpus).
#' @param use_cache Logical indicating whether to reuse cached results.
#' @param required_packages Character vector with additional packages to load.
#' @return List with job information.
#' @examples
#' \dontrun{
#' # Initialize dsHPC first
#' dsHPC.init()
#' 
#' # Submit a job
#' job <- dsHPC.submit_by_name(
#'   func_name = "mean",
#'   args = list(x = c(1, 2, 3, 4, 5))
#' )
#' 
#' # Check job status
#' status <- dsHPC.status(job$job_id)
#' 
#' # Get result when job is complete
#' if (status$status == "COMPLETED") {
#'   result <- dsHPC.result(job$job_id)
#'   print(result)
#' }
#' }
#' @export
dsHPC.submit_by_name <- function(func_name, args = list(), object_hash = NULL,
                                slurm_opts = list(), use_cache = TRUE, 
                                required_packages = NULL) {
  # Validate input
  if (!is.character(func_name) || length(func_name) != 1) {
    stop("func_name must be a single character string")
  }
  
  if (!is.list(args)) {
    stop("args must be a list")
  }
  
  # Get configuration
  config <- getOption("dsHPC.config")
  if (is.null(config)) {
    stop("dsHPC has not been initialized. Call dsHPC.init() first.")
  }
  
  # Check if function exists
  if (!exists(func_name, mode = "function", envir = parent.frame())) {
    stop(paste("Function", func_name, "not found"))
  }
  
  # Get function
  func <- get(func_name, mode = "function", envir = parent.frame())
  
  # Calculate a hash for the function and arguments if not provided
  if (is.null(object_hash)) {
    func_str <- deparse(func)
    job_hash <- create_job_hash(func_str, args)
  } else {
    job_hash <- object_hash
  }
  
  # Check if result is already cached
  if (use_cache) {
    cached_result <- get_cached_result(config$connection, job_hash)
    if (!is.null(cached_result)) {
      message("Using cached result")
      return(list(
        job_id = uuid::UUIDgenerate(),
        job_hash = job_hash,
        status = "CACHED",
        result = cached_result
      ))
    }
  }
  
  # Generate job ID
  job_id <- uuid::UUIDgenerate()
  
  # Store job info
  store_job_info(config$connection, job_id, job_hash, func_name, args, status = "SUBMITTED")
  
  # Execute job locally if no scheduler or in simulation mode
  if (!config$scheduler_available || config$simulation_mode) {
    tryCatch({
      # Load required packages
      if (!is.null(required_packages)) {
        for (pkg in required_packages) {
          if (!requireNamespace(pkg, quietly = TRUE)) {
            stop(paste("Required package not available:", pkg))
          }
          library(pkg, character.only = TRUE)
        }
      }
      
      # Execute function with provided arguments
      result <- do.call(func, args)
      
      # Always store the result in the cache
      store_job_result(config$connection, job_hash, result)
      
      # Update job status
      update_job_status(config$connection, job_id, "COMPLETED", completed = TRUE)
      
      return(list(
        job_id = job_id,
        job_hash = job_hash,
        status = "COMPLETED",
        result = result
      ))
    }, error = function(e) {
      update_job_status(config$connection, job_id, "FAILED", 
                        error_message = as.character(e), completed = TRUE)
      stop(paste("Job execution failed:", e))
    })
  } else {
    # Create job script if using a scheduler
    job_script <- create_job_script(func_name, args, job_id, config$db_path, 
                                  required_packages, slurm_opts)
    
    # Submit job to scheduler (e.g., Slurm)
    job_script_path <- file.path(config$scripts_dir, paste0(job_id, ".R"))
    writeLines(job_script, job_script_path)
    
    if (config$scheduler == "slurm") {
      # Prepare Slurm command
      slurm_cmd <- create_slurm_command(job_id, job_script_path, slurm_opts, config)
      
      # Submit job to Slurm
      submit_result <- system(slurm_cmd, intern = TRUE)
      slurm_id <- extract_slurm_id(submit_result)
      
      # Update job info with Slurm job ID
      update_job_scheduler_id(config$connection, job_id, slurm_id)
      
      return(list(
        job_id = job_id,
        job_hash = job_hash,
        status = "SUBMITTED",
        scheduler_id = slurm_id
      ))
    } else {
      stop(paste("Unsupported scheduler:", config$scheduler))
    }
  }
} 