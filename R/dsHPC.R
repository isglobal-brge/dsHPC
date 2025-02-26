#' @title dsHPC: DataSHIELD Interface for High-Performance Computing
#' @description This package provides a server-side interface for DataSHIELD packages to submit
#' and manage jobs on High-Performance Computing clusters. It supports caching results to avoid
#' redundant computations and provides tools for job lifecycle management.

#' @title Initialize the dsHPC System
#' @description Sets up the dsHPC system by initializing the database connection and checking
#' for available job schedulers.
#' @param db_path Optional character string specifying the path to the SQLite database file.
#'   If NULL (default), uses the package's default location in the user's home directory.
#' @param scheduler Character string specifying the job scheduler to use. Currently only "slurm"
#'   is supported.
#' @return A list containing configuration details and connection objects.
#' @examples
#' \dontrun{
#' # Initialize with default settings
#' dsHPC_config <- dsHPC.init()
#' 
#' # Initialize with a custom database path
#' dsHPC_config <- dsHPC.init(db_path = "/path/to/custom/database.sqlite")
#' }
#' @export
dsHPC.init <- function(db_path = NULL, scheduler = "slurm") {
  # Initialize database connection
  con <- init_db_connection(db_path)
  
  # Check for scheduler availability
  scheduler_available <- FALSE
  if (scheduler == "slurm") {
    scheduler_available <- is_slurm_available()
    if (!scheduler_available) {
      warning("Slurm scheduler is not available on this system. Job submission will be simulated.")
    }
  } else {
    stop(paste("Unsupported scheduler:", scheduler))
  }
  
  # Create global configuration
  config <- list(
    connection = con,
    scheduler = scheduler,
    scheduler_available = scheduler_available,
    init_time = Sys.time()
  )
  
  # Set option to make the configuration accessible throughout the package
  options(dsHPC.config = config)
  
  return(invisible(config))
}

#' @title Submit a Function Execution as a Job
#' @description Submits a function to be executed as a job on the HPC cluster.
#' @param func_name Character string with the name of the function to execute.
#' @param args List of arguments to pass to the function.
#' @param package Character string with the package name containing the function.
#' @param object_hash Optional character string with a hash identifying a data object.
#' @param required_packages Character vector with names of additional packages to load.
#' @param use_cache Logical indicating whether to check for cached results (default: TRUE).
#' @param cache_only Logical indicating whether to only check the cache without submitting a job (default: FALSE).
#' @param slurm_opts List of options for Slurm job submission (partition, memory, time, cpus, etc.).
#' @return A list containing job information including job_id, status, and result (if available from cache).
#' @examples
#' \dontrun{
#' # Initialize dsHPC first
#' dsHPC.init()
#' 
#' # Submit a simple job
#' job <- dsHPC.submit(
#'   func_name = "mean",
#'   args = list(x = c(1, 2, 3, 4, 5))
#' )
#' 
#' # Submit a job with package function and custom Slurm options
#' job <- dsHPC.submit(
#'   func_name = "kmeans",
#'   args = list(x = my_data_matrix, centers = 3),
#'   package = "stats",
#'   required_packages = c("stats"),
#'   slurm_opts = list(
#'     partition = "normal",
#'     memory = "8G",
#'     time = "02:00:00",
#'     cpus = 2
#'   )
#' )
#' }
#' @export
dsHPC.submit <- function(func_name, args, package = NULL, object_hash = NULL,
                         required_packages = NULL, use_cache = TRUE, cache_only = FALSE,
                         slurm_opts = list()) {
  
  # Get configuration
  config <- getOption("dsHPC.config")
  if (is.null(config)) {
    stop("dsHPC has not been initialized. Call dsHPC.init() first.")
  }
  
  # Create a job hash for identifying this specific job
  job_hash <- create_job_hash(func_name, args, object_hash)
  
  # Check if result is already cached
  if (use_cache) {
    cached_result <- get_cached_result(config$connection, job_hash)
    if (!is.null(cached_result)) {
      message("Using cached result for this job")
      return(list(
        job_id = NULL,
        job_hash = job_hash,
        status = "CACHED",
        result = cached_result
      ))
    }
  }
  
  # If cache_only is TRUE, return NULL for result
  if (cache_only) {
    return(list(
      job_id = NULL,
      job_hash = job_hash,
      status = "NOT_CACHED",
      result = NULL
    ))
  }
  
  # Generate a new job ID
  job_id <- generate_job_id()
  
  # Create temporary result file path
  result_dir <- file.path(tempdir(), "dsHPC_results")
  if (!dir.exists(result_dir)) {
    dir.create(result_dir, recursive = TRUE)
  }
  result_file <- file.path(result_dir, paste0(job_id, ".rds"))
  
  # Store job information in database
  store_job_info(config$connection, job_id, job_hash, func_name, args, object_hash)
  
  # Check scheduler availability
  if (config$scheduler_available) {
    if (config$scheduler == "slurm") {
      # Create R script for the job
      r_script <- create_r_script(func_name, args, package, result_file, required_packages)
      
      # Set default Slurm options
      default_slurm_opts <- list(
        partition = "normal",
        memory = "4G",
        time = "01:00:00",
        cpus = 1,
        r_module = NULL,
        additional_modules = NULL,
        env_vars = NULL
      )
      
      # Merge with user-provided options
      slurm_opts <- utils::modifyList(default_slurm_opts, slurm_opts)
      
      # Create Slurm submission script
      submission_script <- create_slurm_script(
        r_script = r_script,
        job_name = paste0("dsHPC_", job_id),
        partition = slurm_opts$partition,
        memory = slurm_opts$memory,
        time = slurm_opts$time,
        cpus = slurm_opts$cpus,
        r_module = slurm_opts$r_module,
        additional_modules = slurm_opts$additional_modules,
        env_vars = slurm_opts$env_vars
      )
      
      # Submit job to Slurm
      slurm_id <- submit_slurm_job(submission_script)
      
      if (!is.null(slurm_id)) {
        # Update job with Slurm ID
        update_job_status(config$connection, job_id, "SUBMITTED", slurm_id)
        
        return(list(
          job_id = job_id,
          job_hash = job_hash,
          scheduler_id = slurm_id,
          status = "SUBMITTED",
          result = NULL
        ))
      } else {
        update_job_status(config$connection, job_id, "FAILED", 
                         error_message = "Failed to submit job to Slurm")
        stop("Failed to submit job to Slurm")
      }
    }
  } else {
    # Run locally if scheduler is not available
    message("Job scheduler not available. Running job locally...")
    
    # Execute function
    result <- tryCatch({
      if (!is.null(package)) {
        if (!requireNamespace(package, quietly = TRUE)) {
          stop(paste("Package", package, "is not available"))
        }
        do.call(get(func_name, envir = asNamespace(package)), args)
      } else {
        do.call(func_name, args)
      }
    }, error = function(e) {
      update_job_status(config$connection, job_id, "FAILED", 
                       error_message = as.character(e), completed = TRUE)
      stop(e)
    })
    
    # Update job status and cache result
    update_job_status(config$connection, job_id, "COMPLETED", completed = TRUE)
    store_job_result(config$connection, job_hash, result)
    
    return(list(
      job_id = job_id,
      job_hash = job_hash,
      status = "COMPLETED",
      result = result
    ))
  }
}

#' @title Get Job Status
#' @description Checks the current status of a job and updates the database.
#' @param job_id Character string with the job ID.
#' @param return_result Logical indicating whether to retrieve the result if completed.
#' @return A list containing job information including status and result (if available and requested).
#' @examples
#' \dontrun{
#' # Initialize dsHPC first
#' dsHPC.init()
#' 
#' # Submit a job
#' job <- dsHPC.submit(
#'   func_name = "mean",
#'   args = list(x = c(1, 2, 3, 4, 5))
#' )
#' 
#' # Check job status
#' status <- dsHPC.status(job$job_id)
#' 
#' # Check status and get result if available
#' result <- dsHPC.status(job$job_id, return_result = TRUE)
#' }
#' @export
dsHPC.status <- function(job_id, return_result = FALSE) {
  # Get configuration
  config <- getOption("dsHPC.config")
  if (is.null(config)) {
    stop("dsHPC has not been initialized. Call dsHPC.init() first.")
  }
  
  # Get job information from database
  job_info <- get_job_info(config$connection, job_id)
  
  if (is.null(job_info)) {
    stop(paste("Job not found:", job_id))
  }
  
  # Check if job is already completed or failed
  if (job_info$status %in% c("COMPLETED", "FAILED", "CACHED")) {
    # Job is already in a terminal state
    response <- list(
      job_id = job_id,
      job_hash = job_info$job_hash,
      status = job_info$status
    )
    
    # Add result if requested and available
    if (return_result && job_info$status == "COMPLETED") {
      result <- get_cached_result(config$connection, job_info$job_hash)
      response$result <- result
    }
    
    return(response)
  }
  
  # Check status with scheduler
  if (config$scheduler_available && !is.null(job_info$scheduler_id)) {
    if (config$scheduler == "slurm") {
      # Check Slurm job status
      slurm_status <- check_slurm_job_status(job_info$scheduler_id)
      
      if (slurm_status == "COMPLETED") {
        # Job is completed, get the result
        result_dir <- file.path(tempdir(), "dsHPC_results")
        result_file <- file.path(result_dir, paste0(job_id, ".rds"))
        
        if (file.exists(result_file)) {
          result <- readRDS(result_file)
          
          # Check if result is an error
          if (inherits(result, "error")) {
            update_job_status(config$connection, job_id, "FAILED", 
                             error_message = as.character(result), completed = TRUE)
            
            return(list(
              job_id = job_id,
              job_hash = job_info$job_hash,
              status = "FAILED",
              error = as.character(result)
            ))
          } else {
            # Store the result in cache
            store_job_result(config$connection, job_info$job_hash, result)
            update_job_status(config$connection, job_id, "COMPLETED", completed = TRUE)
            
            response <- list(
              job_id = job_id,
              job_hash = job_info$job_hash,
              status = "COMPLETED"
            )
            
            if (return_result) {
              response$result <- result
            }
            
            return(response)
          }
        } else {
          # Result file not found
          update_job_status(config$connection, job_id, "FAILED", 
                          error_message = "Result file not found", completed = TRUE)
          
          return(list(
            job_id = job_id,
            job_hash = job_info$job_hash,
            status = "FAILED",
            error = "Result file not found"
          ))
        }
      } else if (slurm_status == "FAILED") {
        update_job_status(config$connection, job_id, "FAILED", completed = TRUE)
        
        return(list(
          job_id = job_id,
          job_hash = job_info$job_hash,
          status = "FAILED"
        ))
      } else {
        # Job is still running
        update_job_status(config$connection, job_id, slurm_status)
        
        return(list(
          job_id = job_id,
          job_hash = job_info$job_hash,
          status = slurm_status
        ))
      }
    }
  } else {
    # If scheduler not available, the job should have been run locally and already completed
    return(list(
      job_id = job_id,
      job_hash = job_info$job_hash,
      status = job_info$status
    ))
  }
}

#' @title List All Jobs
#' @description Lists all jobs in the database, optionally filtering by status.
#' @param status_filter Optional character vector with statuses to filter by.
#' @return A data frame containing job information.
#' @examples
#' \dontrun{
#' # Initialize dsHPC first
#' dsHPC.init()
#' 
#' # List all jobs
#' all_jobs <- dsHPC.list_jobs()
#' 
#' # List only completed jobs
#' completed_jobs <- dsHPC.list_jobs(status_filter = "COMPLETED")
#' 
#' # List failed and running jobs
#' jobs <- dsHPC.list_jobs(status_filter = c("FAILED", "RUNNING"))
#' }
#' @export
dsHPC.list_jobs <- function(status_filter = NULL) {
  # Get configuration
  config <- getOption("dsHPC.config")
  if (is.null(config)) {
    stop("dsHPC has not been initialized. Call dsHPC.init() first.")
  }
  
  # Build query
  query <- "SELECT job_id, job_hash, function_name, status, submission_time, completion_time, scheduler_id FROM jobs"
  
  if (!is.null(status_filter)) {
    status_values <- paste("'", status_filter, "'", collapse = ",", sep = "")
    query <- paste0(query, " WHERE status IN (", status_values, ")")
  }
  
  query <- paste0(query, " ORDER BY submission_time DESC")
  
  # Execute query
  jobs <- DBI::dbGetQuery(config$connection, query)
  
  return(jobs)
}

#' @title Cancel a Job
#' @description Cancels a running job.
#' @param job_id Character string with the job ID.
#' @return Logical indicating whether the cancellation was successful.
#' @examples
#' \dontrun{
#' # Initialize dsHPC first
#' dsHPC.init()
#' 
#' # Submit a job
#' job <- dsHPC.submit(
#'   func_name = "mean",
#'   args = list(x = c(1, 2, 3, 4, 5))
#' )
#' 
#' # Cancel the job
#' dsHPC.cancel(job$job_id)
#' }
#' @export
dsHPC.cancel <- function(job_id) {
  # Get configuration
  config <- getOption("dsHPC.config")
  if (is.null(config)) {
    stop("dsHPC has not been initialized. Call dsHPC.init() first.")
  }
  
  # Get job information from database
  job_info <- get_job_info(config$connection, job_id)
  
  if (is.null(job_info)) {
    stop(paste("Job not found:", job_id))
  }
  
  # Check if job can be cancelled
  if (!job_info$status %in% c("SUBMITTED", "PENDING", "RUNNING")) {
    warning(paste("Job is in", job_info$status, "state and cannot be cancelled"))
    return(FALSE)
  }
  
  # Cancel job with scheduler
  if (config$scheduler_available && !is.null(job_info$scheduler_id)) {
    if (config$scheduler == "slurm") {
      cancelled <- cancel_slurm_job(job_info$scheduler_id)
      
      if (cancelled) {
        update_job_status(config$connection, job_id, "CANCELLED", completed = TRUE)
      }
      
      return(cancelled)
    }
  } else {
    # Job can't be cancelled if scheduler not available
    warning("Cannot cancel job because scheduler is not available")
    return(FALSE)
  }
}

#' @title Clean Old Cached Results
#' @description Removes cached results older than a specified time period.
#'   This function only removes entries from the cache that are older than
#'   the specified number of days and are not referenced by active jobs.
#' @param days_to_keep Integer specifying how many days of cache to retain (default: 30).
#'   Set to 0 to remove all completed job results that are not referenced by active jobs.
#' @return The number of records deleted.
#' @examples
#' \dontrun{
#' # Initialize dsHPC first
#' dsHPC.init()
#' 
#' # Clean results older than 14 days
#' dsHPC.clean_cache(days_to_keep = 14)
#' 
#' # Remove all completed job results
#' dsHPC.clean_cache(days_to_keep = 0)
#' }
#' @export
dsHPC.clean_cache <- function(days_to_keep = 30) {
  # Get configuration
  config <- getOption("dsHPC.config")
  if (is.null(config)) {
    stop("dsHPC has not been initialized. Call dsHPC.init() first.")
  }
  
  # Clean old results
  deleted <- clean_old_results(config$connection, days_to_keep)
  
  message(paste("Deleted", deleted, "cached results"))
  return(invisible(deleted))
}

#' @title Get Job Result
#' @description Retrieves the result of a job by its ID.
#' @param job_id Character string with the job ID.
#' @return The job result or NULL if the job is not completed or the result is not available.
#' @examples
#' \dontrun{
#' # Initialize dsHPC first
#' dsHPC.init()
#' 
#' # Submit a simple job
#' job <- dsHPC.submit(mean, args = list(x = 1:10))
#' 
#' # Wait for job completion (in real applications, this would usually be asynchronous)
#' while (dsHPC.status(job$job_id)$status == "RUNNING") {
#'   Sys.sleep(1)
#' }
#' 
#' # Get the job result
#' result <- dsHPC.result(job$job_id)
#' print(result)
#' }
#' @export
dsHPC.result <- function(job_id) {
  # Get configuration
  config <- getOption("dsHPC.config")
  if (is.null(config)) {
    stop("dsHPC has not been initialized. Call dsHPC.init() first.")
  }
  
  # Check if the job exists
  job_info <- get_job_info(config$connection, job_id)
  if (is.null(job_info)) {
    stop("Job not found.")
  }
  
  # Check if the job has completed
  if (job_info$status != "COMPLETED" && job_info$status != "CACHED") {
    warning("Job has not completed. Current status: ", job_info$status)
    return(NULL)
  }
  
  # Retrieve the result from the cache
  result <- get_cached_result(config$connection, job_info$object_hash)
  
  # If we don't find the result in the cache (shouldn't happen with new design)
  if (is.null(result)) {
    warning("Result not found in cache despite job being marked as completed.")
    return(NULL)
  }
  
  return(result)
} 