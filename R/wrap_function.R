#' @title Function Wrapping Utility
#' @description Creates a wrapped version of a function for execution in HPC.
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
      func = func,
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