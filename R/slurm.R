#' @title Slurm Job Scheduler Interface for dsHPC
#' @description This file contains functions for interacting with the Slurm job scheduler.

#' @title Check if Slurm is Available
#' @description Checks if Slurm commands are available on the system.
#' @return Logical indicating whether Slurm is available.
#' @keywords internal
is_slurm_available <- function() {
  # Check if sbatch command is available
  sbatch_check <- suppressWarnings(system("command -v sbatch", intern = TRUE, ignore.stderr = TRUE))
  return(length(sbatch_check) > 0)
}

#' @title Create Slurm Submission Script
#' @description Creates a temporary bash script for Slurm job submission.
#' @param r_script Path to the R script to be executed.
#' @param job_name Character string with the job name.
#' @param partition Character string with the Slurm partition to use.
#' @param memory Character string with the memory allocation (e.g., "4G").
#' @param time Character string with the time limit (e.g., "01:00:00").
#' @param cpus Integer specifying the number of CPUs to allocate.
#' @param r_module Character string with the name of the R module to load (or NULL).
#' @param additional_modules Character vector with additional modules to load.
#' @param env_vars Named list of environment variables to set.
#' @return Path to the created submission script.
#' @keywords internal
create_slurm_script <- function(r_script, job_name, partition = "normal",
                               memory = "4G", time = "01:00:00", cpus = 1,
                               r_module = NULL, additional_modules = NULL,
                               env_vars = NULL) {
  
  # Create a temporary file
  submission_script <- tempfile(pattern = "slurm_", fileext = ".sh")
  
  # Start building the script content
  script_content <- c(
    "#!/bin/bash",
    paste0("#SBATCH --job-name=", job_name),
    paste0("#SBATCH --partition=", partition),
    paste0("#SBATCH --mem=", memory),
    paste0("#SBATCH --time=", time),
    paste0("#SBATCH --cpus-per-task=", cpus),
    "#SBATCH --output=%j.out",
    "#SBATCH --error=%j.err",
    ""
  )
  
  # Add module loading if specified
  if (!is.null(r_module)) {
    script_content <- c(script_content, paste0("module load ", r_module))
  }
  
  if (!is.null(additional_modules)) {
    for (mod in additional_modules) {
      script_content <- c(script_content, paste0("module load ", mod))
    }
  }
  
  # Add environment variables
  if (!is.null(env_vars)) {
    script_content <- c(script_content, "")
    for (var_name in names(env_vars)) {
      script_content <- c(script_content, 
                         paste0("export ", var_name, "=", escape_shell_arg(env_vars[[var_name]])))
    }
  }
  
  # Add command to execute the R script
  script_content <- c(
    script_content,
    "",
    "# Execute the R script",
    paste0("Rscript ", r_script)
  )
  
  # Write the script to file
  writeLines(script_content, submission_script)
  
  # Make the script executable
  system(paste("chmod +x", submission_script))
  
  return(submission_script)
}

#' @title Create R Execution Script
#' @description Creates a temporary R script that will execute the specified function with arguments.
#' @param function_name Character string with the name of the function to execute.
#' @param args List of arguments to pass to the function.
#' @param package Character string with the package name containing the function.
#' @param result_file Character string with the path where the result should be saved.
#' @param required_packages Character vector with names of packages to load.
#' @return Path to the created R script.
#' @keywords internal
create_r_script <- function(function_name, args, package = NULL, 
                          result_file, required_packages = NULL) {
  
  # Create a temporary file
  r_script <- tempfile(pattern = "dsHPC_job_", fileext = ".R")
  
  # Start building the script content
  script_content <- c(
    "#!/usr/bin/env Rscript",
    "# R script created by dsHPC for executing a function in a Slurm job",
    ""
  )
  
  # Add library loading
  if (!is.null(required_packages)) {
    script_content <- c(script_content, "# Load required packages")
    for (pkg in required_packages) {
      script_content <- c(script_content, paste0("library(", pkg, ")"))
    }
    script_content <- c(script_content, "")
  }
  
  # Add the function call
  script_content <- c(
    script_content,
    "# Execute the specified function",
    "tryCatch({",
    paste0("  # Call: ", format_function_call(function_name, args, package)),
    ""
  )
  
  # Format the function call
  if (!is.null(package)) {
    script_content <- c(script_content, 
                       paste0("  result <- ", package, "::", function_name, "("))
  } else {
    script_content <- c(script_content, 
                       paste0("  result <- ", function_name, "("))
  }
  
  # Add arguments
  arg_lines <- character(0)
  for (arg_name in names(args)) {
    if (arg_name == "") {
      # Unnamed argument
      arg_lines <- c(arg_lines, paste0("    ", deparse(args[[arg_name]])))
    } else {
      # Named argument
      arg_lines <- c(arg_lines, paste0("    ", arg_name, " = ", deparse(args[[arg_name]])))
    }
  }
  
  # Join the arguments with commas
  if (length(arg_lines) > 0) {
    for (i in 1:(length(arg_lines) - 1)) {
      arg_lines[i] <- paste0(arg_lines[i], ",")
    }
    script_content <- c(script_content, arg_lines)
  }
  
  # Close the function call and save the result
  script_content <- c(
    script_content,
    "  )",
    "",
    "  # Save the result to file",
    paste0("  saveRDS(result, file = '", result_file, "')"),
    "}, error = function(e) {",
    "  # Save the error object in case of failure",
    paste0("  saveRDS(e, file = '", result_file, "')"),
    "  stop(e)",
    "})"
  )
  
  # Write the script to file
  writeLines(script_content, r_script)
  
  # Make the script executable
  system(paste("chmod +x", r_script))
  
  return(r_script)
}

#' @title Submit Job to Slurm
#' @description Submits a job to the Slurm scheduler.
#' @param submission_script Path to the Slurm submission script.
#' @return Character string with the Slurm job ID, or NULL if submission failed.
#' @keywords internal
submit_slurm_job <- function(submission_script) {
  # Submit the job to Slurm
  result <- suppressWarnings(
    system(paste("sbatch", submission_script), intern = TRUE, ignore.stderr = TRUE)
  )
  
  if (length(result) == 0) {
    warning("Failed to submit job to Slurm")
    return(NULL)
  }
  
  # Extract job ID from Slurm output (format: "Submitted batch job 123456")
  job_id <- stringr::str_extract(result, "\\d+")
  return(job_id)
}

#' @title Check Slurm Job Status
#' @description Queries the status of a Slurm job.
#' @param slurm_job_id Character string with the Slurm job ID.
#' @return Character string with the job status (PENDING, RUNNING, COMPLETED, FAILED, or UNKNOWN).
#' @keywords internal
check_slurm_job_status <- function(slurm_job_id) {
  # Use squeue to check job status
  result <- suppressWarnings(
    system(paste("squeue -j", slurm_job_id, "-h -o %T"), intern = TRUE, ignore.stderr = TRUE)
  )
  
  if (length(result) == 0) {
    # Job not in queue, check if it completed successfully
    result <- suppressWarnings(
      system(paste("sacct -j", slurm_job_id, "-n -o State"), intern = TRUE, ignore.stderr = TRUE)
    )
    
    if (length(result) == 0) {
      return("UNKNOWN")
    } else {
      # Clean up the returned state
      state <- stringr::str_trim(result[1])
      
      if (grepl("COMPLETED", state, ignore.case = TRUE)) {
        return("COMPLETED")
      } else if (grepl("FAILED|CANCELLED|TIMEOUT", state, ignore.case = TRUE)) {
        return("FAILED")
      } else {
        return("UNKNOWN")
      }
    }
  } else {
    # Job is in the queue, map status
    state <- stringr::str_trim(result[1])
    
    if (grepl("PENDING", state, ignore.case = TRUE)) {
      return("PENDING")
    } else if (grepl("RUNNING", state, ignore.case = TRUE)) {
      return("RUNNING")
    } else {
      return("UNKNOWN")
    }
  }
}

#' @title Cancel Slurm Job
#' @description Cancels a running or pending Slurm job.
#' @param slurm_job_id Character string with the Slurm job ID.
#' @return Logical indicating whether the cancellation was successful.
#' @keywords internal
cancel_slurm_job <- function(slurm_job_id) {
  result <- suppressWarnings(
    system(paste("scancel", slurm_job_id), ignore.stdout = TRUE, ignore.stderr = TRUE)
  )
  
  return(result == 0)
} 