#' @title Python Integration Functions for dsHPC
#' @description This file contains functions for submitting and managing Python jobs
#' through the reticulate package.

#' @title Submit Python Function
#' @description Submit a Python function to be executed via reticulate.
#' @param py_module Character string with the name of the Python module containing the function.
#' @param py_function Character string with the name of the Python function to execute.
#' @param args List of arguments to pass to the Python function.
#' @param python_path Optional character string with the path to the Python executable.
#' @param virtualenv Optional character string with the name of the virtualenv to use.
#' @param condaenv Optional character string with the name of the conda environment to use.
#' @param slurm_opts Optional list with Slurm-specific options (partition, memory, time, cpus).
#' @param use_cache Logical indicating whether to reuse cached results.
#' @param required_modules Character vector with additional Python modules to import.
#' @return List with job information.
#' @examples
#' \dontrun{
#' # Initialize dsHPC first
#' dsHPC.init()
#' 
#' # Submit a Python job using numpy
#' job <- dsHPC.submit_python(
#'   py_module = "numpy",
#'   py_function = "mean",
#'   args = list(a = c(1, 2, 3, 4, 5))
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
dsHPC.submit_python <- function(py_module, py_function, args = list(), 
                               python_path = NULL, virtualenv = NULL, condaenv = NULL,
                               slurm_opts = list(), use_cache = TRUE,
                               required_modules = NULL) {
  # Validate inputs
  if (!is.character(py_module) || length(py_module) != 1) {
    stop("py_module must be a single character string")
  }
  
  if (!is.character(py_function) || length(py_function) != 1) {
    stop("py_function must be a single character string")
  }
  
  if (!is.list(args)) {
    stop("args must be a list")
  }
  
  # Ensure reticulate is available
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("The reticulate package is required to submit Python jobs. Please install it with install.packages('reticulate').")
  }
  
  # Get configuration
  config <- getOption("dsHPC.config")
  if (is.null(config)) {
    stop("dsHPC has not been initialized. Call dsHPC.init() first.")
  }
  
  # Create a wrapper function that will call the Python function using reticulate
  r_wrapper <- create_python_wrapper(py_module, py_function, args, 
                                   python_path, virtualenv, condaenv, required_modules)
  
  # Try to get the actual Python function definition for better hashing
  python_func_source <- NULL
  try({
    # Set up Python environment if needed
    if (!is.null(python_path)) {
      reticulate::use_python(python_path, required = TRUE)
    }
    
    if (!is.null(virtualenv)) {
      reticulate::use_virtualenv(virtualenv, required = TRUE)
    } else if (!is.null(condaenv)) {
      reticulate::use_condaenv(condaenv, required = TRUE)
    }
    
    # Import the module
    py_mod <- reticulate::import(py_module)
    
    # Get the function from the module
    py_func <- py_mod[[py_function]]
    
    # Try to get source code if possible
    if (reticulate::py_has_attr(py_func, "__code__")) {
      code_obj <- py_func$`__code__`
      if (reticulate::py_has_attr(code_obj, "co_code")) {
        # Hash the bytecode as a representation of the function
        python_func_source <- as.character(code_obj$co_code)
      }
    }
  }, silent = TRUE)
  
  # Calculate job hash for caching
  if (!is.null(python_func_source)) {
    # Use the actual Python function source code if available
    func_info <- paste(py_module, py_function, python_func_source, sep = "::")
    job_hash <- create_job_hash(func_info, args)
  } else {
    # Fall back to module and function name if source not available
    func_info <- paste(py_module, py_function, sep = "::")
    job_hash <- create_job_hash(func_info, args)
  }
  
  # Check if result is already cached
  if (use_cache) {
    cached_result <- get_cached_result(config$connection, job_hash)
    if (!is.null(cached_result)) {
      message("Using cached result for this Python job")
      return(list(
        job_id = uuid::UUIDgenerate(),
        job_hash = job_hash,
        status = "CACHED",
        py_module = py_module,
        py_function = py_function,
        result = cached_result
      ))
    }
  }
  
  # Create a proper function name for the wrapper
  py_wrapper_name <- paste0("python_", py_module, "_", py_function)
  
  # Generate a unique job ID
  job_id <- uuid::UUIDgenerate()
  
  # Define the wrapper as a function in the parent environment
  # This is necessary for the R script to find and execute it
  wrapper_env <- new.env(parent = parent.frame())
  assign(py_wrapper_name, r_wrapper, envir = wrapper_env)
  
  # Store job information in database
  store_job_info(
    config$connection, 
    job_id, 
    job_hash, 
    py_wrapper_name, 
    list(), # Arguments are already packaged in the wrapper
    status = "SUBMITTED"
  )
  
  # Execute the job locally if no scheduler or in simulation mode
  if (!config$scheduler_available || config$simulation_mode) {
    # Execute the wrapper function
    tryCatch({
      # Call the Python function
      result <- r_wrapper()
      
      # Always store the result in the cache using the Python-specific function
      store_python_result(config$connection, job_hash, result)
      
      # Update job status
      update_job_status(config$connection, job_id, "COMPLETED", completed = TRUE)
      
      # Convert Python result to R for return value
      r_result <- reticulate::py_to_r(result)
      
      return(list(
        job_id = job_id,
        job_hash = job_hash,
        status = "COMPLETED",
        py_module = py_module,
        py_function = py_function,
        result = r_result
      ))
    }, error = function(e) {
      update_job_status(config$connection, job_id, "FAILED", 
                       error_message = as.character(e), completed = TRUE)
      stop(e)
    })
  } else {
    # TODO: Implement Slurm job submission for Python jobs
    # For now, we'll simulate execution
    message("Slurm submission for Python jobs is not implemented yet. Executing locally.")
    
    # Execute the wrapper function
    tryCatch({
      # Call the Python function
      result <- r_wrapper()
      
      # Always store the result in the cache using the Python-specific function
      store_python_result(config$connection, job_hash, result)
      
      # Update job status
      update_job_status(config$connection, job_id, "COMPLETED", completed = TRUE)
      
      # Convert Python result to R for return value
      r_result <- reticulate::py_to_r(result)
      
      return(list(
        job_id = job_id,
        job_hash = job_hash,
        status = "COMPLETED",
        py_module = py_module,
        py_function = py_function,
        result = r_result
      ))
    }, error = function(e) {
      update_job_status(config$connection, job_id, "FAILED", 
                       error_message = as.character(e), completed = TRUE)
      stop(e)
    })
  }
}

#' @title Create Python Wrapper Function
#' @description Creates an R function that will execute a Python function via reticulate.
#' @param py_module Character string with the name of the Python module.
#' @param py_function Character string with the name of the Python function.
#' @param args List of arguments to pass to the Python function.
#' @param python_path Optional character string with the path to the Python executable.
#' @param virtualenv Optional character string with the name of the virtualenv to use.
#' @param condaenv Optional character string with the name of the conda environment to use.
#' @param required_modules Character vector with additional Python modules to import.
#' @return An R function that will execute the Python function.
#' @keywords internal
create_python_wrapper <- function(py_module, py_function, args = list(),
                                python_path = NULL, virtualenv = NULL, condaenv = NULL,
                                required_modules = NULL) {
  # Create a wrapper function to execute the Python code
  wrapper <- function() {
    # Set up Python environment
    if (!is.null(python_path)) {
      reticulate::use_python(python_path, required = TRUE)
    }
    
    if (!is.null(virtualenv)) {
      reticulate::use_virtualenv(virtualenv, required = TRUE)
    } else if (!is.null(condaenv)) {
      reticulate::use_condaenv(condaenv, required = TRUE)
    }
    
    # Import required modules first
    if (!is.null(required_modules)) {
      for (module in required_modules) {
        reticulate::import(module)
      }
    }
    
    # Import the target module
    py_mod <- reticulate::import(py_module)
    
    # Get the function from the module
    py_func <- py_mod[[py_function]]
    
    # Call the Python function with arguments
    if (length(args) > 0) {
      result <- do.call(py_func, args)
    } else {
      result <- py_func()
    }
    
    return(result)
  }
  
  return(wrapper)
}

#' @title Create Python Script for Job Execution
#' @description Creates a Python script for executing a function from a module.
#' @param py_module Character string with the name of the Python module.
#' @param py_function Character string with the name of the Python function.
#' @param args List of arguments to pass to the Python function.
#' @param result_file Character string with the path where the result should be saved.
#' @param python_path Optional character string with the path to the Python executable.
#' @param virtualenv Optional character string with the name of the virtualenv to use.
#' @param condaenv Optional character string with the name of the conda environment to use.
#' @param required_modules Character vector with additional Python modules to import.
#' @return Path to the created Python script.
#' @keywords internal
create_python_script <- function(py_module, py_function, args, result_file,
                              python_path = NULL, virtualenv = NULL, condaenv = NULL,
                              required_modules = NULL) {
  # Create a temporary file for the Python script
  py_script <- tempfile(pattern = "dsHPC_python_job_", fileext = ".py")
  
  # Start building the script content
  script_content <- c(
    "#!/usr/bin/env python",
    "# Python script created by dsHPC for executing a function in an HPC job",
    "",
    "import sys",
    "import pickle",
    "import traceback"
  )
  
  # Add imports for required modules
  if (!is.null(required_modules)) {
    script_content <- c(script_content, "", "# Load required modules")
    for (module in required_modules) {
      script_content <- c(script_content, paste0("import ", module))
    }
  }
  
  # Add import for the target module
  script_content <- c(
    script_content,
    "",
    "# Import the target module",
    paste0("import ", py_module)
  )
  
  # Format the function arguments as Python code
  arg_lines <- character(0)
  for (arg_name in names(args)) {
    # Convert R arguments to Python format
    py_value <- to_python_arg(args[[arg_name]])
    
    if (arg_name == "") {
      # Unnamed argument
      arg_lines <- c(arg_lines, py_value)
    } else {
      # Named argument
      arg_lines <- c(arg_lines, paste0(arg_name, "=", py_value))
    }
  }
  
  # Join arguments with commas
  if (length(arg_lines) > 0) {
    args_str <- paste(arg_lines, collapse = ", ")
  } else {
    args_str <- ""
  }
  
  # Add the function call and result saving code
  script_content <- c(
    script_content,
    "",
    "try:",
    paste0("    # Call: ", py_module, ".", py_function, "(", args_str, ")"),
    paste0("    result = ", py_module, ".", py_function, "(", args_str, ")"),
    "",
    "    # Save the result to file",
    paste0("    with open('", result_file, "', 'wb') as f:"),
    "        pickle.dump(result, f)",
    "",
    "except Exception as e:",
    "    # Save the error information in case of failure",
    paste0("    with open('", result_file, "', 'wb') as f:"),
    "        error_info = {",
    "            'error': str(e),",
    "            'traceback': traceback.format_exc()",
    "        }",
    "        pickle.dump(error_info, f)",
    "    sys.exit(1)"
  )
  
  # Write the script to file
  writeLines(script_content, py_script)
  
  # Make the script executable
  system(paste("chmod +x", py_script))
  
  return(py_script)
}

#' @title Convert R Object to Python-compatible String Representation
#' @description Converts an R object to a string that can be used in a Python script.
#' @param arg The R object to convert.
#' @return A character string with the Python representation of the object.
#' @keywords internal
to_python_arg <- function(arg) {
  if (is.null(arg)) {
    return("None")
  } else if (is.logical(arg)) {
    if (length(arg) == 1) {
      return(ifelse(arg, "True", "False"))
    } else {
      # For logical vectors, convert to Python list of booleans
      bool_values <- ifelse(arg, "True", "False")
      return(paste0("[", paste(bool_values, collapse = ", "), "]"))
    }
  } else if (is.numeric(arg)) {
    if (length(arg) == 1) {
      # Single numeric value
      return(as.character(arg))
    } else {
      # Numeric vector -> Python list
      return(paste0("[", paste(arg, collapse = ", "), "]"))
    }
  } else if (is.character(arg)) {
    if (length(arg) == 1) {
      # Single string -> quoted Python string
      escaped <- gsub("'", "\\\\'", arg)
      return(paste0("'", escaped, "'"))
    } else {
      # Character vector -> Python list of strings
      escaped <- sapply(arg, function(s) {
        s <- gsub("'", "\\\\'", s)
        paste0("'", s, "'")
      })
      return(paste0("[", paste(escaped, collapse = ", "), "]"))
    }
  } else if (is.list(arg)) {
    # Convert list to Python dict or list
    if (is.null(names(arg))) {
      # Unnamed list -> Python list
      items <- sapply(arg, to_python_arg)
      return(paste0("[", paste(items, collapse = ", "), "]"))
    } else {
      # Named list -> Python dict
      items <- character(0)
      for (name in names(arg)) {
        key <- paste0("'", gsub("'", "\\\\'", name), "'")
        value <- to_python_arg(arg[[name]])
        items <- c(items, paste0(key, ": ", value))
      }
      return(paste0("{", paste(items, collapse = ", "), "}"))
    }
  } else if (is.data.frame(arg)) {
    # Convert data frame to a dictionary of lists
    df_dict <- lapply(arg, function(col) {
      to_python_arg(as.list(col))
    })
    
    # Create Python dictionary representation
    items <- character(0)
    for (name in names(df_dict)) {
      key <- paste0("'", gsub("'", "\\\\'", name), "'")
      value <- df_dict[[name]]
      items <- c(items, paste0(key, ": ", value))
    }
    
    return(paste0("{", paste(items, collapse = ", "), "}"))
  } else {
    # Default for other types - not ideal but prevents errors
    warning("Unsupported type for Python conversion: ", class(arg)[1])
    return("None  # Unsupported type")
  }
} 