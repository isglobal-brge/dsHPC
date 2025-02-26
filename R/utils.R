#' @title Utility Functions for the dsHPC Package
#' @description This file contains utility functions used throughout the package.

#' @title Create a Hash from Function and Arguments
#' @description Creates a unique hash based on a function definition and its arguments to use for
#' caching and identifying jobs.
#' @param func_or_name Function object, character string representing the name of the function, 
#'   or its string representation from deparse.
#' @param args List of arguments passed to the function.
#' @param object_hash Optional hash of an object being processed (default: NULL).
#' @return A character string containing a unique hash.
#' @keywords internal
create_job_hash <- function(func_or_name, args, object_hash = NULL) {
  # Handle different input types for func_or_name
  if (is.function(func_or_name)) {
    # For function objects, capture the entire function body, environment, and formal arguments
    func_body <- deparse(body(func_or_name))
    func_formals <- capture.output(str(formals(func_or_name)))
    func_env <- environmentName(environment(func_or_name))
    
    # Combine all function components into a comprehensive representation
    func_representation <- list(
      body = func_body,
      formals = func_formals,
      env = func_env
    )
    
    # Convert to a single string
    func_str <- digest::digest(func_representation, algo = "sha256")
  } else if (is.character(func_or_name)) {
    # If it's a vector of strings from deparse, collapse it
    if (length(func_or_name) > 1) {
      func_str <- paste(func_or_name, collapse = "\n")
    } else {
      func_str <- func_or_name
    }
  } else {
    stop("func_or_name must be a function, character string, or vector")
  }
  
  # Serialize the arguments to create a consistent representation
  serialized_args <- list(
    func = func_str,
    args = args,
    object = object_hash
  )
  
  # Create a hash of the serialized data
  hash <- digest::digest(serialized_args, algo = "sha256")
  return(hash)
}

#' @title Generate Unique Job ID
#' @description Creates a unique job ID based on the current timestamp and a random UUID.
#' @return A character string containing a unique job ID.
#' @keywords internal
generate_job_id <- function() {
  # Combine timestamp and UUID for uniqueness
  timestamp <- format(Sys.time(), "%Y%m%d%H%M%S")
  random_id <- uuid::UUIDgenerate(use.time = TRUE)
  job_id <- paste0("dshpc_", timestamp, "_", substring(random_id, 1, 8))
  return(job_id)
}

#' @title Format Function Call for Execution
#' @description Formats a function call and its arguments for execution in an R script.
#' @param func_name Character string representing the name of the function.
#' @param args List of arguments passed to the function.
#' @param package Character string representing the package containing the function (default: NULL).
#' @return A character string containing the formatted function call.
#' @keywords internal
format_function_call <- function(func_name, args, package = NULL) {
  # Format the package::function part
  if (!is.null(package)) {
    func_call <- paste0(package, "::", func_name)
  } else {
    func_call <- func_name
  }
  
  # Format arguments
  arg_strings <- character(0)
  for (arg_name in names(args)) {
    # Handle unnamed arguments
    if (arg_name == "") {
      arg_strings <- c(arg_strings, deparse(args[[arg_name]]))
    } else {
      # For named arguments
      arg_strings <- c(arg_strings, paste0(arg_name, " = ", deparse(args[[arg_name]])))
    }
  }
  
  # Combine function name and arguments
  full_call <- paste0(func_call, "(", paste(arg_strings, collapse = ", "), ")")
  return(full_call)
}

#' @title Escape Shell Arguments
#' @description Safely escapes arguments for shell execution to prevent command injection.
#' @param arg Character string to be escaped.
#' @return A character string with properly escaped shell arguments.
#' @keywords internal
escape_shell_arg <- function(arg) {
  # Basic shell escaping for safety
  escaped <- gsub("'", "'\\''", arg, fixed = TRUE)
  return(paste0("'", escaped, "'"))
}

#' @title Check if Running in DataSHIELD Environment
#' @description Determines if the code is being executed in a DataSHIELD environment.
#' @return Logical indicating whether running in a DataSHIELD environment.
#' @keywords internal
is_datashield_env <- function() {
  # Check for DataSHIELD-specific environment variables or packages
  ds_env <- Sys.getenv("DATASHIELD_ENV", unset = "")
  if (ds_env != "") {
    return(TRUE)
  }
  
  # Check if opal package is available and initialized
  if (requireNamespace("opal", quietly = TRUE)) {
    if (!is.null(getOption("datashield.connections"))) {
      return(TRUE)
    }
  }
  
  return(FALSE)
} 