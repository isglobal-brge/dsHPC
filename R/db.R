#' @title Database Management Functions for dsHPC
#' @description This file contains functions for interacting with the SQLite database
#' that stores job information and results for caching purposes.

#' @title Initialize Database Connection
#' @description Creates and initializes the SQLite database for storing job information.
#'   This function ensures that the database filename contains "dsHPC" to avoid
#'   conflicts with other SQLite databases. If a custom path is provided without
#'   "dsHPC" in the name, it will be renamed and a warning will be issued.
#' @param db_path Character string specifying the path to the SQLite database file.
#'   If NULL (default), uses the package's default location at ~/.dsHPC/dsHPC.sqlite.
#' @return A DBI connection object to the database.
#' @keywords internal
init_db_connection <- function(db_path = NULL) {
  if (is.null(db_path)) {
    # Use a default location in the user's home directory
    db_dir <- file.path(Sys.getenv("HOME"), ".dsHPC")
    if (!dir.exists(db_dir)) {
      dir.create(db_dir, recursive = TRUE)
    }
    db_path <- file.path(db_dir, "dsHPC.sqlite")
  } else {
    # If a custom path is provided, ensure it has dsHPC in the name to avoid conflicts
    db_name <- basename(db_path)
    if (!grepl("dsHPC", db_name, ignore.case = TRUE)) {
      # Replace the filename with one that includes dsHPC
      db_dir <- dirname(db_path)
      file_ext <- tools::file_ext(db_path)
      if (file_ext != "") {
        new_name <- paste0("dsHPC.", file_ext)
      } else {
        new_name <- "dsHPC.sqlite"
      }
      db_path <- file.path(db_dir, new_name)
      warning("Database name changed to ", new_name, " to avoid conflicts")
    }
  }
  
  # Create connection to the database
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  
  # Create tables if they don't exist
  if (!DBI::dbExistsTable(con, "jobs")) {
    DBI::dbExecute(con, "
      CREATE TABLE jobs (
        job_id TEXT PRIMARY KEY,
        job_hash TEXT NOT NULL,
        function_name TEXT NOT NULL,
        args TEXT NOT NULL,
        object_hash TEXT,
        status TEXT NOT NULL,
        submission_time TIMESTAMP NOT NULL,
        completion_time TIMESTAMP,
        scheduler_id TEXT,
        error_message TEXT
      )
    ")
    
    # Create an index on job_hash for fast lookups
    DBI::dbExecute(con, "CREATE INDEX idx_job_hash ON jobs(job_hash)")
  }
  
  if (!DBI::dbExistsTable(con, "results")) {
    DBI::dbExecute(con, "
      CREATE TABLE results (
        job_hash TEXT PRIMARY KEY,
        result_data BLOB,
        result_type TEXT NOT NULL,
        cache_time TIMESTAMP NOT NULL
      )
    ")
  }
  
  return(con)
}

#' @title Check if Job Result is Cached
#' @description Checks if a result for a specific job hash exists in the cache.
#' @param con DBI connection object to the SQLite database.
#' @param job_hash Character string with the job hash to check.
#' @return Logical indicating whether the job result is cached.
#' @keywords internal
is_result_cached <- function(con, job_hash) {
  query <- sprintf("SELECT 1 FROM results WHERE job_hash = '%s' LIMIT 1", job_hash)
  result <- DBI::dbGetQuery(con, query)
  return(nrow(result) > 0)
}

#' @title Store Job Information
#' @description Stores job details in the jobs table.
#' @param con DBI connection object to the SQLite database.
#' @param job_id Character string with the unique job ID.
#' @param job_hash Character string with the hash of the job (function + args).
#' @param function_name Character string with the name of the function being executed.
#' @param args List of arguments (will be serialized to JSON).
#' @param object_hash Optional character string with the hash of an object being processed.
#' @param status Character string with the current job status.
#' @param scheduler_id Optional character string with the ID assigned by the scheduler.
#' @return Logical indicating success.
#' @keywords internal
store_job_info <- function(con, job_id, job_hash, function_name, args, 
                           object_hash = NULL, status = "SUBMITTED", scheduler_id = NULL) {
  # Serialize arguments to JSON
  args_json <- jsonlite::toJSON(args, auto_unbox = TRUE)
  
  # Prepare query
  query <- sprintf("
    INSERT INTO jobs 
    (job_id, job_hash, function_name, args, object_hash, status, submission_time, scheduler_id)
    VALUES ('%s', '%s', '%s', '%s', %s, '%s', datetime('now'), %s)
  ", 
  job_id, 
  job_hash, 
  function_name,
  args_json,
  ifelse(is.null(object_hash), "NULL", paste0("'", object_hash, "'")),
  status,
  ifelse(is.null(scheduler_id), "NULL", paste0("'", scheduler_id, "'")))
  
  # Execute query
  result <- DBI::dbExecute(con, query)
  return(result > 0)
}

#' @title Update Job Status
#' @description Updates the status of a job in the database.
#' @param con DBI connection object to the SQLite database.
#' @param job_id Character string with the unique job ID.
#' @param status Character string with the new status.
#' @param scheduler_id Optional character string with the ID assigned by the scheduler.
#' @param error_message Optional character string with error information if the job failed.
#' @param completed Logical indicating if the job completed (sets completion_time).
#' @return Logical indicating success.
#' @keywords internal
update_job_status <- function(con, job_id, status, scheduler_id = NULL, 
                             error_message = NULL, completed = FALSE) {
  # Build query parts
  query_parts <- sprintf("UPDATE jobs SET status = '%s'", status)
  
  if (!is.null(scheduler_id)) {
    query_parts <- paste0(query_parts, sprintf(", scheduler_id = '%s'", scheduler_id))
  }
  
  if (!is.null(error_message)) {
    # Sanitize error message
    error_message <- gsub("'", "''", error_message)
    query_parts <- paste0(query_parts, sprintf(", error_message = '%s'", error_message))
  }
  
  if (completed) {
    query_parts <- paste0(query_parts, ", completion_time = datetime('now')")
  }
  
  # Complete the query
  query <- paste0(query_parts, sprintf(" WHERE job_id = '%s'", job_id))
  
  # Execute query
  result <- DBI::dbExecute(con, query)
  return(result > 0)
}

#' @title Store Job Result
#' @description Stores a job result in the cache.
#' @param con DBI connection object to the SQLite database.
#' @param job_hash Character string with the hash of the job.
#' @param result The R object containing the result.
#' @return Logical indicating success.
#' @keywords internal
store_job_result <- function(con, job_hash, result) {
  # Serialize the result and convert to base64
  serialized_result <- serialize(result, NULL)
  base64_result <- base64enc::base64encode(serialized_result)
  result_type <- class(result)[1]
  
  # Check if result already exists
  if (is_result_cached(con, job_hash)) {
    query <- "
      UPDATE results 
      SET result_data = ?, result_type = ?, cache_time = datetime('now')
      WHERE job_hash = ?
    "
    # Execute query with parameters
    result <- DBI::dbExecute(con, query, 
                            params = list(
                              base64_result,
                              result_type,
                              job_hash
                            ))
  } else {
    query <- "
      INSERT INTO results 
      (job_hash, result_data, result_type, cache_time)
      VALUES (?, ?, ?, datetime('now'))
    "
    # Execute query with parameters
    result <- DBI::dbExecute(con, query, 
                            params = list(
                              job_hash,
                              base64_result,
                              result_type
                            ))
  }
  
  return(result > 0)
}

#' @title Store Python Job Result
#' @description Stores a Python job result in the cache, handling conversion from Python objects.
#' @param con DBI connection object to the SQLite database.
#' @param job_hash Character string with the hash of the job.
#' @param result The Python object containing the result.
#' @return Logical indicating success.
#' @keywords internal
store_python_result <- function(con, job_hash, result) {
  # Convert Python object to R
  if (requireNamespace("reticulate", quietly = TRUE)) {
    # First convert the Python object to R
    r_result <- reticulate::py_to_r(result)
    
    # Store the converted R object in the cache
    return(store_job_result(con, job_hash, r_result))
  } else {
    warning("reticulate package not available, storing Python result as is.")
    return(store_job_result(con, job_hash, result))
  }
}

#' @title Retrieve Job Result
#' @description Retrieves a cached job result from the database.
#' @param con DBI connection object to the SQLite database.
#' @param job_hash Character string with the hash of the job.
#' @return The cached R object if found, or NULL if not cached.
#' @keywords internal
get_cached_result <- function(con, job_hash) {
  if (!is_result_cached(con, job_hash)) {
    return(NULL)
  }
  
  query <- sprintf("SELECT result_data FROM results WHERE job_hash = '%s'", job_hash)
  result_data <- DBI::dbGetQuery(con, query)
  
  if (nrow(result_data) == 0) {
    return(NULL)
  }
  
  # Decode base64 and deserialize the result
  decoded_data <- base64enc::base64decode(result_data$result_data[[1]])
  result <- unserialize(decoded_data)
  return(result)
}

#' @title Get Job Information
#' @description Retrieves information about a specific job.
#' @param con DBI connection object to the SQLite database.
#' @param job_id Character string with the job ID.
#' @return A data frame with job information or NULL if not found.
#' @keywords internal
get_job_info <- function(con, job_id) {
  query <- sprintf("SELECT * FROM jobs WHERE job_id = '%s'", job_id)
  job_info <- DBI::dbGetQuery(con, query)
  
  if (nrow(job_info) == 0) {
    return(NULL)
  }
  
  # Parse JSON args
  job_info$args <- jsonlite::fromJSON(job_info$args)
  
  return(job_info)
}

#' @title Find Jobs by Hash
#' @description Finds jobs with a specific hash value.
#' @param con DBI connection object to the SQLite database.
#' @param job_hash Character string with the job hash.
#' @return A data frame with matching jobs or empty data frame if none found.
#' @keywords internal
find_jobs_by_hash <- function(con, job_hash) {
  query <- sprintf("SELECT * FROM jobs WHERE job_hash = '%s' ORDER BY submission_time DESC", job_hash)
  jobs <- DBI::dbGetQuery(con, query)
  
  if (nrow(jobs) > 0) {
    # Parse JSON args for each job
    jobs$args <- lapply(jobs$args, jsonlite::fromJSON)
  }
  
  return(jobs)
}

#' @title Get Active Jobs
#' @description Retrieves information about all currently active jobs.
#' @param con DBI connection object to the SQLite database.
#' @return A data frame with active job information.
#' @keywords internal
get_active_jobs <- function(con) {
  query <- "SELECT * FROM jobs WHERE status IN ('SUBMITTED', 'RUNNING') ORDER BY submission_time"
  active_jobs <- DBI::dbGetQuery(con, query)
  
  if (nrow(active_jobs) > 0) {
    # Parse JSON args for each job
    active_jobs$args <- lapply(active_jobs$args, jsonlite::fromJSON)
  }
  
  return(active_jobs)
}

#' @title Clean Old Results
#' @description Removes cached results older than a specified time period.
#' @param con DBI connection object to the SQLite database.
#' @param days_to_keep Integer specifying how many days of cache to retain (default: 30).
#' @return The number of records deleted.
#' @keywords internal
clean_old_results <- function(con, days_to_keep = 30) {
  # Only delete results that aren't referenced by active jobs
  query <- sprintf("
    DELETE FROM results 
    WHERE cache_time < datetime('now', '-%d days')
    AND job_hash NOT IN (SELECT job_hash FROM jobs WHERE status IN ('SUBMITTED', 'RUNNING'))
  ", days_to_keep)
  
  deleted <- DBI::dbExecute(con, query)
  return(deleted)
} 