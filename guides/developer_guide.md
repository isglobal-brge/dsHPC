# dsHPC Developer Integration Guide

This guide is for DataSHIELD package developers who want to integrate the dsHPC system into their packages. It provides information on how to use dsHPC to submit computationally intensive tasks to HPC resources, while maintaining compatibility with the DataSHIELD framework.

## Table of Contents

1. [Overview](#overview)
2. [Setting Up Dependencies](#setting-up-dependencies)
3. [Integrating with Your Package](#integrating-with-your-package)
4. [Job Submission Patterns](#job-submission-patterns)
5. [Intelligent Function Caching](#intelligent-function-caching)
6. [Handling Results](#handling-results)
7. [Python Integration](#python-integration)
8. [Error Handling](#error-handling)
9. [Best Practices](#best-practices)

## Overview

dsHPC allows DataSHIELD packages to offload computationally intensive tasks to HPC resources, with support for:

- Running R functions on HPC nodes
- Executing Python code and scripts
- Caching results to avoid redundant computations
- Job status monitoring and result retrieval

The system is designed to work in two modes:
1. With an actual Slurm scheduler (in a real HPC environment)
2. In local execution mode (when scheduler is not available)

## Setting Up Dependencies

### Add dsHPC as a Dependency

In your package's `DESCRIPTION` file:

```
Imports:
    dsHPC
```

In your `NAMESPACE` file:

```
import(dsHPC)
# OR
importFrom(dsHPC, dsHPC.init, dsHPC.submit, dsHPC.result, dsHPC.status)
```

### Initialize dsHPC in Your Package

When your package loads, you should initialize dsHPC. Here's a recommended pattern:

```r
.onLoad <- function(libname, pkgname) {
  # Initialize dsHPC if not already initialized
  if (is.null(getOption("dsHPC.config"))) {
    tryCatch({
      dsHPC::dsHPC.init()
    }, error = function(e) {
      warning("Failed to initialize dsHPC: ", e$message)
    })
  }
}
```

## Integrating with Your Package

### Basic Pattern for Computationally Intensive Functions

```r
#' My computationally intensive function
#' 
#' @param data The data to process
#' @param param1 First parameter
#' @param param2 Second parameter
#' @return Processed results
#' @export
myPackage.intensiveFunction <- function(data, param1, param2) {
  # Check if dsHPC is available
  if (requireNamespace("dsHPC", quietly = TRUE) && !is.null(getOption("dsHPC.config"))) {
    # Define the actual computation function
    computation_function <- function(data, param1, param2) {
      # Actual computation code goes here
      result <- # ... compute something intensive
      return(result)
    }
    
    # Submit the job
    job <- dsHPC::dsHPC.submit(
      func = computation_function,
      args = list(
        data = data,
        param1 = param1,
        param2 = param2
      ),
      use_cache = TRUE  # Leverage caching
    )
    
    # Check if the job completed immediately
    if (job$status == "COMPLETED" || job$status == "CACHED") {
      return(job$result)
    } else {
      # For a synchronous approach, wait for completion
      return(dsHPC::dsHPC.result(job$job_id, wait_for_completion = TRUE))
    }
  } else {
    # Fallback to regular processing if dsHPC is not available
    # Actual computation code goes here
    result <- # ... compute something intensive
    return(result)
  }
}
```

### For Methods That Need Multiple Steps

```r
#' Multi-step analysis function
#' 
#' @param data The data to analyze
#' @param params Analysis parameters
#' @return Analysis results
#' @export
myPackage.multiStepAnalysis <- function(data, params) {
  # Step 1: Submit preprocessing job
  preprocess_job <- dsHPC::dsHPC.submit(
    func = preprocess_function,
    args = list(data = data, params = params),
    use_cache = TRUE
  )
  
  # Wait for preprocessing to complete
  preprocess_result <- wait_for_job(preprocess_job)
  
  # Step 2: Submit main analysis job using preprocessed data
  analysis_job <- dsHPC::dsHPC.submit(
    func = analysis_function,
    args = list(
      preprocessed_data = preprocess_result,
      params = params
    ),
    use_cache = TRUE
  )
  
  # Wait for analysis to complete
  analysis_result <- wait_for_job(analysis_job)
  
  # Step 3: Submit post-processing job
  postprocess_job <- dsHPC::dsHPC.submit(
    func = postprocess_function,
    args = list(
      analysis_result = analysis_result,
      params = params
    ),
    use_cache = TRUE
  )
  
  # Return final result
  return(wait_for_job(postprocess_job))
}

# Helper function to wait for job completion
wait_for_job <- function(job) {
  if (job$status == "COMPLETED" || job$status == "CACHED") {
    return(job$result)
  }
  
  while (TRUE) {
    status <- dsHPC::dsHPC.status(job$job_id, return_result = TRUE)
    if (status$status == "COMPLETED" || status$status == "CACHED") {
      return(status$result)
    } else if (status$status == "FAILED") {
      stop("Job failed: ", status$error_message)
    }
    Sys.sleep(2)
  }
}
```

## Job Submission Patterns

### Using Function References vs. Names

You can submit jobs in two ways:

1. Using function references (preferred for internal functions):

```r
# Using function reference
job <- dsHPC::dsHPC.submit(
  func = my_calculation_function,
  args = list(data = data, params = params)
)
```

2. Using function names (useful for functions from other packages):

```r
# Using function name
job <- dsHPC::dsHPC.submit_by_name(
  func_name = "randomForest",
  args = list(x = data$x, y = data$y, ntree = 500),
  package = "randomForest"
)
```

### Specifying Slurm Parameters

For computationally intensive jobs, you might want to customize Slurm parameters:

```r
job <- dsHPC::dsHPC.submit(
  func = my_intensive_function,
  args = list(data = large_dataset, params = params),
  slurm_opts = list(
    partition = "compute",  # Specify compute partition
    memory = "32G",         # Request 32GB memory
    time = "08:00:00",      # Request 8 hours
    cpus = 8                # Request 8 CPUs
  )
)
```

## Intelligent Function Caching

dsHPC includes a sophisticated function caching system that identifies jobs based on their actual function implementation, not just the function name. This helps avoid redundant computations and improves efficiency.

### How Function Hashing Works

When a job is submitted, dsHPC creates a unique hash based on:

1. The function's body (actual code implementation)
2. The function's formal arguments
3. The function's environment
4. The exact arguments passed to the function

This allows the system to identify functionally equivalent code, even if it's formatted differently or comes from different sources.

### Leveraging the Cache in Your Code

Always use the `use_cache = TRUE` parameter (default) in your job submissions to benefit from this system:

```r
job <- dsHPC::dsHPC.submit(
  func = my_function,
  args = list(x = data),
  use_cache = TRUE  # This is the default, but shown here for clarity
)
```

### Smart Function Wrapping

To maximize caching efficiency across different parts of your code, consider using factory functions that produce computational functions with consistent implementations:

```r
# Factory function that produces a computation function
create_computation <- function(method = "default") {
  # Return a specific implementation based on the method
  if (method == "fast") {
    return(function(x) {
      # Fast implementation
      result <- # ...
      return(result)
    })
  } else {
    return(function(x) {
      # Default implementation
      result <- # ...
      return(result)
    })
  }
}

# Use the factory to create consistent functions
fast_compute <- create_computation("fast")
default_compute <- create_computation("default")

# These submissions will correctly use the cache when appropriate
job1 <- dsHPC::dsHPC.submit(func = fast_compute, args = list(x = data1))
job2 <- dsHPC::dsHPC.submit(func = fast_compute, args = list(x = data1)) # Uses cache
job3 <- dsHPC::dsHPC.submit(func = fast_compute, args = list(x = data2)) # New computation
job4 <- dsHPC::dsHPC.submit(func = default_compute, args = list(x = data1)) # New computation (different function)
```

## Handling Results

### Synchronous vs. Asynchronous

Choose the pattern based on your needs:

1. **Synchronous Pattern**: Wait for the job to complete and return the result

```r
result <- myPackage.synchronousFunction(data, params)
# Result is immediately available
```

2. **Asynchronous Pattern**: Submit job and let user check status/retrieve results later

```r
# Submit function
job_id <- myPackage.submitAnalysis(data, params)

# Later, in another function call:
status <- myPackage.checkAnalysisStatus(job_id)

# And when complete:
result <- myPackage.getAnalysisResult(job_id)
```

### Caching Strategy

The caching system of dsHPC automatically caches results based on the function and arguments. You can:

- Use `use_cache = TRUE` (default) to leverage cached results
- Use `use_cache = FALSE` to force recomputation
- Use `cache_only = TRUE` to check if a result is cached without submitting a job

Example:

```r
# Function that allows user to control caching behavior
myPackage.analyzeData <- function(data, params, use_cache = TRUE, force_recompute = FALSE) {
  job <- dsHPC::dsHPC.submit(
    func = actual_analysis_function,
    args = list(data = data, params = params),
    use_cache = use_cache && !force_recompute
  )
  
  # Wait for and return result
  return(wait_for_job(job))
}
```

## Python Integration

### Using Python Functions

If your package uses Python functionality via reticulate, you can submit Python jobs:

```r
#' Analyze data using a Python algorithm
#' 
#' @param data The data to analyze
#' @param method The analysis method
#' @return Analysis results
#' @export
myPackage.pythonAnalysis <- function(data, method = "pca") {
  # Check if dsHPC and reticulate are available
  if (requireNamespace("dsHPC", quietly = TRUE) && 
      requireNamespace("reticulate", quietly = TRUE)) {
    
    # Submit a Python job
    job <- dsHPC::dsHPC.submit_python(
      py_module = "sklearn.decomposition",
      py_function = "PCA",
      args = list(
        n_components = 2
      ),
      python_code = sprintf('
# Import necessary libraries
import numpy as np
from sklearn.decomposition import PCA

# Convert R data to numpy array
data = r.data
print("Analyzing data with shape:", data.shape)

# Create and fit PCA model
pca = PCA(n_components=2)
transformed = pca.fit_transform(data)
explained_variance = pca.explained_variance_ratio_

# Return results as a dictionary
result = {
  "transformed": transformed,
  "explained_variance": explained_variance,
  "components": pca.components_
}
      ')
    )
    
    # Wait for and return result
    return(wait_for_job(job))
  } else {
    stop("dsHPC or reticulate not available")
  }
}
```

### Using Custom Python Modules

If your package includes custom Python modules, you can use them with dsHPC:

1. Place your Python modules in the `inst/python` directory of your package
2. Create an initialization function that adds these modules to the Python path:

```r
# In R/init.R
init_python_modules <- function() {
  if (requireNamespace("reticulate", quietly = TRUE)) {
    # Get path to package Python modules
    module_path <- system.file("python", package = "myPackage")
    
    # Add to Python path
    reticulate::py_run_string(paste0("import sys; sys.path.append('", module_path, "')"))
    
    return(TRUE)
  }
  return(FALSE)
}
```

3. Use your modules in dsHPC submissions:

```r
#' Run custom Python analysis
#' 
#' @param data The data to analyze
#' @return Analysis results
#' @export
myPackage.customPythonAnalysis <- function(data) {
  # Initialize Python modules
  init_python_modules()
  
  # Submit job using custom module
  job <- dsHPC::dsHPC.submit_python(
    py_module = "mypackage_analysis",  # Your custom module name
    py_function = "run_analysis",      # Function in your module
    args = list(
      data = data,
      params = list(iterations = 100, threshold = 0.01)
    )
  )
  
  return(wait_for_job(job))
}
```

## Error Handling

When using dsHPC, there are several types of errors to handle:

1. **Initialization errors** - if dsHPC can't initialize:

```r
tryCatch({
  dsHPC::dsHPC.init()
}, error = function(e) {
  warning("dsHPC initialization failed, will compute locally: ", e$message)
  options(myPackage.use_local_compute = TRUE)
})
```

2. **Job submission errors**:

```r
job <- tryCatch({
  dsHPC::dsHPC.submit(func = analysis_function, args = list(data = data))
}, error = function(e) {
  warning("Job submission failed: ", e$message)
  # Return a custom error structure
  return(list(status = "SUBMISSION_FAILED", error = e$message))
})

if (job$status == "SUBMISSION_FAILED") {
  # Handle submission failure
}
```

3. **Job execution errors**:

```r
status <- dsHPC::dsHPC.status(job$job_id)
if (status$status == "FAILED") {
  # Handle job failure
  warning("Job failed: ", status$error_message)
  # Maybe try an alternative computation approach
}
```

## Best Practices

1. **Always check for dsHPC availability**:

```r
if (requireNamespace("dsHPC", quietly = TRUE) && !is.null(getOption("dsHPC.config"))) {
  # Use dsHPC
} else {
  # Fall back to local computation
}
```

2. **Provide fallback implementations**:

```r
myPackage.analyze <- function(data, params) {
  if (can_use_dsHPC()) {
    # Submit to HPC
    return(submit_to_hpc(data, params))
  } else {
    # Compute locally
    return(compute_locally(data, params))
  }
}
```

3. **Handle large data efficiently**:

```r
# For large data, consider saving to disk instead of passing directly
temp_file <- tempfile(fileext = ".rds")
saveRDS(large_data, temp_file)

job <- dsHPC::dsHPC.submit(
  func = function(data_file) {
    # Load data inside the function
    data <- readRDS(data_file)
    # Process data
    result <- process_data(data)
    return(result)
  },
  args = list(data_file = temp_file)
)
```

4. **Use appropriate timeouts**:

```r
# For potentially long-running jobs, implement timeouts
result <- with_timeout(wait_for_job(job), timeout = 3600)  # 1-hour timeout
```

5. **Document HPC requirements**:

Make sure to document the hardware/software requirements for your package, including:

- Required R and Python versions
- Required system libraries
- Memory/CPU recommendations
- Expected runtime for different data sizes

By following these patterns, you can integrate dsHPC seamlessly into your DataSHIELD packages, providing high-performance computing capabilities while maintaining compatibility with the DataSHIELD framework. 