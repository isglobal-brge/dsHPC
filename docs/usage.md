# dsHPC Usage Guide

This guide provides examples and explanations for using the dsHPC package, a DataSHIELD Interface for High-Performance Computing that allows R and Python code to be executed on HPC resources.

## Table of Contents

1. [Installation](#installation)
2. [Initialization](#initialization)
3. [Basic R Job Submission](#basic-r-job-submission)
4. [Working with Python](#working-with-python)
   - [Submitting Python Functions](#submitting-python-functions)
   - [Running Python Code Directly](#running-python-code-directly)
   - [Using Custom Python Modules](#using-custom-python-modules)
   - [Image Processing with Pillow](#image-processing-with-pillow)
5. [Job Management](#job-management)
   - [Checking Job Status](#checking-job-status)
   - [Retrieving Results](#retrieving-results)
   - [Listing Jobs](#listing-jobs)
   - [Cancelling Jobs](#cancelling-jobs)
6. [Caching System](#caching-system)
   - [Using the Cache](#using-the-cache)
   - [Cache Management](#cache-management)
7. [Advanced Features](#advanced-features)
   - [Slurm Integration](#slurm-integration)
   - [Custom Configuration](#custom-configuration)

## Installation

You can install dsHPC directly from GitHub:

```r
devtools::install_github("isglobal-brge/dsHPC")
```

## Initialization

Before using dsHPC, you need to initialize it:

```r
library(dsHPC)

# Initialize with default settings
dsHPC.init()

# Check if the initialization was successful
config <- getOption("dsHPC.config")
print(paste("Scheduler available:", config$scheduler_available))

# You can also initialize with a custom database path
dsHPC.init(db_path = "/path/to/custom/dsHPC.sqlite")
```

## Basic R Job Submission

### Submitting a Simple R Function

```r
# Initialize dsHPC first
dsHPC.init()

# Submit a basic calculation
job <- dsHPC.submit(
  func = mean,
  args = list(x = c(1, 2, 3, 4, 5))
)

# Print job information
print(job)

# If the job completed immediately (in simulation mode), you can access the result
if (job$status == "COMPLETED") {
  print(paste("Result:", job$result))
}
```

### Submitting a Function from a Package

```r
# Submit a job using a function from a package
job <- dsHPC.submit(
  func = kmeans,
  args = list(x = matrix(rnorm(100), ncol = 2), centers = 3),
  # Optional: specify the package if it's not already loaded
  package = "stats"
)
```

### Submitting by Function Name

```r
# Submit a job using a function name
job <- dsHPC.submit_by_name(
  func_name = "mean",
  args = list(x = c(1, 2, 3, 4, 5))
)

# For package functions
job <- dsHPC.submit_by_name(
  func_name = "kmeans",
  args = list(x = matrix(rnorm(100), ncol = 2), centers = 3),
  package = "stats"
)
```

## Working with Python

### Submitting Python Functions

```r
# Initialize Python
library(reticulate)

# Submit a job using a Python function from a module
job <- dsHPC.submit_python(
  py_module = "numpy",
  py_function = "mean",
  args = list(a = c(1, 2, 3, 4, 5))
)

# Check the result
if (job$status == "COMPLETED") {
  print(paste("Mean calculated by NumPy:", job$result))
}
```

### Running Python Code Directly

```r
# Submit arbitrary Python code
job <- dsHPC.submit_python(
  python_code = "
import numpy as np
def calculate_stats(data):
    return {
        'mean': np.mean(data),
        'median': np.median(data),
        'std_dev': np.std(data)
    }
result = calculate_stats([1, 2, 3, 4, 5])
"
)

# Access the result (a Python dictionary converted to an R list)
if (job$status == "COMPLETED") {
  print(paste("Mean:", job$result$mean))
  print(paste("Median:", job$result$median))
  print(paste("Standard Deviation:", job$result$std_dev))
}
```

### Using Custom Python Modules

You can use custom Python modules with dsHPC. First, ensure your Python module is accessible in the Python path:

```r
# Add a directory with your custom module to Python's path
custom_module_path <- "/path/to/your/module"
reticulate::py_run_string(paste0("import sys; sys.path.append('", custom_module_path, "')"))

# Now you can submit jobs using functions from your custom module
job <- dsHPC.submit_python(
  py_module = "my_custom_module",
  py_function = "my_function",
  args = list(param1 = "value", param2 = 42)
)
```

Example with a custom analytics module:

```r
# Assuming you have a module named "data_analytics.py" with a function "analyze"
job <- dsHPC.submit_python(
  py_module = "data_analytics",
  py_function = "analyze",
  args = list(
    data = read.csv("data.csv"),
    columns = c("age", "income", "education"),
    method = "correlation"
  )
)
```

### Image Processing with Pillow

dsHPC provides built-in functions for image processing using Python's Pillow library:

```r
# Create a test image
image_job <- dsHPC.create_test_image(
  output_path = "test_image.png",
  width = 400,
  height = 300,
  color = c(255, 0, 0)  # Red image
)

# Get information about an image
info_job <- dsHPC.get_image_info(
  image_path = "test_image.png"
)

# Print image information
if (info_job$status == "COMPLETED") {
  print(paste("Width:", info_job$result$width))
  print(paste("Height:", info_job$result$height))
  print(paste("Format:", info_job$result$format))
  print(paste("File size:", info_job$result$file_size, "bytes"))
}

# Apply a filter to an image
filter_job <- dsHPC.apply_filter(
  image_path = "test_image.png",
  filter_type = "blur",
  output_path = "blurred_image.png",
  filter_params = list(radius = 5)
)

# List of available filter types:
# - "blur" - Gaussian blur
# - "sharpen" - Sharpening filter
# - "contour" - Contour filter
# - "edge_enhance" - Edge enhancement
# - "emboss" - Emboss filter
# - "grayscale" - Convert to grayscale
# - "sepia" - Apply sepia tone
# - "invert" - Invert colors
```

## Job Management

### Checking Job Status

```r
# Submit a job
job <- dsHPC.submit(
  func = kmeans,
  args = list(x = matrix(rnorm(1000), ncol = 2), centers = 5),
  package = "stats"
)

# Check job status
status <- dsHPC.status(job$job_id)
print(status)

# Check status and get result if available
result_status <- dsHPC.status(job$job_id, return_result = TRUE)
if (result_status$status == "COMPLETED") {
  print("Job completed!")
  print(result_status$result)
}
```

### Retrieving Results

```r
# Submit a job
job <- dsHPC.submit(
  func = sum,
  args = list(x = 1:100)
)

# Retrieve result when job is complete
result <- dsHPC.result(job$job_id)
print(result)

# For long-running jobs, you might want to wait for completion
status <- dsHPC.status(job$job_id)
while (status$status %in% c("SUBMITTED", "PENDING", "RUNNING")) {
  cat("Job is still running. Status:", status$status, "\n")
  Sys.sleep(5)  # Wait 5 seconds before checking again
  status <- dsHPC.status(job$job_id)
}

# Now retrieve the result
result <- dsHPC.result(job$job_id)
print(result)
```

### Listing Jobs

```r
# List all jobs
all_jobs <- dsHPC.list_jobs()
print(all_jobs)

# List only completed jobs
completed_jobs <- dsHPC.list_jobs(status_filter = "COMPLETED")
print(completed_jobs)

# List multiple status types
jobs <- dsHPC.list_jobs(status_filter = c("FAILED", "RUNNING"))
print(jobs)
```

### Cancelling Jobs

```r
# Submit a long-running job
job <- dsHPC.submit(
  func = function(n) { Sys.sleep(n); return(n) },
  args = list(n = a60)  # Sleep for 60 seconds
)

# Cancel the job
cancelled <- dsHPC.cancel(job$job_id)
if (cancelled) {
  print("Job was successfully cancelled")
} else {
  print("Job could not be cancelled")
}
```

## Caching System

### Using the Cache

dsHPC automatically caches results, so if you submit the same job again, it will use the cached result:

```r
# Submit a job with caching enabled (default)
job1 <- dsHPC.submit(
  func = mean,
  args = list(x = c(1, 2, 3, 4, 5)),
  use_cache = TRUE
)

# Submit the same job again
job2 <- dsHPC.submit(
  func = mean,
  args = list(x = c(1, 2, 3, 4, 5)),
  use_cache = TRUE
)

# The second job should use the cached result
print(job2$status)  # Should be "CACHED"
print(job2$result)  # Should have the result immediately

# Disable caching if you want to recompute
job3 <- dsHPC.submit(
  func = mean,
  args = list(x = c(1, 2, 3, 4, 5)),
  use_cache = FALSE
)

# Check if result is in cache without submitting a job
cached_result <- dsHPC.submit(
  func = mean,
  args = list(x = c(1, 2, 3, 4, 5)),
  use_cache = TRUE,
  cache_only = TRUE
)

if (cached_result$status == "CACHED") {
  print("Result found in cache")
  print(cached_result$result)
} else {
  print("Result not in cache")
}
```

### Cache Management

```r
# Clean up old cached results (default: older than 30 days)
dsHPC.clean_cache()

# Clean up results older than 7 days
dsHPC.clean_cache(days_to_keep = 7)

# Remove all completed job results
dsHPC.clean_cache(days_to_keep = 0)
```

## Advanced Features

### Slurm Integration

If Slurm is available, dsHPC will automatically submit jobs to the Slurm scheduler. You can customize the Slurm job parameters:

```r
# Submit a job with custom Slurm parameters
job <- dsHPC.submit(
  func = function(n) { 
    # Some computationally intensive task
    result <- matrix(0, n, n)
    for (i in 1:n) {
      for (j in 1:n) {
        result[i, j] <- i * j
      }
    }
    return(result)
  },
  args = list(n = 1000),
  slurm_opts = list(
    partition = "compute",
    memory = "16G",
    time = "01:00:00",  # 1 hour
    cpus = 4,
    r_module = "R/4.2.0",  # If you need to load a specific R module
    additional_modules = c("gcc/11.2.0"),  # Additional modules to load
    env_vars = list(OMP_NUM_THREADS = "4")  # Environment variables
  )
)
```

### Custom Configuration

You can customize the database location during initialization:

```r
# Initialize with a custom database path
dsHPC.init(db_path = "/path/to/custom/dsHPC.sqlite")

# Note: The database filename must contain "dsHPC" to avoid conflicts
# If it doesn't, it will be renamed automatically with a warning
```
