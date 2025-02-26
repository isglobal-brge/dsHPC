# dsHPC: DataSHIELD Interface for High-Performance Computing

## Overview

`dsHPC` is a server-side DataSHIELD package that enables the execution of High-Performance Computing (HPC) jobs. It provides a standardized interface for submitting, monitoring, and retrieving results from jobs running on HPC resources, with built-in caching to avoid redundant computations. **This package is designed to be used by other DataSHIELD server-side packages.**

## Features

- **HPC Integration**: Interface with the job scheduler Slurm to submit and manage computational jobs
- **Efficient Caching**: Store and retrieve results based on function and parameter hashing to avoid redundant computations
- **Job Management**: Submit, monitor, cancel, and retrieve results from computational jobs
- **DataSHIELD Integration**: Secure server-side implementation compatible with DataSHIELD's privacy-preserving framework
- **Flexible Configuration**: Customizable job submission parameters (memory, CPUs, time limits, etc.)
- **Local Fallback**: Gracefully falls back to local execution when HPC resources are unavailable

## Installation

You can install the development version from GitHub:

```r
# install.packages("devtools")
devtools::install_github("isglobal-brge/dsHPC")
```

## Basic Usage

### Initialization

Before using the package, you need to initialize it:

```r
library(dsHPC)
dsHPC.init()
```

This creates the necessary database connections and checks for available job schedulers.

### Submitting Jobs

Submit a function to be executed on the HPC cluster:

```r
# Simple example
job <- dsHPC.submit(
  func_name = "mean",
  args = list(x = c(1, 2, 3, 4, 5))
)

# More complex example with custom Slurm options
job <- dsHPC.submit(
  func_name = "kmeans",
  args = list(x = my_data_matrix, centers = 3),
  package = "stats",
  required_packages = c("stats"),
  slurm_opts = list(
    partition = "normal",
    memory = "8G",
    time = "02:00:00",
    cpus = 2
  )
)
```

### Job Status Monitoring

Check the status of a submitted job:

```r
status <- dsHPC.status(job$job_id)
```

### Retrieving Results

Get the result of a completed job:

```r
# Non-blocking (returns NULL if not completed)
result <- dsHPC.result(job$job_id)

# Blocking (waits for job completion)
result <- dsHPC.result(job$job_id, wait_for_completion = TRUE)
```

### Job Management

List all jobs or filter by status:

```r
# List all jobs
all_jobs <- dsHPC.list_jobs()

# List only completed jobs
completed_jobs <- dsHPC.list_jobs(status_filter = "COMPLETED")
```

Cancel a running job:

```r
dsHPC.cancel(job$job_id)
```

Clean up old cached results:

```r
dsHPC.clean_cache(days_to_keep = 14)
```

## DataSHIELD Integration

For DataSHIELD environments, use the DataSHIELD-specific functions:

```r
# Initialize in DataSHIELD environment
dsHPC.ds.init()

# Submit a job in DataSHIELD environment
job <- dsHPC.ds.submit(
  func_name = "kmeans",
  args = list(x = my_data_matrix, centers = 3),
  package = "stats"
)
```

## Function Wrapping

Create a wrapped version of a function that will automatically be executed via HPC:

```r
# Create a wrapped version of the kmeans function
kmeans_hpc <- dsHPC.wrap_function(
  func = kmeans,
  package = "stats",
  required_packages = c("stats"),
  slurm_opts = list(memory = "8G", cpus = 2)
)

# Use the wrapped function as normal
result <- kmeans_hpc(x = my_data, centers = 3)
```

## Architecture

The package consists of several key components:

1. **Core Interface**: Main functions for job submission and management
2. **Database Management**: SQLite-based storage for job information and result caching
3. **Scheduler Interface**: Functions that interact with HPC job schedulers

## For Developers

Other DataSHIELD server-side packages can leverage dsHPC by:

1. Adding dsHPC as a dependency
2. Using `dsHPC.submit()` to execute computationally intensive functions
3. Using `dsHPC.wrap_function()` to create HPC-enabled versions of existing functions

Example of integration in another package:

```r
# In your package's function
my_intensive_analysis <- function(data, ...) {
  # Initialize dsHPC if not already done
  if (is.null(getOption("dsHPC.config"))) {
    dsHPC::dsHPC.init()
  }
  
  # Submit the actual analysis function to HPC
  job <- dsHPC::dsHPC.submit(
    func_name = "actual_analysis",
    args = list(data = data, ...),
    package = "myPackage"
  )
  
  # Wait for and return the result
  dsHPC::dsHPC.result(job$job_id, wait_for_completion = TRUE)
}
```

## License

This package is licensed under the [MIT License](LICENSE).
