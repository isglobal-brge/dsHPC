# dsHPC: DataSHIELD Interface for High-Performance Computing

## Overview

`dsHPC` is a server-side DataSHIELD package that enables the execution of High-Performance Computing (HPC) jobs within the DataSHIELD framework. It provides a standardized interface for submitting, monitoring, and retrieving results from jobs running on HPC resources (Slurm clusters), with built-in caching to avoid redundant computations. **This package is designed to be used exclusively within the DataSHIELD server-side environment.**

## Features

- **DataSHIELD Integration**: Fully integrated with the DataSHIELD privacy-preserving framework
- **HPC Integration**: Direct interface with the Slurm job scheduler to submit and manage computational jobs
- **Efficient Caching**: Store and retrieve results based on function and parameter hashing to avoid redundant computations
- **Job Management**: Submit, monitor, cancel, and retrieve results from computational jobs
- **Flexible Configuration**: Customizable job submission parameters (memory, CPUs, time limits, etc.)
- **Local Fallback**: Gracefully falls back to local execution when Slurm is unavailable
- **Python Integration**: Seamlessly execute Python code and modules, with support for image processing via Pillow

## Documentation

- [Package Website](https://isglobal-brge.github.io/dsHPC/) - Complete documentation with function reference and vignettes
- [Usage Guide](docs/usage.md) - Comprehensive documentation for using dsHPC
- [Developer Integration Guide](docs/developer_guide.md) - Guide for DataSHIELD package developers who want to integrate dsHPC
- [Docker Setup](docker/) - Example Dockerfile to set up all dsHPC requirements in a rock instance

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

This creates the necessary database connections and checks for the Slurm scheduler. The package will always attempt to use Slurm for job submission, falling back to local execution only if Slurm is unavailable.

### Submitting Jobs

Submit a function to be executed on the Slurm cluster:

```r
# Simple example with function reference
job <- dsHPC.submit(
  func = mean,
  args = list(x = c(1, 2, 3, 4, 5))
)

# Using a function name
job <- dsHPC.submit_by_name(
  func_name = "kmeans",
  args = list(x = my_data_matrix, centers = 3),
  package = "stats"
)

# With custom Slurm options
job <- dsHPC.submit(
  func = kmeans,
  args = list(x = my_data_matrix, centers = 3),
  slurm_opts = list(
    partition = "normal",
    memory = "8G",
    time = "02:00:00",
    cpus = 2
  )
)
```

### Python Integration

Submit Python code or functions:

```r
# Submit a Python function from a module
job <- dsHPC.submit_python(
  py_module = "numpy",
  py_function = "mean",
  args = list(a = c(1, 2, 3, 4, 5))
)

# Submit arbitrary Python code
job <- dsHPC.submit_python(
  python_code = "
import numpy as np
result = np.mean([1, 2, 3, 4, 5])
"
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
# Get the result (returns NULL if not completed)
result <- dsHPC.result(job$job_id)

# Check status and get result
status <- dsHPC.status(job$job_id, return_result = TRUE)
if (status$status == "COMPLETED") {
  result <- status$result
  print(result)
}
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

## For Developers

Other DataSHIELD server-side packages can leverage dsHPC by:

1. Adding dsHPC as a dependency
2. Using `dsHPC.submit()` to execute computationally intensive functions

Example of integration in another package:

```r
# In your package's function
my_intensive_analysis <- function(data, ...) {
  # Check if dsHPC is available
  if (requireNamespace("dsHPC", quietly = TRUE) && !is.null(getOption("dsHPC.config"))) {
    # Define the computation function
    compute_function <- function(data, ...) {
      # Intensive computation
      result <- # ...
      return(result)
    }
    
    # Submit to HPC
    job <- dsHPC::dsHPC.submit(
      func = compute_function,
      args = list(data = data, ...),
      use_cache = TRUE
    )
    
    # Wait for and return the result
    if (job$status == "COMPLETED" || job$status == "CACHED") {
      return(job$result)
    } else {
      # Wait for job completion
      while (TRUE) {
        status <- dsHPC::dsHPC.status(job$job_id, return_result = TRUE)
        if (status$status %in% c("COMPLETED", "CACHED")) {
          return(status$result)
        } else if (status$status == "FAILED") {
          stop("Job failed: ", status$error_message)
        }
        Sys.sleep(2)
      }
    }
  } else {
    # Fallback to local computation
    warning("dsHPC not available, computing locally")
    # Local computation code
  }
}
```

For more advanced integration patterns, see the [Developer Integration Guide](docs/developer_guide.md).

## License

This package is licensed under the [MIT License](LICENSE).

