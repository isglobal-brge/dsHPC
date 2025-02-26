#' @title dsHPC: DataSHIELD Interface for High-Performance Computing
#' @description A server-side package providing DataSHIELD with access to High-Performance Computing capabilities.
#'
#' @details 
#' The \code{dsHPC} package enables DataSHIELD users to submit computationally intensive 
#' functions to HPC clusters, monitor job status, and retrieve results efficiently. The package
#' provides an interface to job schedulers (currently Slurm) and includes a caching system
#' to avoid redundant computations.
#' 
#' Key features include:
#' \itemize{
#'   \item Submit R functions to be executed on HPC clusters
#'   \item Job status monitoring and management
#'   \item Efficient caching of results based on function+arguments hashing
#'   \item Secure server-side operations within the DataSHIELD framework
#'   \item Function wrapping utilities to simplify HPC interactions
#' }
#' 
#' @section Basic Workflow:
#' 
#' 1. Initialize the system with \code{dsHPC.init()}
#' 2. Submit a job with \code{dsHPC.submit()}
#' 3. Check job status with \code{dsHPC.status()}
#' 4. Retrieve results with \code{dsHPC.result()}
#' 
#' @examples
#' \dontrun{
#' # Initialize the system
#' dsHPC.init()
#' 
#' # Submit a job
#' job <- dsHPC.submit(
#'   func_name = "kmeans",
#'   args = list(x = my_data, centers = 3),
#'   package = "stats"
#' )
#' 
#' # Check job status
#' status <- dsHPC.status(job$job_id)
#' 
#' # Get result (with waiting)
#' result <- dsHPC.result(job$job_id, wait_for_completion = TRUE)
#' }
#'
#' @docType package
#' @name dsHPC-package
"_PACKAGE" 