% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/05_jobs.R
\name{job_succeeded}
\alias{job_succeeded}
\title{Check if a job succeeded}
\usage{
job_succeeded(config, file_path, method_name, parameters = list())
}
\arguments{
\item{config}{API configuration created by create_api_config}

\item{file_path}{Path to the file processed}

\item{method_name}{Name of the method executed}

\item{parameters}{Parameters used for the method}
}
\value{
TRUE if the job completed successfully, FALSE otherwise
}
\description{
Check if a job succeeded
}
\examples{
\dontrun{
config <- create_api_config("http://localhost", 9000, "please_change_me")
if (job_succeeded(config, "image.jpg", "count_black_pixels", list(threshold = 30))) {
  print("Job completed successfully")
}
}
}
