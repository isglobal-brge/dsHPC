% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/slurm.R
\name{submit_slurm_job}
\alias{submit_slurm_job}
\title{Submit Job to Slurm}
\usage{
submit_slurm_job(submission_script)
}
\arguments{
\item{submission_script}{Path to the Slurm submission script.}
}
\value{
Character string with the Slurm job ID, or NULL if submission failed.
}
\description{
Submits a job to the Slurm scheduler.
}
\keyword{internal}
