% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{create_job_hash}
\alias{create_job_hash}
\title{Utility Functions for the dsHPC Package}
\usage{
create_job_hash(func_or_name, args, object_hash = NULL)
}
\arguments{
\item{func_or_name}{Character string representing the name of the function or its string representation from deparse.}

\item{args}{List of arguments passed to the function.}

\item{object_hash}{Optional hash of an object being processed (default: NULL).}
}
\value{
A character string containing a unique hash.
}
\description{
This file contains utility functions used throughout the package.

Creates a unique hash based on a function name or definition and its arguments to use for
caching and identifying jobs.
}
\keyword{internal}
