% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/02_methods.R
\name{get_methods}
\alias{get_methods}
\title{Get available methods from the API}
\usage{
get_methods(config)
}
\arguments{
\item{config}{API configuration created by create_api_config}
}
\value{
A named list of methods and their parameters
}
\description{
Get available methods from the API
}
\examples{
config <- create_api_config("http://localhost", 9000, "please_change_me")
methods <- get_methods(config)
}
