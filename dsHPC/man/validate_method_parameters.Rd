% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/02_methods.R
\name{validate_method_parameters}
\alias{validate_method_parameters}
\title{Validate method parameters against method specification}
\usage{
validate_method_parameters(method_name, params, method_spec)
}
\arguments{
\item{method_name}{The name of the method to validate against}

\item{params}{A named list of parameters to validate}

\item{method_spec}{The method specification as returned by get_methods()}
}
\value{
TRUE if validation passes, otherwise throws an error with details
}
\description{
Validate method parameters against method specification
}
\examples{
config <- create_api_config("http://localhost", 9000, "please_change_me")
methods <- get_methods(config)
params <- list(threshold = 50)
validate_method_parameters("count_black_pixels", params, methods)
}
