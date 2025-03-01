% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/test_utils.R
\name{dsHPC.test_image_path}
\alias{dsHPC.test_image_path}
\title{Get Path to Test Image}
\usage{
dsHPC.test_image_path(image_name = NULL)
}
\arguments{
\item{image_name}{Character string specifying the name of the test image.
If NULL, returns the directory containing test images.}
}
\value{
Character string with the path to the test image or directory.
}
\description{
Returns the full path to a test image included in the package.
}
\examples{
\dontrun{
# Get path to the test image
image_path <- dsHPC.test_image_path("test_image.png")

# Get path to the test image directory
image_dir <- dsHPC.test_image_path()
}

}
