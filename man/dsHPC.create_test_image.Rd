% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pillow_test.R
\name{dsHPC.create_test_image}
\alias{dsHPC.create_test_image}
\title{Create Test Image}
\usage{
dsHPC.create_test_image(
  output_path,
  width = 100,
  height = 100,
  color = c(255, 0, 0),
  use_cache = TRUE
)
}
\arguments{
\item{output_path}{Character string specifying the path to save the created image.}

\item{width}{Integer specifying the width of the image in pixels (default: 100).}

\item{height}{Integer specifying the height of the image in pixels (default: 100).}

\item{color}{Vector of three integers (0-255) specifying the RGB color (default: c(255, 0, 0)).}

\item{use_cache}{Logical indicating whether to use cached results (default: TRUE).}
}
\value{
A list containing information about the created image, including dimensions,
color, format, path, and file size.
}
\description{
Create Test Image
}
\examples{
\dontrun{
# Initialize dsHPC
dsHPC.init()

# Create a test image with blue color
result <- dsHPC.create_test_image(
  output_path = "test_image.png",
  width = 200,
  height = 200,
  color = c(0, 0, 255)
)
print(result)
}

}
