#' @title Utility Functions for Testing
#' @name test_utils
#' @description Utility functions to support testing in the dsHPC package.
#'
#' @importFrom utils packageVersion
NULL

#' Get Path to Test Image
#'
#' Returns the full path to a test image included in the package.
#'
#' @param image_name Character string specifying the name of the test image.
#'   If NULL, returns the directory containing test images.
#'
#' @return Character string with the path to the test image or directory.
#'
#' @examples
#' \dontrun{
#' # Get path to the test image
#' image_path <- dsHPC.test_image_path("test_image.png")
#'
#' # Get path to the test image directory
#' image_dir <- dsHPC.test_image_path()
#' }
#'
#' @export
dsHPC.test_image_path <- function(image_name = NULL) {
  # Get the package installation directory
  pkg_dir <- system.file(package = "dsHPC")
  
  # Get the image directory
  img_dir <- file.path(pkg_dir, "img")
  
  # Return the directory if image_name is NULL
  if (is.null(image_name)) {
    return(img_dir)
  }
  
  # Otherwise, return the path to the specific image
  image_path <- file.path(img_dir, image_name)
  
  # Check if the image exists
  if (!file.exists(image_path)) {
    warning("Test image not found: ", image_name)
  }
  
  return(image_path)
} 