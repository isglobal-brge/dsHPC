#' @title Python PIL/Pillow Image Processing Functions
#' @name pillow_test
#' @description R interface to Python PIL/Pillow image processing functions for testing the Python integration.
#'
#' @import reticulate
#' @importFrom utils packageVersion
NULL

#' Initialize the Python module path for testing
#'
#' This function ensures that the pillow_test.py module can be found by Python.
#' It's called internally by the other functions in this module.
#'
#' @return NULL, invisibly
#' @keywords internal
init_pillow_test <- function() {
  # Find the Python module directory
  python_dir <- system.file("python", package = "dsHPC")
  
  # If running from the source directory during development
  if (python_dir == "") {
    # Use the development path
    pkg_dir <- getwd()
    if (basename(pkg_dir) == "dsHPC") {
      python_dir <- file.path(pkg_dir, "inst", "python")
    }
  }
  
  # Add the directory to the Python path if it exists
  if (python_dir != "" && dir.exists(python_dir)) {
    # Use reticulate to add the directory to the Python path
    reticulate::py_run_string(paste0("import sys; sys.path.append('", python_dir, "')"))
  } else {
    warning("Could not find the Python module directory. Some functions may not work correctly.")
  }
  
  invisible(NULL)
}

#' Get Basic Image Information
#'
#' @param image_path Character string specifying the path to the image file.
#' @param use_cache Logical indicating whether to use cached results (default: TRUE).
#'
#' @return A list containing basic information about the image, including dimensions,
#'   format, mode, and file size.
#'
#' @examples
#' \dontrun{
#' # Initialize dsHPC
#' dsHPC.init()
#'
#' # Get image information
#' img_info <- dsHPC.get_image_info("path/to/image.jpg")
#' print(img_info)
#' }
#'
#' @export
dsHPC.get_image_info <- function(image_path, use_cache = TRUE) {
  # Initialize Python path
  init_pillow_test()
  
  # Validate inputs
  if (!is.character(image_path) || length(image_path) != 1) {
    stop("image_path must be a single character string")
  }
  
  # Check if the file exists
  if (!file.exists(image_path)) {
    stop("Image file not found: ", image_path)
  }
  
  # Submit the Python job
  result <- dsHPC.submit_python(
    py_module = "pillow_test",
    py_function = "get_image_info",
    args = list(image_path = image_path),
    use_cache = use_cache
  )
  
  return(result)
}

#' Apply Image Filter
#'
#' @param image_path Character string specifying the path to the input image file.
#' @param filter_type Character string specifying the type of filter to apply.
#'   Options are "blur", "sharpen", "contour", or "grayscale" (default: "blur").
#' @param output_path Optional character string specifying the path to save the processed image.
#'   If NULL, the image will not be saved (default: NULL).
#' @param use_cache Logical indicating whether to use cached results (default: TRUE).
#'
#' @return A list containing information about the processed image, including dimensions,
#'   mode, filter type applied, and statistics about the image array.
#'
#' @examples
#' \dontrun{
#' # Initialize dsHPC
#' dsHPC.init()
#'
#' # Apply a blur filter to an image
#' result <- dsHPC.apply_filter(
#'   image_path = "path/to/image.jpg",
#'   filter_type = "blur",
#'   output_path = "path/to/output.jpg"
#' )
#' print(result)
#' }
#'
#' @export
dsHPC.apply_filter <- function(image_path, filter_type = "blur", output_path = NULL, use_cache = TRUE) {
  # Initialize Python path
  init_pillow_test()
  
  # Validate inputs
  if (!is.character(image_path) || length(image_path) != 1) {
    stop("image_path must be a single character string")
  }
  
  if (!is.character(filter_type) || length(filter_type) != 1) {
    stop("filter_type must be a single character string")
  }
  
  valid_filters <- c("blur", "sharpen", "contour", "grayscale")
  if (!filter_type %in% valid_filters) {
    stop("filter_type must be one of: ", paste(valid_filters, collapse = ", "))
  }
  
  if (!is.null(output_path) && (!is.character(output_path) || length(output_path) != 1)) {
    stop("output_path must be NULL or a single character string")
  }
  
  # Check if the input file exists
  if (!file.exists(image_path)) {
    stop("Input image file not found: ", image_path)
  }
  
  # Submit the Python job
  result <- dsHPC.submit_python(
    py_module = "pillow_test",
    py_function = "apply_filter",
    args = list(
      image_path = image_path,
      filter_type = filter_type,
      output_path = output_path
    ),
    use_cache = use_cache
  )
  
  return(result)
}

#' Create Test Image
#'
#' @param output_path Character string specifying the path to save the created image.
#' @param width Integer specifying the width of the image in pixels (default: 100).
#' @param height Integer specifying the height of the image in pixels (default: 100).
#' @param color Vector of three integers (0-255) specifying the RGB color (default: c(255, 0, 0)).
#' @param use_cache Logical indicating whether to use cached results (default: TRUE).
#'
#' @return A list containing information about the created image, including dimensions,
#'   color, format, path, and file size.
#'
#' @examples
#' \dontrun{
#' # Initialize dsHPC
#' dsHPC.init()
#'
#' # Create a test image with blue color
#' result <- dsHPC.create_test_image(
#'   output_path = "test_image.png",
#'   width = 200,
#'   height = 200,
#'   color = c(0, 0, 255)
#' )
#' print(result)
#' }
#'
#' @export
dsHPC.create_test_image <- function(output_path, width = 100, height = 100, 
                                  color = c(255, 0, 0), use_cache = TRUE) {
  # Initialize Python path
  init_pillow_test()
  
  # Validate inputs
  if (!is.character(output_path) || length(output_path) != 1) {
    stop("output_path must be a single character string")
  }
  
  if (!is.numeric(width) || length(width) != 1 || width <= 0) {
    stop("width must be a positive number")
  }
  
  if (!is.numeric(height) || length(height) != 1 || height <= 0) {
    stop("height must be a positive number")
  }
  
  if (!is.numeric(color) || length(color) != 3 || 
      any(color < 0) || any(color > 255)) {
    stop("color must be a vector of three integers between 0 and 255")
  }
  
  # Create output directory if it doesn't exist
  output_dir <- dirname(output_path)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Submit the Python job
  result <- dsHPC.submit_python(
    py_module = "pillow_test",
    py_function = "create_test_image",
    args = list(
      output_path = output_path,
      width = as.integer(width),
      height = as.integer(height),
      color = as.integer(color)  # Pass as a list, Python will convert
    ),
    use_cache = use_cache
  )
  
  return(result)
} 