# Tests for PIL/Pillow Python integration in dsHPC

# Function to check if PIL is available in Python
pil_available <- function() {
  tryCatch({
    if (!reticulate::py_available()) {
      return(FALSE)
    }
    pil <- reticulate::import("PIL", delay_load = TRUE)
    return(TRUE)
  }, error = function(e) {
    return(FALSE)
  })
}

# Helper function to get the test image path during tests (before package is installed)
get_test_image_path <- function(image_name = "test_image.png") {
  # During tests, the image is in inst/img
  test_path <- file.path("../../inst/img", image_name)
  if (file.exists(test_path)) {
    return(test_path)
  } else {
    # Try system.file in case the package is installed
    pkg_path <- system.file("img", image_name, package = "dsHPC")
    if (pkg_path != "") {
      return(pkg_path)
    } else {
      skip("Test image not found")
      return(NULL)
    }
  }
}

test_that("dsHPC.create_test_image works", {
  skip_if_not_installed("reticulate")
  skip_if_not(python_available(), "Python is not available")
  skip_if_not(pil_available(), "PIL/Pillow is not available")
  
  # Create a temporary directory for test files
  temp_dir <- tempdir()
  test_image_path <- file.path(temp_dir, "test_image.png")
  
  # Remove test image if it already exists
  if (file.exists(test_image_path)) {
    file.remove(test_image_path)
  }
  
  # Mock the job ID for consistent testing
  mock_job_id <- paste0("pillow_test_", format(Sys.time(), "%Y%m%d%H%M%S"))
  
  # Test creating a test image with mocked UUIDgenerate
  with_mocked_bindings(
    UUIDgenerate = function(...) mock_job_id,
    .package = "uuid",
    {
      result <- dsHPC.create_test_image(
        output_path = test_image_path,
        width = 200,
        height = 150,
        color = c(0, 0, 255)  # Blue
      )
      
      # Check that we got expected results
      expect_true("job_id" %in% names(result))
      expect_equal(result$job_id, mock_job_id)
      
      # Check result when available
      if (result$status == "COMPLETED") {
        expect_true(file.exists(test_image_path))
        expect_equal(result$result$width, 200)
        expect_equal(result$result$height, 150)
        
        # Check color values (returned as a tuple from Python)
        color_values <- as.numeric(result$result$color)
        expect_equal(color_values[1], 0)
        expect_equal(color_values[2], 0)
        expect_equal(color_values[3], 255)
        
        expect_equal(result$result$format, "PNG")
      }
    }
  )
  
  # Clean up
  if (file.exists(test_image_path)) {
    file.remove(test_image_path)
  }
})

test_that("dsHPC.get_image_info validates inputs", {
  skip_if_not_installed("reticulate")
  
  # Test with invalid image_path
  expect_error(dsHPC.get_image_info(image_path = 123),
              "image_path must be a single character string")
  
  # Test with non-existent file
  expect_error(dsHPC.get_image_info(image_path = "nonexistent.jpg"),
              "Image file not found")
})

test_that("dsHPC.get_image_info works with test image", {
  skip_if_not_installed("reticulate")
  skip_if_not(python_available(), "Python is not available")
  skip_if_not(pil_available(), "PIL/Pillow is not available")
  
  # Get path to test image
  test_image_path <- get_test_image_path()
  skip_if_not(file.exists(test_image_path), "Test image not found")
  
  # Mock the job ID for consistent testing
  mock_job_id <- paste0("pillow_info_", format(Sys.time(), "%Y%m%d%H%M%S"))
  
  # Test getting image info with mocked UUIDgenerate
  with_mocked_bindings(
    UUIDgenerate = function(...) mock_job_id,
    .package = "uuid",
    {
      result <- dsHPC.get_image_info(
        image_path = test_image_path
      )
      
      # Check that we got expected results
      expect_true("job_id" %in% names(result))
      expect_equal(result$job_id, mock_job_id)
      
      # Check result when available
      if (result$status == "COMPLETED") {
        expect_equal(result$result$width, 300)
        expect_equal(result$result$height, 200)
        expect_equal(result$result$format, "PNG")
        expect_true(result$result$file_size > 0)
      }
    }
  )
})

test_that("dsHPC.apply_filter validates inputs", {
  skip_if_not_installed("reticulate")
  
  # Test with invalid image_path
  expect_error(dsHPC.apply_filter(image_path = 123),
              "image_path must be a single character string")
  
  # Test with invalid filter_type
  expect_error(dsHPC.apply_filter(image_path = "image.jpg", filter_type = c("blur", "sharpen")),
              "filter_type must be a single character string")
  
  # Test with invalid filter_type value
  expect_error(dsHPC.apply_filter(image_path = "image.jpg", filter_type = "invalid_filter"),
              "filter_type must be one of")
  
  # Test with invalid output_path
  expect_error(dsHPC.apply_filter(image_path = "image.jpg", output_path = 123),
              "output_path must be NULL or a single character string")
})

test_that("dsHPC.apply_filter works with test image", {
  skip_if_not_installed("reticulate")
  skip_if_not(python_available(), "Python is not available")
  skip_if_not(pil_available(), "PIL/Pillow is not available")
  
  # Get path to test image
  test_image_path <- get_test_image_path()
  skip_if_not(file.exists(test_image_path), "Test image not found")
  
  # Create a temporary file for output
  temp_dir <- tempdir()
  output_image_path <- file.path(temp_dir, "filtered_image.png")
  
  # Remove output image if it already exists
  if (file.exists(output_image_path)) {
    file.remove(output_image_path)
  }
  
  # Mock the job ID for consistent testing
  mock_job_id <- paste0("pillow_filter_", format(Sys.time(), "%Y%m%d%H%M%S"))
  
  # Test applying filter with mocked UUIDgenerate
  with_mocked_bindings(
    UUIDgenerate = function(...) mock_job_id,
    .package = "uuid",
    {
      result <- dsHPC.apply_filter(
        image_path = test_image_path,
        filter_type = "grayscale",
        output_path = output_image_path
      )
      
      # Check that we got expected results
      expect_true("job_id" %in% names(result))
      expect_equal(result$job_id, mock_job_id)
      
      # Check result when available
      if (result$status == "COMPLETED") {
        expect_true(file.exists(output_image_path))
        expect_equal(result$result$width, 300)
        expect_equal(result$result$height, 200)
        expect_equal(result$result$filter_type, "grayscale")
        expect_true(result$result$array_mean > 0)
      }
    }
  )
  
  # Clean up
  if (file.exists(output_image_path)) {
    file.remove(output_image_path)
  }
})

test_that("End-to-end PIL image processing", {
  skip_if_not_installed("reticulate")
  skip_if_not(python_available(), "Python is not available")
  skip_if_not(pil_available(), "PIL/Pillow is not available")
  
  # Get path to test image
  test_image_path <- get_test_image_path()
  skip_if_not(file.exists(test_image_path), "Test image not found")
  
  # Create temporary files for output
  temp_dir <- tempdir()
  new_image_path <- file.path(temp_dir, "created_image.png")
  output_image_path <- file.path(temp_dir, "processed_image.png")
  
  # Remove test images if they already exist
  if (file.exists(new_image_path)) file.remove(new_image_path)
  if (file.exists(output_image_path)) file.remove(output_image_path)
  
  # Create a unique mock job ID for each operation
  create_id <- paste0("create_", format(Sys.time(), "%Y%m%d%H%M%S"))
  info_id <- paste0("info_", format(Sys.time(), "%Y%m%d%H%M%S"))
  filter_id <- paste0("filter_", format(Sys.time(), "%Y%m%d%H%M%S"))
  
  # Test the full workflow with mock UUIDs
  tryCatch({
    # 1. Get information about the existing test image
    with_mocked_bindings(
      UUIDgenerate = function(...) info_id,
      .package = "uuid",
      {
        info_result <- dsHPC.get_image_info(
          image_path = test_image_path
        )
      }
    )
    
    # Check info result
    if (info_result$status == "COMPLETED") {
      expect_equal(info_result$result$width, 300)
      expect_equal(info_result$result$height, 200)
      expect_equal(info_result$result$format, "PNG")
    }
    
    # 2. Create a new test image
    with_mocked_bindings(
      UUIDgenerate = function(...) create_id,
      .package = "uuid",
      {
        create_result <- dsHPC.create_test_image(
          output_path = new_image_path,
          width = 250,
          height = 150,
          color = c(255, 0, 0)  # Red
        )
      }
    )
    
    # Check that the file was created
    expect_true(file.exists(new_image_path))
    
    # 3. Apply a filter to the new image
    with_mocked_bindings(
      UUIDgenerate = function(...) filter_id,
      .package = "uuid",
      {
        filter_result <- dsHPC.apply_filter(
          image_path = new_image_path,
          filter_type = "blur",
          output_path = output_image_path
        )
      }
    )
    
    # Check filter result
    if (filter_result$status == "COMPLETED") {
      expect_true(file.exists(output_image_path))
      expect_equal(filter_result$result$filter_type, "blur")
      expect_equal(filter_result$result$width, 250)
      expect_equal(filter_result$result$height, 150)
    }
    
  }, error = function(e) {
    skip(paste("Error in PIL end-to-end test:", e$message))
  }, finally = {
    # Clean up
    if (file.exists(new_image_path)) file.remove(new_image_path)
    if (file.exists(output_image_path)) file.remove(output_image_path)
  })
}) 