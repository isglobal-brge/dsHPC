# Python test helper functions
# This file runs before tests to properly configure Python environment

# Configure Python for reticulate
.setup_python <- function() {
  if (requireNamespace("reticulate", quietly = TRUE)) {
    # Get Python path from environment variable if set
    python_path <- Sys.getenv("PYTHON_PATH", "")
    
    # If not set, try the system Python
    if (python_path == "") {
      python_path <- Sys.which("python3")
    }
    
    if (python_path != "") {
      message("Attempting to use Python at: ", python_path)
      reticulate::use_python(python_path, required = FALSE)
    }
    
    # Check if Python is available
    is_available <- tryCatch({
      reticulate::py_run_string("import sys; sys.version")
      TRUE
    }, error = function(e) {
      message("Error initializing Python: ", e$message)
      FALSE
    })
    
    # Handle the case where Python is not available
    if (!is_available) {
      message("Python not available. Tests requiring Python will be skipped.")
      
      # Add a flag to environment to help tests decide whether to skip
      options(dsHPC.python_available = FALSE)
    } else {
      # Try to import numpy
      has_numpy <- tryCatch({
        reticulate::py_run_string("import numpy; numpy.__version__")
        TRUE
      }, error = function(e) {
        message("NumPy not available: ", e$message)
        FALSE
      })
      
      options(dsHPC.python_available = TRUE)
      options(dsHPC.numpy_available = has_numpy)
      
      message("Python configuration complete. Python available: ", is_available, 
              ", NumPy available: ", has_numpy)
    }
  }
}

# Function to check if Python is available (for tests to use)
python_available <- function() {
  getOption("dsHPC.python_available", FALSE)
}

# Function to check if NumPy is available (for tests to use)
numpy_available <- function() {
  getOption("dsHPC.numpy_available", FALSE)
}

# Run setup on load
.setup_python() 