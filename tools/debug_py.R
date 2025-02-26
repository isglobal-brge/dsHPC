# Debug script for Python configuration

cat("Starting Python debug script\n")

# Check if reticulate is installed
if (!requireNamespace("reticulate", quietly = TRUE)) {
  cat("reticulate not installed. Installing now...\n")
  install.packages("reticulate")
}

library(reticulate)

cat("reticulate version:", as.character(packageVersion("reticulate")), "\n")

# Get Python configuration
cat("\nSystem Python:\n")
system_python <- Sys.which("python3")
cat("Python3 in PATH:", system_python, "\n")

# Check Python discovery
cat("\nPython discovery:\n")
try({
  config <- py_discover_config()
  print(config)
})

# Try to use the system Python
cat("\nAttempting to use system Python:\n")
if (system_python != "") {
  try({
    use_python(system_python, required = FALSE)
    cat("System Python configured\n")
  })
}

# Check if Python is available now
cat("\nPython availability:\n")
is_available <- py_available()
cat("py_available():", is_available, "\n")

# If Python is available, check modules
if (is_available) {
  # Check Python version
  cat("\nPython version:\n")
  py_run_string("import sys; print(sys.version)")
  
  # Check numpy
  cat("\nNumPy availability:\n")
  has_numpy <- py_module_available("numpy")
  cat("py_module_available('numpy'):", has_numpy, "\n")
  
  if (has_numpy) {
    cat("\nNumPy version:\n")
    py_run_string("import numpy; print(numpy.__version__)")
    
    # Try a numpy calculation
    cat("\nNumPy test calculation:\n")
    try({
      result <- py_run_string("import numpy as np; result = np.mean([1, 2, 3, 4, 5])")
      cat("Mean calculation result:", result$result, "\n")
    })
  }
} else {
  cat("\nPython not available, cannot check modules\n")
  
  # Try a different approach
  cat("\nTrying alternative Python configuration:\n")
  try({
    # If virtualenv or conda is available, we could try that
    if (system("which conda", ignore.stdout = TRUE, ignore.stderr = TRUE) == 0) {
      cat("Conda detected, trying to use a conda environment\n")
      try({
        use_condaenv("base", required = FALSE)
        cat("Conda base environment configured\n")
        is_available <- py_available()
        cat("py_available() after conda config:", is_available, "\n")
      })
    }
  })
}

cat("\nDebug script completed\n") 