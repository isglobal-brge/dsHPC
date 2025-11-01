#' Sort parameters alphabetically by name with RECURSIVE sorting
#'
#' This function ensures that parameters are always in the same order
#' regardless of how they were specified by the user. This is important
#' for job deduplication as MongoDB's document comparison is order-sensitive.
#'
#' Now includes RECURSIVE sorting of all nested structures to ensure
#' complete deterministic ordering at all nesting levels.
#'
#' @param parameters A named list of parameters
#'
#' @return A named list with parameters sorted alphabetically by name at all levels
#' @export
#'
#' @examples
#' params <- list(c = 3, a = 1, b = list(z = 10, x = 5))
#' sorted_params <- sort_parameters(params)
#' # Returns: list(a = 1, b = list(x = 5, z = 10), c = 3)
sort_parameters <- function(parameters) {
  if (!is.list(parameters)) {
    stop("parameters must be a list")
  }
  
  # Return empty named list if no parameters
  # This ensures it serializes as {} instead of []
  if (length(parameters) == 0) {
    return(structure(list(), names = character(0)))
  }
  
  # Get parameter names
  param_names <- names(parameters)
  
  # Check if all parameters are named
  if (is.null(param_names) || any(param_names == "")) {
    stop("All parameters must be named")
  }
  
  # Use recursive sorting helper
  return(sort_list_recursive(parameters))
}


#' Recursively sort a list and all nested lists by their names
#'
#' This helper function handles the recursive sorting of nested structures.
#' It sorts:
#' - Named lists by their names
#' - Recursively processes all values
#' - Handles data.frames, factors, and other R-specific types
#'
#' @param obj The object to sort (can be list, vector, data.frame, etc.)
#'
#' @return Sorted version of the object
#' @export
#'
#' @examples
#' nested <- list(z = list(c = 3, a = 1), a = 5)
#' sorted <- sort_list_recursive(nested)
#' # Returns: list(a = 5, z = list(a = 1, c = 3))
sort_list_recursive <- function(obj) {
  # Handle NULL
  if (is.null(obj)) {
    return(NULL)
  }
  
  # Handle data.frames specially - preserve their structure
  if (is.data.frame(obj)) {
    # Sort columns alphabetically
    col_order <- sort(names(obj))
    return(obj[, col_order, drop = FALSE])
  }
  
  # Handle named lists - sort by name and recurse
  if (is.list(obj) && !is.null(names(obj))) {
    # Sort names alphabetically
    sorted_names <- sort(names(obj))
    
    # Create new list with sorted names and recursively sorted values
    sorted_list <- list()
    for (name in sorted_names) {
      sorted_list[[name]] <- sort_list_recursive(obj[[name]])
    }
    
    return(sorted_list)
  }
  
  # Handle unnamed lists - recurse on elements but don't sort order
  if (is.list(obj)) {
    return(lapply(obj, sort_list_recursive))
  }
  
  # Handle vectors (including factors) - return as-is
  # We don't sort vector elements, just their container
  if (is.vector(obj) || is.factor(obj)) {
    return(obj)
  }
  
  # Handle matrices - return as-is
  if (is.matrix(obj)) {
    return(obj)
  }
  
  # For all other types (primitives, etc.), return as-is
  return(obj)
}
