#' Sorting Utilities for Deterministic Ordering
#'
#' This module provides specialized sorting functions for different data types
#' used throughout the dsHPC system. Consistent sorting is critical for:
#' 1. Hash computation (for deduplication)
#' 2. API requests (for finding existing jobs/meta-jobs/pipelines)
#' 3. Reproducibility (same input -> same output)
#'
#' All sorting is done alphabetically by key/name to ensure consistency
#' between R client and Python server.


#' Sort file inputs by name
#'
#' Sort a named list of file inputs alphabetically by their names.
#' Used for multi-file operations to ensure consistent ordering.
#'
#' @param file_inputs Named list mapping input names to file hashes
#'
#' @return Named list with keys sorted alphabetically
#' @export
#'
#' @examples
#' inputs <- list(file_c = "hash3", file_a = "hash1", file_b = "hash2")
#' sorted <- sort_file_inputs(inputs)
#' # Returns: list(file_a = "hash1", file_b = "hash2", file_c = "hash3")
sort_file_inputs <- function(file_inputs) {
  if (is.null(file_inputs) || length(file_inputs) == 0) {
    return(structure(list(), names = character(0)))
  }
  
  if (!is.list(file_inputs)) {
    stop("file_inputs must be a named list")
  }
  
  if (is.null(names(file_inputs)) || any(names(file_inputs) == "")) {
    stop("All file inputs must be named")
  }
  
  # Sort by name
  sorted_names <- sort(names(file_inputs))
  sorted_list <- list()
  for (name in sorted_names) {
    sorted_list[[name]] <- file_inputs[[name]]
  }
  
  return(sorted_list)
}


#' Sort pipeline nodes by ID
#'
#' Sort a named list of pipeline nodes alphabetically by their IDs.
#' Also recursively sorts all nested structures within each node.
#'
#' @param nodes Named list mapping node IDs to node definitions
#'
#' @return Named list with node IDs sorted alphabetically
#' @export
#'
#' @examples
#' nodes <- list(
#'   node_c = list(chain = list(), dependencies = list()),
#'   node_a = list(chain = list(), dependencies = list())
#' )
#' sorted <- sort_nodes(nodes)
sort_nodes <- function(nodes) {
  if (is.null(nodes) || length(nodes) == 0) {
    return(structure(list(), names = character(0)))
  }
  
  if (!is.list(nodes)) {
    stop("nodes must be a named list")
  }
  
  if (is.null(names(nodes)) || any(names(nodes) == "")) {
    stop("All nodes must be named (have node IDs)")
  }
  
  # Sort node IDs alphabetically
  sorted_node_ids <- sort(names(nodes))
  
  # Create sorted list with recursively sorted node contents
  sorted_list <- list()
  for (node_id in sorted_node_ids) {
    node <- nodes[[node_id]]
    # Recursively sort all contents of the node
    sorted_list[[node_id]] <- sort_list_recursive(node)
  }
  
  return(sorted_list)
}


#' Sort dependencies vector
#'
#' Sort a character vector of dependencies (node IDs) alphabetically.
#'
#' @param dependencies Character vector of node IDs
#'
#' @return Sorted character vector
#' @export
#'
#' @examples
#' deps <- c("node_c", "node_a", "node_b")
#' sorted <- sort_dependencies(deps)
#' # Returns: c("node_a", "node_b", "node_c")
sort_dependencies <- function(dependencies) {
  if (is.null(dependencies) || length(dependencies) == 0) {
    return(character(0))
  }
  
  if (!is.character(dependencies) && !is.list(dependencies)) {
    stop("dependencies must be a character vector or list")
  }
  
  # Convert list to vector if needed
  if (is.list(dependencies)) {
    dependencies <- as.character(unlist(dependencies))
  }
  
  # Sort alphabetically
  return(sort(dependencies))
}


#' Sort method chain parameters
#'
#' Sort all parameters within a method chain.
#' The chain order itself is preserved (it's sequential),
#' but parameters within each step are sorted recursively.
#'
#' @param chain List of method steps, each with method_name and parameters
#'
#' @return Chain with all parameters recursively sorted
#' @export
#'
#' @examples
#' chain <- list(
#'   list(method_name = "concat", parameters = list(b = "2", a = "1")),
#'   list(method_name = "process", parameters = list(z = 10, x = 5))
#' )
#' sorted <- sort_chain(chain)
sort_chain <- function(chain) {
  if (is.null(chain) || length(chain) == 0) {
    return(list())
  }
  
  if (!is.list(chain)) {
    stop("chain must be a list")
  }
  
  # Process each step in the chain
  sorted_chain <- list()
  for (i in seq_along(chain)) {
    step <- chain[[i]]
    
    if (is.list(step)) {
      # Recursively sort the step (including its parameters)
      sorted_chain[[i]] <- sort_list_recursive(step)
    } else {
      sorted_chain[[i]] <- step
    }
  }
  
  return(sorted_chain)
}

