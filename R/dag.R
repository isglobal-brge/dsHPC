# Module: DAG Pipeline Normalization
# Compiles declarative DAG specs into the existing durable step contract.

#' @keywords internal
.normalize_job_graph <- function(spec) {
  if (!is.null(spec$pipeline) && is.null(spec$dag)) {
    spec$dag <- spec$pipeline
  }
  if (is.null(spec$dag)) return(spec)
  if (!is.null(spec$steps)) {
    stop("Job spec cannot contain both 'steps' and 'dag'.", call. = FALSE)
  }

  compiled <- .compile_dag(spec$dag)
  spec$steps <- compiled$steps
  spec$dag_compiled <- list(
    node_order = compiled$node_order,
    node_step_index = compiled$node_step_index
  )
  spec
}

#' @keywords internal
.compile_dag <- function(dag) {
  nodes <- dag$nodes %||% dag
  if (!is.list(nodes) || length(nodes) == 0) {
    stop("DAG must contain a non-empty 'nodes' list.", call. = FALSE)
  }

  node_names <- names(nodes) %||% rep("", length(nodes))
  parsed <- vector("list", length(nodes))
  ids <- character(length(nodes))

  for (i in seq_along(nodes)) {
    node <- nodes[[i]]
    if (!is.list(node)) stop("DAG node ", i, " must be a list.", call. = FALSE)
    id <- node$id %||% node$name %||% node_names[[i]]
    if (is.null(id) || !nzchar(as.character(id))) {
      stop("DAG node ", i, " must have an 'id' or a list name.", call. = FALSE)
    }
    id <- .validate_identifier(as.character(id)[1], paste0("DAG node ", i, " id"))
    ids[[i]] <- id

    step <- node$step
    if (is.null(step)) {
      step <- node[setdiff(names(node),
        c("id", "name", "step", "inputs", "depends_on", "after", "needs"))]
    }
    if (!is.list(step)) stop("DAG node '", id, "' step must be a list.", call. = FALSE)
    deps <- node$inputs %||% node$depends_on %||% node$after %||% node$needs %||%
      step$inputs
    parsed[[i]] <- list(id = id, step = step, inputs = deps)
  }

  if (anyDuplicated(ids)) {
    stop("DAG node ids must be unique: ", ids[duplicated(ids)][1], call. = FALSE)
  }
  names(parsed) <- ids

  deps_by_id <- lapply(parsed, function(node) {
    refs <- .dag_dependency_ids(node$inputs)
    unknown <- setdiff(refs, ids)
    if (length(unknown) > 0) {
      stop("DAG node '", node$id, "' references unknown node: ",
           unknown[[1]], call. = FALSE)
    }
    unique(refs)
  })

  order <- .dag_topological_order(ids, deps_by_id)
  node_step_index <- stats::setNames(seq_along(order), order)
  steps <- vector("list", length(order))
  for (i in seq_along(order)) {
    id <- order[[i]]
    node <- parsed[[id]]
    step <- node$step
    step$node_id <- id
    compiled_inputs <- .dag_compile_inputs(node$inputs, node_step_index)
    if (length(compiled_inputs) > 0) step$inputs <- compiled_inputs
    else step$inputs <- NULL
    steps[[i]] <- step
  }

  list(steps = steps, node_order = order,
       node_step_index = as.list(node_step_index))
}

#' @keywords internal
.dag_dependency_ids <- function(inputs) {
  if (is.null(inputs)) return(character(0))
  if (is.character(inputs)) return(as.character(inputs))
  if (!is.list(inputs)) return(character(0))
  refs <- character(0)
  for (item in inputs) {
    if (is.character(item) && length(item) == 1) refs <- c(refs, item)
    else if (is.list(item)) {
      ref <- item$ref %||% item$node %||% item$id
      if (is.character(ref) && length(ref) == 1) refs <- c(refs, ref)
    }
  }
  refs[nzchar(refs)]
}

#' @keywords internal
.dag_compile_inputs <- function(inputs, node_step_index) {
  if (is.null(inputs)) return(list())
  if (is.character(inputs)) {
    return(lapply(inputs, function(ref) {
      list(name = ref, ref = ref, step = as.integer(node_step_index[[ref]]))
    }))
  }
  if (is.numeric(inputs)) return(as.list(as.integer(inputs)))
  if (!is.list(inputs)) return(list())

  nm <- names(inputs) %||% rep("", length(inputs))
  out <- list()
  for (i in seq_along(inputs)) {
    item <- inputs[[i]]
    name <- nm[[i]]
    if (is.character(item) && length(item) == 1) {
      ref <- item
      out[[length(out) + 1L]] <- list(
        name = if (nzchar(name)) name else ref,
        ref = ref,
        step = as.integer(node_step_index[[ref]])
      )
    } else if (is.numeric(item) && length(item) == 1) {
      out[[length(out) + 1L]] <- as.integer(item)
    } else if (is.list(item)) {
      ref <- item$ref %||% item$node %||% item$id
      step <- item$step %||% item$step_index
      if (is.character(ref) && length(ref) == 1) {
        step <- as.integer(node_step_index[[ref]])
      }
      if (!is.null(step)) {
        out[[length(out) + 1L]] <- list(
          name = item$name %||% if (nzchar(name)) name else ref %||% paste0("input_", i),
          ref = ref,
          step = as.integer(step),
          output = item$output %||% item$output_name %||% NULL
        )
      }
    }
  }
  out
}

#' @keywords internal
.dag_topological_order <- function(ids, deps_by_id) {
  remaining <- ids
  resolved <- character(0)
  order <- character(0)
  while (length(remaining) > 0) {
    ready <- remaining[vapply(remaining, function(id) {
      all(deps_by_id[[id]] %in% resolved)
    }, logical(1))]
    if (length(ready) == 0) {
      stop("DAG contains a dependency cycle.", call. = FALSE)
    }
    order <- c(order, ready)
    resolved <- c(resolved, ready)
    remaining <- setdiff(remaining, ready)
  }
  order
}
