# Module: session-bound HPC unit selection

.DSHPC_UNIT_SESSION_BINDING <- ".dshpc_private_unit_state_v1"
.dshpc_unit_session_marker <- new.env(parent = emptyenv())
.dshpc_unit_tombstone <- new.env(parent = emptyenv())

.dshpc_unit_session_state <- function(owner_env, create = FALSE) {
  if (!is.environment(owner_env)) .dshpc_unit_fail()
  binding <- .DSHPC_UNIT_SESSION_BINDING
  if (exists(binding, envir = owner_env, inherits = FALSE)) {
    state <- get(binding, envir = owner_env, inherits = FALSE)
    valid <- is.environment(state) &&
      identical(state$marker, .dshpc_unit_session_marker) &&
      is.environment(state$selections) && is.environment(state$current) &&
      bindingIsLocked(binding, owner_env) && environmentIsLocked(state)
    if (!isTRUE(valid)) .dshpc_unit_fail()
    return(state)
  }
  if (!isTRUE(create)) return(NULL)

  state <- new.env(parent = emptyenv())
  state$marker <- .dshpc_unit_session_marker
  state$selections <- new.env(parent = emptyenv())
  state$current <- new.env(parent = emptyenv())
  lockEnvironment(state, bindings = TRUE)
  assign(binding, state, envir = owner_env)
  lockBinding(binding, owner_env)
  state
}

.dshpc_new_unit_capability <- function() {
  token <- gsub("-", "", paste0(uuid::UUIDgenerate(use.time = FALSE),
    uuid::UUIDgenerate(use.time = FALSE)), fixed = TRUE)
  if (!grepl("^[0-9a-f]{64}$", token)) .dshpc_unit_fail()
  paste0("hpcu_", token)
}

.dshpc_is_unit_reference <- function(x) {
  inherits(x, "dshpc_unit_ref") && is.list(x) &&
    identical(names(x), "capability") &&
    is.character(x$capability) && length(x$capability) == 1L &&
    !is.na(x$capability) && grepl("^hpcu_[0-9a-f]{64}$", x$capability)
}

.dshpc_unit_reference_visible <- function(owner_env, capability) {
  symbols <- setdiff(ls(owner_env, all.names = TRUE),
    .DSHPC_UNIT_SESSION_BINDING)
  for (symbol in symbols) {
    active <- tryCatch(bindingIsActive(symbol, owner_env),
      error = function(e) TRUE)
    if (isTRUE(active)) next
    object <- tryCatch(get(symbol, envir = owner_env, inherits = FALSE),
      error = function(e) NULL)
    if (.dshpc_is_unit_reference(object) &&
        identical(object$capability, capability)) return(TRUE)
  }
  FALSE
}

.dshpc_register_unit_selection <- function(selection, owner_env) {
  selection <- .dshpc_validate_unit_snapshot(selection)
  state <- .dshpc_unit_session_state(owner_env, create = TRUE)
  selections <- state$selections
  current <- state$current
  if (exists("capability", envir = current, inherits = FALSE)) {
    current_capability <- current$capability
    entry <- selections[[current_capability]]
    if (!.dshpc_unit_reference_visible(owner_env, current_capability)) {
      selections[[current_capability]] <- .dshpc_unit_tombstone
      rm(list = "capability", envir = current)
      entry <- .dshpc_unit_tombstone
    }
    if (!is.null(entry) && !identical(entry, .dshpc_unit_tombstone)) {
      stop("An HPC unit is already selected in this session.", call. = FALSE)
    }
    if (exists("capability", envir = current, inherits = FALSE)) {
      rm(list = "capability", envir = current)
    }
  }
  capability <- .dshpc_new_unit_capability()
  selections[[capability]] <- selection
  current$capability <- capability
  structure(list(capability = capability), class = "dshpc_unit_ref")
}

.dshpc_unit_selection_from_session <- function(owner_env) {
  if (is.null(owner_env)) return(NULL)
  state <- .dshpc_unit_session_state(owner_env, create = FALSE)
  if (is.null(state) ||
      !exists("capability", envir = state$current, inherits = FALSE)) {
    return(NULL)
  }
  capability <- state$current$capability
  selection <- state$selections[[capability]]
  if (!.dshpc_unit_reference_visible(owner_env, capability)) {
    selections <- state$selections
    current <- state$current
    selections[[capability]] <- .dshpc_unit_tombstone
    rm(list = "capability", envir = current)
    return(NULL)
  }
  if (is.null(selection) || identical(selection, .dshpc_unit_tombstone)) {
    .dshpc_unit_fail()
  }
  .dshpc_validate_unit_snapshot(selection)
}

#' Initialize a session HPC unit selection
#'
#' DataSHIELD assign method. `resource_symbol` names a Resource previously
#' assigned and resolved by `datashield.assign.resource()`.
#'
#' @param resource_symbol Name of the assigned Resource symbol.
#' @return An opaque, session-bound unit reference.
#' @export
hpcUnitInitDS <- function(resource_symbol) {
  .dshpc_require_literal_or_symbol(substitute(resource_symbol),
    "resource_symbol")
  owner_env <- parent.frame()
  if (!is.character(resource_symbol) || length(resource_symbol) != 1L ||
      is.na(resource_symbol) ||
      !grepl("^[A-Za-z][A-Za-z0-9._]{0,127}$", resource_symbol) ||
      !exists(resource_symbol, envir = owner_env, inherits = FALSE)) {
    .dshpc_unit_fail()
  }
  object <- get(resource_symbol, envir = owner_env, inherits = FALSE)
  selection <- tryCatch({
    if (inherits(object, "DsHpcUnitResourceClient")) {
      object$getUnitSelection()
    } else {
      .dshpc_unit_fail()
    }
  }, error = function(e) .dshpc_unit_fail())
  .dshpc_register_unit_selection(selection, owner_env)
}

#' Destroy a session HPC unit selection
#'
#' @param handle_symbol Name of the opaque unit reference symbol.
#' @return The opaque reference, invisibly, as a retry tombstone.
#' @export
hpcUnitDestroyDS <- function(handle_symbol) {
  .dshpc_require_literal_or_symbol(substitute(handle_symbol), "handle_symbol")
  owner_env <- parent.frame()
  unavailable <- function() .dshpc_unit_fail()
  if (!is.character(handle_symbol) || length(handle_symbol) != 1L ||
      is.na(handle_symbol) ||
      !grepl("^[A-Za-z][A-Za-z0-9._]{0,127}$", handle_symbol) ||
      !exists(handle_symbol, envir = owner_env, inherits = FALSE)) unavailable()
  reference <- get(handle_symbol, envir = owner_env, inherits = FALSE)
  if (!.dshpc_is_unit_reference(reference) ||
      bindingIsLocked(handle_symbol, owner_env)) unavailable()
  state <- .dshpc_unit_session_state(owner_env, create = FALSE)
  if (is.null(state)) unavailable()
  selections <- state$selections
  current <- state$current
  entry <- selections[[reference$capability]]
  if (identical(entry, .dshpc_unit_tombstone)) {
    rm(list = handle_symbol, envir = owner_env)
    return(invisible(reference))
  }
  if (is.null(entry)) unavailable()
  selections[[reference$capability]] <- .dshpc_unit_tombstone
  if (exists("capability", envir = current, inherits = FALSE) &&
      identical(current$capability, reference$capability)) {
    rm(list = "capability", envir = current)
  }
  rm(list = handle_symbol, envir = owner_env)
  invisible(reference)
}

#' Read the active unit selection from trusted server code
#'
#' Domain packages use this only when they must persist an orchestration
#' context that will submit more jobs after the originating session has gone.
#'
#' @param session_env DataSHIELD session environment.
#' @param default_label Optional domain-package label. When supplied and the
#'   session has no selected Resource, the effective site default is returned
#'   as a durable snapshot instead of `NULL`.
#' @return A sealed non-secret unit snapshot, or `NULL` when no Resource is
#'   active and `default_label` was not supplied.
#' @export
hpcUnitSelectionInternal <- function(session_env, default_label = NULL) {
  .dshpc_require_trusted_server_caller()
  if (!is.null(default_label)) {
    default_label <- .dshpc_require_label_value(default_label,
      "A domain label is required when pinning an HPC unit.")
    .dshpc_require_trusted_server_caller(default_label)
  }
  selected <- .dshpc_unit_selection_from_session(session_env)
  if (!is.null(selected)) {
    if (!is.null(default_label)) {
      selected <- .dshpc_validate_unit_snapshot(selected,
        spec = list(label = default_label, steps = list()))
    }
    return(selected)
  }
  if (is.null(default_label)) return(NULL)
  .dshpc_site_default_snapshot(default_label)
}
