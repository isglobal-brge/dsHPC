# Module: resourcer integration for HPC unit selectors

#' dsHPC execution unit Resource client
#'
#' Resolves a `dshpc+unit://` Resource to a sealed, non-secret execution-unit
#' snapshot from the server administrator catalogue. Generic data-frame
#' materialization is deliberately unavailable.
#'
#' @importFrom R6 R6Class
#' @export
DsHpcUnitResourceClient <- R6::R6Class(
  "DsHpcUnitResourceClient",
  inherit = resourcer::ResourceClient,
  private = list(.selection = NULL),
  public = list(
    #' @description Create a client for an assigned execution-unit Resource.
    #' @param resource A DataSHIELD Resource descriptor.
    initialize = function(resource) {
      private$.selection <- .dshpc_unit_from_resource(resource)
      # Armadillo may attach a short-lived access token to the descriptor.
      # Retain only a canonical non-secret control Resource in this client.
      super$initialize(.dshpc_sanitized_unit_resource(private$.selection))
    },
    #' @description Return the validated, non-secret unit snapshot.
    getUnitSelection = function() private$.selection,
    #' @description Refuse generic materialization of a control Resource.
    asDataFrame = function() {
      stop("HPC unit resources cannot be materialized as data frames.",
        call. = FALSE)
    }
  )
)

#' dsHPC execution unit Resource resolver
#'
#' @export
DsHpcUnitResourceResolver <- R6::R6Class(
  "DsHpcUnitResourceResolver",
  inherit = resourcer::ResourceResolver,
  public = list(
    #' @description Test whether a Resource uses the dsHPC unit URL scheme.
    #' @param x A DataSHIELD Resource descriptor.
    isFor = function(x) {
      url <- tryCatch(x$url, error = function(e) "")
      format <- tryCatch(x$format, error = function(e) "")
      has_locator <- function(value) {
        is.character(value) && length(value) == 1L && !is.na(value) &&
          startsWith(value, "dshpc+unit://")
      }
      has_locator(url) ||
        (is.character(format) && length(format) == 1L && !is.na(format) &&
         startsWith(format, "dshpc-unit:"))
    },
    #' @description Create the dsHPC Resource client.
    #' @param x A DataSHIELD Resource descriptor.
    newClient = function(x) DsHpcUnitResourceClient$new(x)
  )
)
