#' @title Package Initialization Functions
#' @description Functions that run when the package is loaded or attached.

#' Package initialization
#'
#' @param libname The library name
#' @param pkgname The package name
#'
#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # Any initialization that doesn't produce output
}

#' Package attachment 
#'
#' @param libname The library name
#' @param pkgname The package name
#'
#' @keywords internal
.onAttach <- function(libname, pkgname) {
  # Display startup message
  packageStartupMessage("dsHPC: DataSHIELD Interface for High-Performance Computing")
  packageStartupMessage("Initialize with dsHPC.init() before using other functions")
}

#' Package unload function 
#'
#' @param libpath The library path
#'
#' @keywords internal
.onUnload <- function(libpath) {
  # Clean up resources when package is unloaded
  config <- getOption("dsHPC.config")
  if (!is.null(config) && !is.null(config$connection)) {
    if (DBI::dbIsValid(config$connection)) {
      DBI::dbDisconnect(config$connection)
    }
  }
  
  # Remove option
  options(dsHPC.config = NULL)
} 