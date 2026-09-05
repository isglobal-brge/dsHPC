# Test helpers for dsHPC

# Tests exercise canonical reuse under one explicit immutable deployment seal.
# Individual fail-closed tests remove it locally.
options(dshpc.runtime_revision = strrep("d", 64L))

#' Create a temporary DSHPC_HOME for testing
#' @return Character; path to temp home
setup_test_home <- function() {
  home <- tempfile("dshpc_test_")
  dir.create(home, recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(home, "runners"), showWarnings = FALSE)
  dir.create(file.path(home, "artifacts"), showWarnings = FALSE)
  dir.create(file.path(home, "publish"), showWarnings = FALSE)
  dir.create(file.path(home, "locks"), showWarnings = FALSE)
  home
}

#' Clean up a test home
cleanup_test_home <- function(home) {
  unlink(home, recursive = TRUE)
}

# Exercise server-only APIs through a package-namespace caller, matching how
# dsImaging/dsRadiomics invoke them in production.
trusted_hpc_call <- function(fn, ...) fn(...)
environment(trusted_hpc_call) <- asNamespace("dsHPC")

.description_method_table <- function(field) {
  description <- system.file("DESCRIPTION", package = "dsHPC")
  raw <- read.dcf(description, fields = field)[1, 1]
  if (is.na(raw) || !nzchar(trimws(raw))) {
    return(data.frame(name = character(0), value = character(0),
      package = character(0), version = character(0), type = character(0),
      class = character(0), stringsAsFactors = FALSE))
  }
  entries <- trimws(strsplit(raw, ",", fixed = TRUE)[[1]])
  aliases <- grepl("=", entries, fixed = TRUE)
  names <- ifelse(aliases, sub("=.*$", "", entries), entries)
  values <- ifelse(aliases, sub("^[^=]*=", "", entries),
    paste0("dsHPC::", entries))
  data.frame(
    name = names,
    value = values,
    package = "dsHPC",
    version = as.character(utils::packageVersion("dsHPC")),
    type = if (identical(field, "AggregateMethods")) "aggregate" else "assign",
    class = "function",
    stringsAsFactors = FALSE
  )
}

.dshpc_dslite_config <- function() {
  config <- DSLite::defaultDSConfiguration()
  config$AggregateMethods <- .description_method_table("AggregateMethods")
  config$AssignMethods <- .description_method_table("AssignMethods")
  config
}

#' Create a minimal valid job spec
make_test_spec <- function(n_steps = 1) {
  steps <- lapply(seq_len(n_steps), function(i) {
    list(type = "emit", plane = "session",
         output_name = paste0("out_", i), value = i)
  })
  list(steps = steps, label = "dsHPC_test", resource_class = "default")
}
