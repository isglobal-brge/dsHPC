# Test helpers for dsHPC

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

#' Create a minimal valid job spec
make_test_spec <- function(n_steps = 1) {
  steps <- lapply(seq_len(n_steps), function(i) {
    list(type = "emit", plane = "session",
         output_name = paste0("out_", i), value = i)
  })
  list(steps = steps, label = "dsHPC_test", resource_class = "default")
}
