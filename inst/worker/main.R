#!/usr/bin/env Rscript
# dsHPC Worker Daemon
# Launched by .dshpc_worker_start() as a background R process.
# Usage: Rscript main.R /srv/dshpc

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript main.R <DSHPC_HOME>")
}

dshpc_home <- args[1]
options(dshpc.home = dshpc_home)

library(dsHPC)
dsHPC:::.worker_main()
