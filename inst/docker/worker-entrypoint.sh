#!/bin/bash
# Add this to your Rock container's entrypoint or docker-compose.
# Example docker-compose addition:
#
#   dshpc-worker:
#     image: datashield/rock-base:latest
#     command: ["/bin/bash", "/opt/dshpc/worker-entrypoint.sh"]
#     volumes:
#       - dshpc_data:/srv/dshpc
#     restart: unless-stopped
#
# Or add to existing Rock entrypoint:
#   Rscript /usr/local/lib/R/site-library/dsHPC/worker/main.R /srv/dshpc &

DSHPC_HOME="${DSHPC_HOME:-/srv/dshpc}"
mkdir -p "$DSHPC_HOME"/runners "$DSHPC_HOME"/artifacts \
  "$DSHPC_HOME"/publish "$DSHPC_HOME"/staging
chmod 0777 "$DSHPC_HOME" "$DSHPC_HOME"/runners "$DSHPC_HOME"/artifacts \
  "$DSHPC_HOME"/publish "$DSHPC_HOME"/staging 2>/dev/null || true

exec Rscript /usr/local/lib/R/site-library/dsHPC/worker/main.R "$DSHPC_HOME"
