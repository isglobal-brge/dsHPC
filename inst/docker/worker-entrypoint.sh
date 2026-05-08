#!/bin/bash
# Add this to your Rock container's entrypoint or docker-compose.
# Example docker-compose addition:
#
#   dsjobs-worker:
#     image: datashield/rock-base:latest
#     command: ["/bin/bash", "/opt/dsjobs/worker-entrypoint.sh"]
#     volumes:
#       - dsjobs_data:/srv/dsjobs
#     restart: unless-stopped
#
# Or add to existing Rock entrypoint:
#   Rscript /usr/local/lib/R/site-library/dsJobs/worker/main.R /srv/dsjobs &

DSJOBS_HOME="${DSJOBS_HOME:-/srv/dsjobs}"
mkdir -p "$DSJOBS_HOME"/runners "$DSJOBS_HOME"/artifacts \
  "$DSJOBS_HOME"/publish "$DSJOBS_HOME"/staging
chmod 0777 "$DSJOBS_HOME" "$DSJOBS_HOME"/runners "$DSJOBS_HOME"/artifacts \
  "$DSJOBS_HOME"/publish "$DSJOBS_HOME"/staging 2>/dev/null || true

exec Rscript /usr/local/lib/R/site-library/dsJobs/worker/main.R "$DSJOBS_HOME"
