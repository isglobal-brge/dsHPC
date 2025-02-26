# Dockerfile for dsHPC: DataSHIELD Interface for High-Performance Computing
FROM datashield/rock-base:6.3

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    sqlite3 \
    libsqlite3-dev \
    slurm-client \
    slurmd \
    slurmctld \
    munge \
    uuid-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages required by dsHPC
RUN pip3 install numpy pillow==11.1.0

# Install R packages required by dsHPC
RUN R -e "install.packages(c('reticulate', 'DBI', 'RSQLite', 'jsonlite', 'base64enc', 'uuid', 'devtools', 'testthat'))"

# Configure reticulate to use Python 3
RUN R -e "reticulate::use_python('/usr/bin/python3')"

# Set up a basic Slurm configuration
RUN mkdir -p /etc/slurm
COPY slurm.conf /etc/slurm/slurm.conf

# Create Slurm directories and set permissions
RUN mkdir -p /var/spool/slurmd /var/run/slurmd /var/lib/slurmd /var/log/slurm
RUN chown -R rock:rock /var/spool/slurmd /var/run/slurmd /var/lib/slurmd /var/log/slurm

# Install dsHPC package
RUN R -e "devtools::install_github('isglobal-brge/dsHPC')"

# Create directory for dsHPC database and results
RUN mkdir -p /home/rock/.dsHPC
RUN chown -R rock:rock /home/rock/.dsHPC

# Initialize the Slurm database
RUN sacctmgr -i add cluster name=rock
RUN sacctmgr -i add account name=rock
RUN sacctmgr -i add user name=rock account=rock

# Generate Munge key for Slurm authentication
RUN dd if=/dev/urandom bs=1 count=1024 > /etc/munge/munge.key
RUN chown munge:munge /etc/munge/munge.key
RUN chmod 400 /etc/munge/munge.key

# Change ownership of installed packages to 'rock' to avoid conflicts
RUN chown -R rock /var/lib/rock/R/library/

# Create startup script for services
RUN echo '#!/bin/bash \n\
service munge start \n\
service slurmd start \n\
service slurmctld start \n\
exec "$@"' > /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["R"] 