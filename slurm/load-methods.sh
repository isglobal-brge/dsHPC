#!/bin/bash
# Script to load all methods in the methods directory into the database

# Check if METHODS_DIR environment variable is set
if [ -z "${METHODS_DIR}" ]; then
    echo "METHODS_DIR environment variable not set, using default /methods"
    METHODS_DIR="/methods"
fi

# Check if the methods directory exists
if [ ! -d "${METHODS_DIR}" ]; then
    echo "Methods directory ${METHODS_DIR} does not exist, creating it"
    mkdir -p "${METHODS_DIR}"
    exit 0
fi

# Check if the methods/commands directory exists
if [ ! -d "${METHODS_DIR}/commands" ]; then
    echo "Commands directory ${METHODS_DIR}/commands does not exist, creating it"
    mkdir -p "${METHODS_DIR}/commands"
fi

# Check if the methods/scripts directory exists
if [ ! -d "${METHODS_DIR}/scripts" ]; then
    echo "Scripts directory ${METHODS_DIR}/scripts does not exist, creating it"
    mkdir -p "${METHODS_DIR}/scripts"
fi

echo "Loading methods from ${METHODS_DIR}/commands..."

# Check if there are any json files in the commands directory
if [ ! "$(ls -A ${METHODS_DIR}/commands/*.json 2>/dev/null)" ]; then
    echo "No command files found in ${METHODS_DIR}/commands"
    exit 0
fi

# For each command file in the commands directory
for command_file in ${METHODS_DIR}/commands/*.json; do
    # Get the command name from the file name
    command_name=$(basename "${command_file}" .json)
    echo "Processing command: ${command_name}"
    
    # Check if the script directory exists
    if [ ! -d "${METHODS_DIR}/scripts/${command_name}" ]; then
        echo "Script directory for ${command_name} not found, skipping"
        continue
    fi
    
    # Register the method - using api_python environment
    /opt/venvs/api_python/bin/python /app/scripts/register_method.py "${METHODS_DIR}/scripts/${command_name}" "${command_file}"
done

echo "Methods loading complete" 