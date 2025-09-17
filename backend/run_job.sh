#!/bin/bash
# This script is executed by cron to run a fetch or generate job.

# Exit immediately if a command exits with a non-zero status.
set -e

# Define the project's root directory.
APP_DIR="/app"
LOG_DIR="${APP_DIR}/logs"
JOB_TYPE=$1

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Log the job start
echo "$(date): Starting job: ${JOB_TYPE}" >> "${LOG_DIR}/cron.log"

# Execute the python script, redirecting stdout and stderr to a job-specific log file
# The python executable is called from the system path, as configured in the Dockerfile.
python3 -m backend.data_fetcher ${JOB_TYPE} >> "${LOG_DIR}/${JOB_TYPE}.log" 2>&1

# Log the job completion
echo "$(date): Completed job: ${JOB_TYPE}" >> "${LOG_DIR}/cron.log"
