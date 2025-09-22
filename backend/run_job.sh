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

# Log the job start with more debug info
echo "$(date): Starting job: ${JOB_TYPE}" >> "${LOG_DIR}/cron.log"
echo "$(date): Current directory: $(pwd)" >> "${LOG_DIR}/cron.log"
echo "$(date): Python path: ${PYTHONPATH}" >> "${LOG_DIR}/cron.log"

# Change to app directory (CRITICAL!)
cd "${APP_DIR}"

# Set Python path to ensure modules can be found
export PYTHONPATH="${APP_DIR}:${PYTHONPATH}"

# Debug: Print Python version and module search path
echo "$(date): Python version: $(python3 --version)" >> "${LOG_DIR}/cron.log"
echo "$(date): Working directory: $(pwd)" >> "${LOG_DIR}/cron.log"

# Execute the python script
# The python script now handles push notifications internally for 'generate' jobs
if python3 -m backend.data_fetcher ${JOB_TYPE} >> "${LOG_DIR}/${JOB_TYPE}.log" 2>&1; then
    echo "$(date): Successfully completed job: ${JOB_TYPE}" >> "${LOG_DIR}/cron.log"
else
    echo "$(date): Failed to complete job: ${JOB_TYPE}" >> "${LOG_DIR}/cron.log"
    exit 1
fi