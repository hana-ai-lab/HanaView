#!/bin/bash
# This script is the entrypoint for the Docker container.
# It starts the cron daemon and the uvicorn server.

# Exit immediately if a command exits with a non-zero status.
set -e

# Define the location for the cron environment file
ENV_FILE="/app/backend/cron-env.sh"

echo "Creating cron environment file at ${ENV_FILE}"
# Create a shell script that exports all current environment variables.
# This makes environment variables from docker-compose.yml (via .env) available to cron jobs.
printenv | sed 's/^\(.*\)$/export \1/g' > "${ENV_FILE}"
chmod +x "${ENV_FILE}"

# echo "Starting cron daemon..."
# # Start the cron service in the background
service cron start

echo "Starting Uvicorn web server..."
# Start the uvicorn server in the foreground.
# This will be the main process that keeps the container running.
exec uvicorn backend.main:app --host 0.0.0.0 --port 8000
