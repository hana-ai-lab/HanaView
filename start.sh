#!/bin/bash
# This script is the entrypoint for the Docker container.
# It starts the cron daemon and the uvicorn server.

# Exit immediately if a command exits with a non-zero status.
set -e

# Create logs directory if not exists
mkdir -p /app/logs

# Define the location for the cron environment file
ENV_FILE="/app/backend/cron-env.sh"

echo "Creating cron environment file at ${ENV_FILE}"
# Create a shell script that exports all current environment variables.
printenv | sed 's/^\(.*\)$/export \1/g' > "${ENV_FILE}"
chmod +x "${ENV_FILE}"

# Enable cron logging
echo "Enabling cron logging..."
touch /var/log/cron.log

echo "Starting cron daemon..."
# Start the cron service in the background
service cron start

# Verify cron is running and jobs are loaded
echo "Verifying cron setup..."
service cron status
echo "Cron jobs registered:"
crontab -l

# Start cron log monitoring in background (for debugging)
tail -f /var/log/cron.log &

echo "Starting Uvicorn web server..."
# Start the uvicorn server in the foreground.
exec uvicorn backend.main:app --host 0.0.0.0 --port 8000