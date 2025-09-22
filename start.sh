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

# IMPORTANT: Ensure timezone is properly set before starting cron
echo "Setting timezone to Asia/Tokyo..."
export TZ=Asia/Tokyo
# Reconfigure timezone non-interactively
dpkg-reconfigure -f noninteractive tzdata

# Enable cron logging
echo "Enabling cron logging..."
touch /var/log/cron.log

# Enable more verbose cron logging for debugging
echo "Configuring cron for verbose logging..."
# Enable cron logging to syslog
sed -i 's/^#cron./cron./' /etc/rsyslog.d/50-default.conf 2>/dev/null || true

echo "Starting cron daemon..."
# Restart cron service to ensure it picks up timezone settings
service cron restart

# Verify cron is running and jobs are loaded
echo "Verifying cron setup..."
service cron status || echo "Cron status check failed (may be normal)"

echo "Current system time and timezone:"
date
echo "Timezone file contents:"
cat /etc/timezone

echo "Cron jobs registered:"
crontab -l

echo "Testing environment variable availability in cron context..."
# Create a test to verify cron can access environment variables
(crontab -l ; echo "* * * * * date >> /app/logs/cron_heartbeat.log 2>&1") | crontab -
sleep 2
(crontab -l | grep -v "cron_heartbeat" | crontab -) || true

# Start cron log monitoring in background (for debugging)
echo "Starting log monitoring..."
tail -f /var/log/cron.log /app/logs/cron_error.log 2>/dev/null &

echo "Starting Uvicorn web server..."
# Start the uvicorn server in the foreground.
exec uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload