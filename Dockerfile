# Use the official Python image.
FROM python:3.11-slim-bookworm
WORKDIR /app

# Set the timezone to Japan Standard Time at the very beginning
ENV TZ=Asia/Tokyo

# Install system dependencies including tzdata for timezone support
RUN apt-get update && apt-get install -y \
    cron \
    curl \
    tzdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configure timezone properly for cron
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Copy backend requirements and install Python packages.
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files.
COPY backend /app/backend
COPY frontend /app/frontend

# Copy the startup script
COPY start.sh /app/start.sh

# Make scripts executable
RUN chmod +x /app/start.sh
RUN chmod +x /app/backend/run_job.sh

# Add cron job with explicit timezone
# Important: Include TZ in the crontab itself
RUN ( \
    echo "SHELL=/bin/bash" ; \
    echo "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" ; \
    echo "TZ=Asia/Tokyo" ; \
    echo "" ; \
    echo "15 6 * * 1-5 . /app/backend/cron-env.sh && /app/backend/run_job.sh fetch >> /app/logs/cron_error.log 2>&1" ; \
    echo "28 6 * * 1-5 . /app/backend/cron-env.sh && /app/backend/run_job.sh generate >> /app/logs/cron_error.log 2>&1" \
) | crontab -

# Create logs directory
RUN mkdir -p /app/logs

# Start services using the startup script
CMD [ "/app/start.sh" ]