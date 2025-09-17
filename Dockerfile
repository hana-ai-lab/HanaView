# Use the official Python image.
FROM python:3.11-slim-bookworm
WORKDIR /app

# Set the timezone to Japan Standard Time.
ENV TZ=Asia/Tokyo

# Install system dependencies.
RUN apt-get update && apt-get install -y \
    cron \
    curl \
    # Cleanup
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

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

# Add cron job
COPY backend/cron_jobs /etc/cron.d/hanaview-cron
RUN chmod 0644 /etc/cron.d/hanaview-cron

# Start services using the startup script
CMD [ "/app/start.sh" ]