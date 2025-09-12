# Use the official Python image.
FROM python:3.11-slim-bookworm
WORKDIR /app

# Set the timezone to Japan Standard Time.
ENV TZ=Asia/Tokyo

# Install system dependencies including Chrome and ChromeDriver.
RUN apt-get update && apt-get install -y \
    cron curl wget gnupg unzip jq && \
    # Add Chrome repository
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    # Install Chrome
    apt-get install -y google-chrome-stable && \
    # Install ChromeDriver using the new JSON endpoints
    CHROMEDRIVER_URL=$(curl -s https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json | jq -r '.channels.Stable.downloads.chromedriver[] | select(.platform=="linux64").url') && \
    wget -q -O /tmp/chromedriver.zip ${CHROMEDRIVER_URL} && \
    unzip /tmp/chromedriver.zip -d /tmp/ && \
    mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm /tmp/chromedriver.zip && \
    rm -rf /tmp/chromedriver-linux64 && \
    # Cleanup
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy backend requirements and install Python packages.
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files.
COPY backend /app/backend
COPY frontend /app/frontend

# Make cron scripts executable
RUN chmod +x /app/backend/cron_job_*.sh

# Add cron job
COPY backend/cron_jobs /etc/cron.d/hanaview-cron
RUN chmod 0644 /etc/cron.d/hanaview-cron

# Start services.
CMD cron && cd /app/backend && uvicorn main:app --host 0.0.0.0 --port 8000