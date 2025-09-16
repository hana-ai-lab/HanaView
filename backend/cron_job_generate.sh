#!/bin/bash
LOG_DIR="/app/logs"
echo "$(date): Starting report generation..." >> $LOG_DIR/cron.log
cd /app
python -m backend.data_fetcher generate >> $LOG_DIR/generate.log 2>&1
echo "$(date): Report generation completed" >> $LOG_DIR/cron.log
