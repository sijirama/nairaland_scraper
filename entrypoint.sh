#!/bin/bash
# Entrypoint script for running the scraper with xvfb

# More aggressive cleanup
pkill -9 Xvfb
rm -f /tmp/.X99-lock /tmp/.X11-unix/X99

# Export display for xvfb
export DISPLAY=:99

# Start Xvfb in the background. 
Xvfb :99 -screen 0 1920x1080x24 -ac -nolisten tcp &
XVFB_PID=$!

# Wait for Xvfb to be ready
sleep 5

# Run the scraper with unbuffered Python output
exec python -u src/scraper.py "$@"
