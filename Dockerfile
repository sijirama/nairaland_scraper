# Use the official Playwright Python image which includes all browser dependencies
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# Set the working directory
WORKDIR /app

# Install xvfb for virtual display (helps bypass Cloudflare)
RUN apt-get update && apt-get install -y \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install the browsers
RUN playwright install chromium

# Copy the source code
COPY src/ ./src/

# Create the data directory
RUN mkdir -p /app/data

# Copy and setup entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Default command using the entrypoint script
CMD ["/app/entrypoint.sh"]


