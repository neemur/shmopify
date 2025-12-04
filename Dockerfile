# Use a stable, slim Python image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /usr/src/app

# --- NEW: Install FFmpeg (Required for spotDL) ---
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application files
COPY . .

# Ensure the music library directory exists
RUN mkdir -p library

# Expose the default port 5000
EXPOSE 5000

# Command to run the app using Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "1", "server:app"]