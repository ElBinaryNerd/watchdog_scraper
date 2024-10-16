# Use an official Python runtime as the base image
FROM python:3.10-slim AS base

# Set the working directory in the container
WORKDIR /app

# Install system dependencies for playwright and MySQL client libraries
RUN apt-get update && apt-get install -y \
    libnss3 \
    libatk-bridge2.0-0 \
    libxcomposite1 \
    libxrandr2 \
    libasound2 \
    libpangocairo-1.0-0 \
    libatspi2.0-0 \
    libgtk-3-0 \
    libxdamage1 \
    libssl-dev \
    libffi-dev \
    build-essential \
    pkg-config \
    libmariadb-dev \
    mariadb-client \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt to install dependencies
COPY requirements.txt .

# Upgrade pip to the latest version and install dependencies from requirements.txt
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Install Playwright and its browsers (with dependencies)
RUN pip install playwright \
    && playwright install --with-deps

# Create directories for nltk resources within /app/app to match the application path
RUN mkdir -p /app/app/ai/nltk

# Install NLTK resources including omw-1.4
RUN python -m nltk.downloader -d /app/app/ai/nltk stopwords punkt wordnet omw-1.4

# Copy the current directory contents into the container at /app
COPY . /app

# Expose the port the app runs on
EXPOSE 5000

# Define environment variable for production
ENV FLASK_ENV=production

# Set the PYTHONPATH environment variable to include the app directory
ENV PYTHONPATH="/app:/app/app"

# Run the command to start the Flask server
CMD ["python", "main.py"]
