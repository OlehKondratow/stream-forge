# Use a lightweight Python image
FROM python:3.11-slim-bookworm

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port for FastAPI (if applicable)
EXPOSE 8080

# Command to run the application
CMD ["python", "-m", "app.main"]
