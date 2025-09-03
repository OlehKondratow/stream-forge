FROM registry.dmz.home/kinga/stream-forge/base:v0.1.3

WORKDIR /app

# Copy dependency file from its full path
COPY services/queue-manager/requirements.txt .

# Install dependencies (pip will now find the local library)
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code from its full path
COPY services/queue-manager/app ./app

# Default command to run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
