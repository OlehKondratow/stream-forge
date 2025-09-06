# --- Stage 1: Build Frontend ---
FROM node:18-alpine AS builder

WORKDIR /app/frontend

# Copy package configuration and install dependencies
COPY services/visualizer/frontend/package.json .
# In a real project, you would also copy package-lock.json or yarn.lock
RUN rm -rf node_modules && npm cache clean --force
RUN rm -rf node_modules/lightweight-charts
RUN npm install

# Copy the rest of the frontend source code
COPY services/visualizer/frontend/ .

# Build the React application
RUN npm run build

# --- Stage 2: Build Backend and Serve Frontend ---
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install backend dependencies
COPY services/visualizer/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the backend code
COPY services/visualizer/backend/app .

# Copy the built frontend from the builder stage
COPY --from=builder /app/frontend/build .

# Expose the port the app will run on
EXPOSE 80

# Command to run the application
# We use port 80 as it's the standard HTTP port.
# The --host 0.0.0.0 is crucial to make the server accessible from outside the container.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--log-level", "debug"]