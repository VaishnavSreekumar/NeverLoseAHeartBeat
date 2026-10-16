# MayaCardio Cloud Receiver - Production / Render Container Deployment
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8000 \
    HOST=0.0.0.0

WORKDIR /app

# Install curl for health check probing
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy package specifications and source modules
COPY pyproject.toml README.md ./
COPY shared/ ./shared/
COPY pipeline/ ./pipeline/
COPY biosignal_mock/ ./biosignal_mock/

# Install package in editable mode with cloud dependencies (FastAPI, Uvicorn, psycopg2-binary)
RUN pip install --no-cache-dir -e ".[cloud]"

# Expose default HTTP port
EXPOSE 8000

# Bind to 0.0.0.0 and dynamically bind to Render's assigned $PORT environment variable
CMD ["sh", "-c", "exec uvicorn pipeline.cloud.server:app --host 0.0.0.0 --port ${PORT:-8000}"]
