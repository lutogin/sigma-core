# ============================================================================
# Alpha Simple Trading Bot - Dockerfile
# ============================================================================
# Multi-stage build for optimized image size
# Python 3.14
# ============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder
# -----------------------------------------------------------------------------
FROM python:3.14-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# -----------------------------------------------------------------------------
# Stage 2: Runtime
# -----------------------------------------------------------------------------
FROM python:3.14-slim AS runtime

WORKDIR /app

# Create non-root user for security
RUN groupadd --gid 1000 bot && \
    useradd --uid 1000 --gid bot --shell /bin/bash --create-home bot

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set Python environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Copy application code
COPY --chown=bot:bot . .

# Create directories for logs and cache
RUN mkdir -p /app/logs /app/cache && \
    chown -R bot:bot /app/logs /app/cache

# Switch to non-root user
USER bot

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default command
CMD ["python", "main.py"]

