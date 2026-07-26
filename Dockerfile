# ============================================================================
# Alpha Simple Trading Bot - Dockerfile
# ============================================================================
# Multi-stage build for optimized image size
# Python 3.13 (same runtime used by tests and CI)
# ============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder
# -----------------------------------------------------------------------------
FROM python:3.13-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install only runtime dependencies; CI installs the developer/test layer.
COPY requirements-runtime.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements-runtime.txt

# -----------------------------------------------------------------------------
# Stage 2: Runtime
# -----------------------------------------------------------------------------
FROM python:3.13-slim AS runtime

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
    PYTHONPATH=/app \
    SIGMA_HEALTH_FILE=/tmp/sigma-core-health \
    SIGMA_HEALTH_MAX_AGE_SECONDS=1800

# Copy application code
COPY --chown=bot:bot . .

# Create writable runtime directories, including the maintenance volume mountpoint.
RUN mkdir -p /app/logs /app/cache /app/backtests/results && \
    chown -R bot:bot /app/logs /app/cache /app/backtests/results

# Switch to non-root user
USER bot

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10m --retries=3 \
    CMD python src/infra/healthcheck.py

# Default command
CMD ["python", "main.py"]
