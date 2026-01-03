# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Builder — install dependencies into a virtual environment
# Using a multi-stage build keeps the final image lean.
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Copy only the requirements first to leverage Docker layer caching.
# If requirements.txt doesn't change, this layer is reused on every rebuild.
COPY requirements.txt .

RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2: Runtime — copy only what is needed to run the pipeline
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Non-root user for security best practices
RUN useradd --create-home --shell /bin/bash guardian

WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /opt/venv /opt/venv

# Copy application source
COPY src/ ./src/

# Create writable directories for SQLite DB and logs
RUN mkdir -p data logs && chown -R guardian:guardian /app

# Switch to non-root user
USER guardian

# Put the venv on PATH
ENV PATH="/opt/venv/bin:$PATH"

# Environment overrides (can be overridden in docker-compose.yml)
ENV DB_PATH="data/guardian.db"
ENV LOG_LEVEL="INFO"

# Health check: verify Python + key imports work before marking container healthy
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import requests; print('OK')" || exit 1

# Default command — run the ingestor once and exit
CMD ["python", "src/main.py"]
