"""
config.py - Configuration for the Guardian Ingestor pipeline.
All settings can be overridden via environment variables.
"""

import os

# API settings
API_BASE_URL: str = "https://jsonplaceholder.typicode.com"
RESOURCE_ENDPOINT: str = "/posts"
TOTAL_RECORDS_TO_FETCH: int = 20

# Chaos / simulation settings
# Probability (0.0 to 1.0) that any given record is deleted before fetch
CHAOS_DELETION_PROBABILITY: float = 0.25

# Resilience settings
# When True, 404 errors are caught, logged to audit, and the pipeline continues.
# When False, the pipeline raises on 404 (useful for testing failure behaviour).
HANDLE_MISSING_RECORDS: bool = True

# Retry settings for transient errors (5xx, timeouts)
MAX_RETRY_ATTEMPTS: int = 3
RETRY_BACKOFF_SECONDS: float = 1.5

# Database settings
# DB_TYPE: "sqlite" for local dev, "postgres" for Docker
DB_TYPE: str = os.getenv("DB_TYPE", "sqlite")

# SQLite (local dev)
DB_PATH: str = os.getenv("DB_PATH", "data/guardian.db")

# PostgreSQL (Docker / production)
PG_HOST: str = os.getenv("PG_HOST", "localhost")
PG_PORT: int = int(os.getenv("PG_PORT", "5432"))
PG_DB: str   = os.getenv("PG_DB",   "guardian")
PG_USER: str = os.getenv("PG_USER", "guardian")
PG_PASSWORD: str = os.getenv("PG_PASSWORD", "guardian123")

# Logging
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE: str = "logs/guardian_ingestor.log"
