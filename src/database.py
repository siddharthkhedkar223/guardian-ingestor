"""
database.py - Storage layer supporting SQLite (local dev) and PostgreSQL (Docker).

Set DB_TYPE env var to "sqlite" or "postgres" to switch backends.
"""

import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from config import (
    DB_TYPE, DB_PATH,
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD,
)
from logger import get_logger

log = get_logger("database")

if DB_TYPE == "postgres":
    import psycopg2
    import psycopg2.extras


def get_connection():
    if DB_TYPE == "postgres":
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
        )
        conn.autocommit = False
        log.info("Connected to PostgreSQL at %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
        return conn
    else:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        log.info("Connected to SQLite at %s", DB_PATH)
        return conn


def _ph() -> str:
    """Return the correct SQL placeholder for the active backend."""
    return "%s" if DB_TYPE == "postgres" else "?"


def initialize_schema(conn) -> None:
    """Create tables if they do not already exist."""
    cursor = conn.cursor()

    if DB_TYPE == "postgres":
        statements = [
            """CREATE TABLE IF NOT EXISTS records (
                id          INTEGER PRIMARY KEY,
                user_id     INTEGER NOT NULL,
                title       TEXT    NOT NULL,
                body        TEXT,
                fetched_at  TEXT    NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS ingestion_audit_logs (
                audit_id        SERIAL  PRIMARY KEY,
                record_id       INTEGER NOT NULL,
                failure_reason  TEXT    NOT NULL,
                error_detail    TEXT,
                pipeline_run_id TEXT    NOT NULL,
                retried         INTEGER NOT NULL DEFAULT 0,
                failed_at       TEXT    NOT NULL
            )""",
            "CREATE INDEX IF NOT EXISTS idx_audit_record_id ON ingestion_audit_logs(record_id)",
            "CREATE INDEX IF NOT EXISTS idx_audit_run_id ON ingestion_audit_logs(pipeline_run_id)",
        ]
        for stmt in statements:
            cursor.execute(stmt)
        conn.commit()
    else:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS records (
                id          INTEGER PRIMARY KEY,
                user_id     INTEGER NOT NULL,
                title       TEXT    NOT NULL,
                body        TEXT,
                fetched_at  TEXT    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ingestion_audit_logs (
                audit_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                record_id       INTEGER NOT NULL,
                failure_reason  TEXT    NOT NULL,
                error_detail    TEXT,
                pipeline_run_id TEXT    NOT NULL,
                retried         INTEGER NOT NULL DEFAULT 0,
                failed_at       TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_audit_record_id
                ON ingestion_audit_logs(record_id);
            CREATE INDEX IF NOT EXISTS idx_audit_run_id
                ON ingestion_audit_logs(pipeline_run_id);
        """)

    log.info("Schema ready (%s).", DB_TYPE)


def upsert_record(conn, record: Dict[str, Any]) -> None:
    record["fetched_at"] = _utc_now()
    p = _ph()

    if DB_TYPE == "postgres":
        sql = f"""
            INSERT INTO records (id, user_id, title, body, fetched_at)
            VALUES ({p}, {p}, {p}, {p}, {p})
            ON CONFLICT (id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                title = EXCLUDED.title,
                body = EXCLUDED.body,
                fetched_at = EXCLUDED.fetched_at
        """
        cursor = conn.cursor()
        cursor.execute(sql, (record["id"], record["userId"], record["title"], record["body"], record["fetched_at"]))
    else:
        sql = "INSERT OR REPLACE INTO records (id, user_id, title, body, fetched_at) VALUES (:id, :userId, :title, :body, :fetched_at)"
        conn.execute(sql, record)

    conn.commit()
    log.debug("Saved Record ID=%d.", record["id"])


def log_audit_failure(
    conn,
    record_id: int,
    failure_reason: str,
    error_detail: Optional[str],
    pipeline_run_id: str,
    retried: bool = False,
) -> None:
    try:
        p = _ph()
        sql = f"""
            INSERT INTO ingestion_audit_logs
                (record_id, failure_reason, error_detail, pipeline_run_id, retried, failed_at)
            VALUES ({p}, {p}, {p}, {p}, {p}, {p})
        """
        values = (record_id, failure_reason, error_detail, pipeline_run_id, int(retried), _utc_now())

        if DB_TYPE == "postgres":
            cursor = conn.cursor()
            cursor.execute(sql, values)
        else:
            conn.execute(sql, values)

        conn.commit()
        log.info("Audit entry written -- ID=%d, reason=%s", record_id, failure_reason)
    except Exception as exc:
        log.error("Failed to write audit entry for ID=%d: %s", record_id, exc)


def fetch_audit_summary(conn, pipeline_run_id: str) -> None:
    p = _ph()
    sql = f"""
        SELECT failure_reason, COUNT(*) AS cnt
        FROM ingestion_audit_logs
        WHERE pipeline_run_id = {p}
        GROUP BY failure_reason
        ORDER BY cnt DESC
    """
    if DB_TYPE == "postgres":
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql, (pipeline_run_id,))
        rows = cursor.fetchall()
    else:
        rows = conn.execute(sql, (pipeline_run_id,)).fetchall()

    if not rows:
        log.info("No failures recorded for run %s.", pipeline_run_id[:8])
        return

    log.info("Audit summary for run %s:", pipeline_run_id[:8])
    for row in rows:
        log.info("  %s: %d", row["failure_reason"], row["cnt"])


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
