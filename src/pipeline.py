"""
pipeline.py - Main ETL orchestration loop.

Fetch -> Transform -> Load, with error handling for:
  - Missing records (404): logged to audit table, pipeline continues
  - Transient errors (5xx / timeout): logged to audit table
  - Transform errors (bad schema): logged to audit table
  - Unexpected errors: logged and audited, pipeline continues
"""

import uuid
from typing import List

from config import TOTAL_RECORDS_TO_FETCH, HANDLE_MISSING_RECORDS
from chaos import build_chaos_set, is_chaos_deleted
from fetcher import fetch_record, RecordNotFoundException, TransientFetchError
from transformer import validate_and_transform, TransformationError
from database import (
    get_connection,
    initialize_schema,
    upsert_record,
    log_audit_failure,
    fetch_audit_summary,
)
from logger import get_logger

log = get_logger("pipeline")


def run_pipeline() -> None:
    pipeline_run_id: str = str(uuid.uuid4())

    log.info("=" * 60)
    log.info("Guardian Ingestor starting. Run ID: %s", pipeline_run_id)
    log.info("HANDLE_MISSING_RECORDS = %s", HANDLE_MISSING_RECORDS)
    log.info("=" * 60)

    conn = get_connection()
    initialize_schema(conn)

    record_ids: List[int] = list(range(1, TOTAL_RECORDS_TO_FETCH + 1))
    log.info("Attempting to ingest %d records.", len(record_ids))

    chaos_deleted_set = build_chaos_set(record_ids)

    success_count = 0
    skip_count = 0
    error_count = 0

    for rid in record_ids:
        chaos_hit = is_chaos_deleted(rid, chaos_deleted_set)

        try:
            raw_record = fetch_record(record_id=rid, chaos_deleted=chaos_hit)

            if raw_record is None:
                skip_count += 1
                continue

            clean_record = validate_and_transform(raw_record)
            upsert_record(conn, clean_record)
            success_count += 1

        except RecordNotFoundException as exc:
            log.warning("Record ID=%d not found on source. Logging to audit.", exc.record_id)
            log_audit_failure(
                conn=conn,
                record_id=exc.record_id,
                failure_reason="Source Deleted",
                error_detail=exc.error_body,
                pipeline_run_id=pipeline_run_id,
                retried=False,
            )
            skip_count += 1

        except TransientFetchError as exc:
            log.error("Transient error for Record ID=%d: %s", rid, exc)
            log_audit_failure(
                conn=conn,
                record_id=rid,
                failure_reason="Transient Error",
                error_detail=str(exc),
                pipeline_run_id=pipeline_run_id,
                retried=True,
            )
            error_count += 1

        except TransformationError as exc:
            log.error("Transform error for Record ID=%d: %s", rid, exc)
            log_audit_failure(
                conn=conn,
                record_id=rid,
                failure_reason="Transform Failure",
                error_detail=str(exc),
                pipeline_run_id=pipeline_run_id,
                retried=False,
            )
            error_count += 1

        except Exception as exc:
            log.critical("Unexpected error for Record ID=%d: %s", rid, exc, exc_info=True)
            log_audit_failure(
                conn=conn,
                record_id=rid,
                failure_reason="Unexpected Error",
                error_detail=str(exc),
                pipeline_run_id=pipeline_run_id,
                retried=False,
            )
            error_count += 1

    log.info("=" * 60)
    log.info("Pipeline complete. Run ID: %s", pipeline_run_id[:8])
    log.info("  Ingested : %d", success_count)
    log.info("  Skipped  : %d", skip_count)
    log.info("  Errors   : %d", error_count)
    log.info("=" * 60)

    fetch_audit_summary(conn, pipeline_run_id)
    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    run_pipeline()
