# guardian-ingestor

A resilient Python ETL pipeline that fetches records from a REST API, validates and transforms them, and persists them to a database — with built-in handling for missing or late-deleted source records.

Built this after running into a race condition during my internship where upstream deletions were crashing the ingestion job mid-run. The fix was straightforward once diagnosed, but it highlighted how brittle pipelines get when they assume the source is always in a consistent state.

---

## What it does

- Fetches records from a REST API in a configurable batch
- Validates and transforms each record before writing to the database
- On 404 (record deleted upstream mid-run): logs the failure to an audit table and continues — does not crash
- Simulates late-arriving deletions via a chaos module for local testing
- Stores successful records in a `records` table and all failures in `ingestion_audit_logs`

---

## Project structure

```
src/
├── config.py         # all config in one place
├── logger.py         # rotating file + console logger
├── chaos.py          # simulates upstream record deletions
├── fetcher.py        # HTTP client with retry and 404 handling
├── transformer.py    # schema validation and string sanitisation
├── database.py       # SQLite / PostgreSQL storage layer
├── pipeline.py       # main ETL loop
├── main.py           # entry point
└── test_pipeline.py  # unit tests
```

---

## Running locally

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
python src/main.py
```

## Running tests

```bash
python -m pytest src/test_pipeline.py -v
```

---

## Running with Docker (PostgreSQL)

```bash
docker compose up --build
```

This spins up a PostgreSQL container and runs the pipeline against it. The `DB_TYPE` environment variable controls which backend is used — `sqlite` for local dev, `postgres` for Docker.

---

## Configuration

Edit `src/config.py` to tune behaviour:

| Setting | Default | Description |
|---|---|---|
| `TOTAL_RECORDS_TO_FETCH` | `20` | Batch size |
| `CHAOS_DELETION_PROBABILITY` | `0.25` | Fraction of records simulated as deleted |
| `HANDLE_MISSING_RECORDS` | `True` | If False, pipeline crashes on 404 (pre-fix behaviour) |
| `MAX_RETRY_ATTEMPTS` | `3` | Retries on server errors |
| `DB_TYPE` | `sqlite` | `sqlite` or `postgres` |

---

## Database schema

**`records`** — successfully ingested records

| Column | Type |
|---|---|
| id | INTEGER PK |
| user_id | INTEGER |
| title | TEXT |
| body | TEXT |
| fetched_at | TEXT (ISO-8601) |

**`ingestion_audit_logs`** — all failed fetch attempts

| Column | Type |
|---|---|
| audit_id | SERIAL PK |
| record_id | INTEGER |
| failure_reason | TEXT |
| error_detail | TEXT |
| pipeline_run_id | TEXT |
| retried | INTEGER |
| failed_at | TEXT (ISO-8601) |

Each pipeline run gets a UUID (`pipeline_run_id`) so failures from different runs can be queried independently.

---

## Notes

The `HANDLE_MISSING_RECORDS` flag toggles between the fixed and pre-fix behaviour. Set it to `False` to watch the pipeline crash on the first 404 — useful for demonstrating what the fix actually changed.

The chaos module uses a Bernoulli trial per record, so the number of simulated deletions follows a binomial distribution. This is closer to how real upstream deletes cluster in practice than a fixed count would be.
