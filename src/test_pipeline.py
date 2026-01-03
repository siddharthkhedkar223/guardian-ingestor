"""
test_pipeline.py - Unit tests for the Guardian Ingestor.

Run with: python -m pytest src/test_pipeline.py -v
"""

import sys
import os
import unittest
import sqlite3
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import config


class TestChaosModule(unittest.TestCase):

    def test_no_deletions_when_probability_zero(self):
        import importlib
        import chaos as chaos_mod
        with patch.object(config, "CHAOS_DELETION_PROBABILITY", 0.0):
            importlib.reload(chaos_mod)
            result = chaos_mod.build_chaos_set(list(range(1, 11)))
        self.assertEqual(len(result), 0)

    def test_all_deletions_when_probability_one(self):
        import importlib
        import chaos as chaos_mod
        with patch.object(config, "CHAOS_DELETION_PROBABILITY", 1.0):
            importlib.reload(chaos_mod)
            result = chaos_mod.build_chaos_set([1, 2, 3])
        self.assertEqual(result, {1, 2, 3})

    def test_is_chaos_deleted(self):
        from chaos import is_chaos_deleted
        self.assertTrue(is_chaos_deleted(5, {3, 5, 7}))
        self.assertFalse(is_chaos_deleted(4, {3, 5, 7}))


class TestFetcher(unittest.TestCase):

    def test_not_found_raises_record_not_found_when_flag_true(self):
        import importlib
        import fetcher
        with patch.object(config, "HANDLE_MISSING_RECORDS", True):
            importlib.reload(fetcher)
            with self.assertRaises(fetcher.RecordNotFoundException) as ctx:
                fetcher._handle_not_found(42, '{"error": "deleted"}')
            self.assertEqual(ctx.exception.record_id, 42)

    def test_not_found_raises_http_error_when_flag_false(self):
        import importlib
        import fetcher
        import requests
        with patch.object(config, "HANDLE_MISSING_RECORDS", False):
            importlib.reload(fetcher)
            with self.assertRaises(requests.HTTPError):
                fetcher._handle_not_found(99, "")

    def test_successful_fetch_returns_dict(self):
        import importlib
        import fetcher
        importlib.reload(fetcher)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "id": 1, "userId": 1, "title": "test title", "body": "test body"
        }
        with patch.object(fetcher._SESSION, "get", return_value=mock_resp):
            result = fetcher.fetch_record(1, chaos_deleted=False)
        self.assertEqual(result["id"], 1)

    def test_chaos_deleted_raises_record_not_found(self):
        import importlib
        import fetcher
        with patch.object(config, "HANDLE_MISSING_RECORDS", True):
            importlib.reload(fetcher)
            with self.assertRaises(fetcher.RecordNotFoundException):
                fetcher.fetch_record(7, chaos_deleted=True)


class TestTransformer(unittest.TestCase):

    def _valid(self):
        return {"id": 1, "userId": 1, "title": "Hello World", "body": "Some text"}

    def test_valid_record_passes(self):
        from transformer import validate_and_transform
        result = validate_and_transform(self._valid())
        self.assertEqual(result["id"], 1)

    def test_missing_field_raises_error(self):
        from transformer import validate_and_transform, TransformationError
        bad = self._valid()
        del bad["title"]
        with self.assertRaises(TransformationError):
            validate_and_transform(bad)

    def test_wrong_type_raises_error(self):
        from transformer import validate_and_transform, TransformationError
        bad = self._valid()
        bad["id"] = "not_an_int"
        with self.assertRaises(TransformationError):
            validate_and_transform(bad)

    def test_whitespace_is_sanitised(self):
        from transformer import validate_and_transform
        raw = self._valid()
        raw["title"] = "  hello    world  "
        result = validate_and_transform(raw)
        self.assertEqual(result["title"], "hello world")


class TestDatabase(unittest.TestCase):

    def _make_db(self):
        import importlib
        import database
        with patch.object(config, "DB_PATH", ":memory:"), \
             patch.object(config, "DB_TYPE", "sqlite"):
            importlib.reload(database)
            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            database.initialize_schema(conn)
        return conn, database

    def test_upsert_record(self):
        conn, db = self._make_db()
        db.upsert_record(conn, {"id": 1, "userId": 1, "title": "T", "body": "B"})
        row = conn.execute("SELECT * FROM records WHERE id=1").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["title"], "T")

    def test_upsert_is_idempotent(self):
        conn, db = self._make_db()
        db.upsert_record(conn, {"id": 2, "userId": 1, "title": "Old", "body": "B"})
        db.upsert_record(conn, {"id": 2, "userId": 1, "title": "New", "body": "B"})
        count = conn.execute("SELECT COUNT(*) FROM records WHERE id=2").fetchone()[0]
        self.assertEqual(count, 1)

    def test_audit_log_written(self):
        conn, db = self._make_db()
        db.log_audit_failure(conn, 42, "Source Deleted", "{}", "run-abc", False)
        row = conn.execute(
            "SELECT * FROM ingestion_audit_logs WHERE record_id=42"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["failure_reason"], "Source Deleted")


if __name__ == "__main__":
    unittest.main(verbosity=2)
