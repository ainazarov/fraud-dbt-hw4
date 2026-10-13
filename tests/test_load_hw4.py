"""Exercise snapshot replacement and failure recovery without a server."""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from scripts.load_hw4 import (
    TABLE, import_lock, labelled_totals, publish_snapshot, run_import,
)
from scripts.produce import ValidationError, preflight_csv


class MemoryClickHouse:
    def __init__(self, *, fail_insert: int | None = None, wrong_totals: bool = False):
        self.tables = {TABLE: [{"transaction_id": "previous", "amount": "3.00", "target": 0}]}
        self.statements: list[str] = []
        self.inserts = 0
        self.fail_insert = fail_insert
        self.wrong_totals = wrong_totals

    def query(self, sql, **_kwargs):
        self.statements.append(sql)
        if sql.startswith("CREATE TABLE"):
            self.tables.setdefault(sql.split()[5], [])
        elif sql.startswith("INSERT INTO"):
            self.inserts += 1
            if self.inserts == self.fail_insert:
                raise RuntimeError("simulated insertion failure")
            table = sql.split()[2]
            self.tables[table].extend(json.loads(line) for line in sql.splitlines()[1:])
        elif sql.startswith("EXCHANGE TABLES"):
            first, second = sql.split()[2], sql.split()[4]
            self.tables[first], self.tables[second] = self.tables[second], self.tables[first]
        elif sql.startswith("DROP TABLE"):
            self.tables.pop(sql.split()[4], None)
        return ""

    def rows(self, sql):
        if "system.databases" in sql:
            return [{"engine": "Atomic"}]
        rows = self.tables[sql.split()[-1]]
        return [{
            "rows": len(rows) + int(self.wrong_totals),
            "unique_ids": len({row["transaction_id"] for row in rows}),
            "amount": str(sum((Decimal(row["amount"]) for row in rows), Decimal(0))),
            "fraud": sum(row["target"] for row in rows),
        }]


class SnapshotImportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.lock = self.directory / "import.lock"
        self.receipt = self.directory / "receipt.json"
        with (Path(__file__).parents[1] / "data" / "demo.csv").open(newline="", encoding="utf-8") as stream:
            self.rows = list(csv.DictReader(stream))[:3]

    def write_csv(self, rows, *, fields=None):
        destination = self.directory / "input.csv"
        with destination.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields or list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        return destination

    def load(self, source, client):
        return run_import(source, dataset="demo", manifest_path=self.receipt,
                          lock_path=self.lock, client=client)

    def test_repeated_import_replaces_snapshot_without_duplicate_ids(self):
        source = self.write_csv(self.rows)
        client = MemoryClickHouse()
        first = self.load(source, client)
        ids = [row["transaction_id"] for row in client.tables[TABLE]]
        second = self.load(source, client)
        self.assertEqual([row["transaction_id"] for row in client.tables[TABLE]], ids)
        self.assertEqual(first["input_sha256"], second["input_sha256"])
        self.assertEqual(len(client.tables[TABLE]), 3)
        self.assertEqual(second["amount"], "1532.15")
        self.assertEqual(second["fraud"], 1)
        self.assertEqual(set(client.tables), {TABLE})
        self.assertEqual(sum(sql.startswith("EXCHANGE TABLES") for sql in client.statements), 2)
        self.assertEqual(json.loads(self.receipt.read_text())["rows"], 3)

    def test_duplicate_valued_rows_remain_distinct_source_records(self):
        source = self.write_csv([self.rows[0], self.rows[0]])
        client = MemoryClickHouse()
        result = self.load(source, client)
        self.assertEqual(result["rows"], 2)
        self.assertEqual(result["amount"], "64.30")
        self.assertNotEqual(*[row["transaction_id"] for row in client.tables[TABLE]])

    def test_missing_or_null_target_is_rejected_before_any_database_write(self):
        for target in (None, "", "null"):
            with self.subTest(target=target):
                rows = [dict(row) for row in self.rows]
                if target is None:
                    for row in rows:
                        row.pop("target")
                else:
                    rows[-1]["target"] = target
                client = MemoryClickHouse()
                with self.assertRaises(ValidationError):
                    self.load(self.write_csv(rows), client)
                self.assertEqual(client.statements, [])
                self.assertFalse(self.receipt.exists())

    def test_late_invalid_row_preserves_source_and_previous_receipt(self):
        source = self.write_csv(self.rows)
        client = MemoryClickHouse()
        self.load(source, client)
        previous = list(client.tables[TABLE])
        receipt = self.receipt.read_bytes()
        self.rows[-1]["amount"] = "-1"
        source = self.write_csv(self.rows)
        with self.assertRaises(ValidationError):
            self.load(source, client)
        self.assertEqual(client.tables[TABLE], previous)
        self.assertEqual(self.receipt.read_bytes(), receipt)

    def test_partial_insert_failure_never_exchanges_active_table(self):
        source = self.write_csv(self.rows)
        client = MemoryClickHouse(fail_insert=2)
        previous = list(client.tables[TABLE])
        with preflight_csv(source, dataset="demo") as prepared:
            with self.assertRaisesRegex(RuntimeError, "insertion failure"):
                publish_snapshot(client, prepared, labelled_totals(prepared), batch_rows=1)
        self.assertEqual(client.tables[TABLE], previous)
        self.assertEqual(set(client.tables), {TABLE})
        self.assertFalse(any(sql.startswith("EXCHANGE TABLES") for sql in client.statements))

    def test_reconciliation_failure_never_exchanges_active_table(self):
        source = self.write_csv(self.rows)
        client = MemoryClickHouse(wrong_totals=True)
        previous = list(client.tables[TABLE])
        with self.assertRaisesRegex(RuntimeError, "totals differ"):
            self.load(source, client)
        self.assertEqual(client.tables[TABLE], previous)
        self.assertEqual(set(client.tables), {TABLE})
        self.assertFalse(self.receipt.exists())

    def test_lock_excludes_concurrent_loader_and_is_released_on_error(self):
        with import_lock(self.lock):
            with self.assertRaises(TimeoutError):
                with import_lock(self.lock, timeout=0):
                    self.fail("concurrent lock acquired")
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            with import_lock(self.lock, timeout=0):
                raise RuntimeError("interrupted")
        with import_lock(self.lock, timeout=0):
            pass


if __name__ == "__main__":
    unittest.main()
