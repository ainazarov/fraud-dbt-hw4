"""Tests for the independent verification oracle, without Kafka/ClickHouse."""

import csv
import hashlib
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from verify import CSV_FIELDS, assert_maximums, normalize_results, query_maximums, read_reference, validate_samples, wait_for_rows


class ReferenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "fixture.csv"

    def write_rows(self, values, target=True):
        fields = list(CSV_FIELDS) + (["target"] if target else [])
        with self.path.open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader()
            for changes in values:
                row = {
                    "transaction_time": "2020-01-01 01:02", "merch": "Merchant",
                    "cat_id": "grocery", "amount": "1.00", "name_1": "Ada", "name_2": "Lovelace",
                    "gender": "F", "street": "1 Main St", "one_city": "Boston", "us_state": "MA",
                    "post_code": "00123", "lat": "42.2", "lon": "-71.2", "population_city": "123",
                    "jobs": "Engineer", "merchant_lat": "42.3", "merchant_lon": "-71.3",
                }
                if target:
                    row["target"] = "0"
                row.update(changes)
                writer.writerow(row)

    def test_quoted_commas_ties_and_decimal_order(self):
        self.write_rows([
            {"merch": 'Merchant, "quoted" name', "amount": "9.99", "cat_id": "a"},
            {"amount": "10.10", "cat_id": "a"},
            {"amount": "10.1", "cat_id": "z"},
            {"amount": "10.10", "cat_id": "b"},
            {"us_state": "NY", "amount": "0.30", "jobs": "Editor, film/video"},
            {"us_state": "NY", "amount": "0.29", "cat_id": "z"},
        ])
        result = read_reference(self.path)
        self.assertEqual(result.row_count, 6)
        self.assertEqual(result.maximums, {"MA": (Decimal("10.10"), "z"), "NY": (Decimal("0.30"), "grocery")})
        self.assertEqual(result.samples[1]["merch"], 'Merchant, "quoted" name')
        self.assertEqual(result.samples[1]["post_code"], "00123")
        self.assertEqual(result.null_target_count, 0)
        self.assertEqual(result.source_sha256, hashlib.sha256(self.path.read_bytes()).hexdigest())

    def test_unlabelled_test_and_limit(self):
        self.write_rows([{"amount": "1.00"}, {"amount": "2.00"}, {"amount": "99.00"}], target=False)
        result = read_reference(self.path, limit=2)
        self.assertEqual(result.row_count, 2)
        self.assertEqual(result.null_target_count, 2)
        self.assertEqual(result.maximums["MA"][0], Decimal("2.00"))
        self.assertEqual(read_reference(self.path, limit=4).row_count, 3)

    def test_explicit_null_target(self):
        self.write_rows([{"target": "null"}, {"target": "  "}, {"target": "1"}])
        self.assertEqual(read_reference(self.path).null_target_count, 2)

    def test_sample_validation_normalizes_utc_and_nullable_target(self):
        self.write_rows([{"target": "null", "transaction_time": "2020-01-01 04:02:00+03:00"}])
        reference = read_reference(self.path)
        row = dict(reference.samples[1], transaction_time="2020-01-01 01:02:00", target=None,
                   row_number=1, transaction_id=hashlib.sha256(
                       ("train\0" + reference.source_sha256 + "\0" + "1").encode()).hexdigest())
        class SampleClient:
            def rows(self, sql, **kwargs):
                return [row]
        self.assertEqual(validate_samples(SampleClient(), "train", reference), 1)

    def test_aggregate_waits_for_view_convergence_but_rejects_wrong_results(self):
        class EventualClient:
            calls = 0
            def rows(self, sql, **kwargs):
                self.calls += 1
                return [] if self.calls == 1 else [{"us_state": "MA", "cat_id": "x", "max_amount": "2.00"}]
        expected = {"MA": (Decimal("2.00"), "x")}
        with patch("verify.time.sleep"):
            self.assertEqual(query_maximums(EventualClient(), "aggregate", "train", expected, timeout=1), expected)
        with self.assertRaisesRegex(AssertionError, "mismatch"):
            query_maximums(EventualClient(), "aggregate", "train", expected, timeout=0)

    def test_bad_precision_is_not_rounded_silently(self):
        self.write_rows([{"amount": "0.001"}])
        with self.assertRaisesRegex(ValueError, "cents"):
            read_reference(self.path)

    def test_result_mismatch_and_duplicate_state_fail(self):
        rows = [{"us_state": "MA", "cat_id": "grocery", "max_amount": "10.10"}]
        actual = normalize_results(rows)
        self.assertEqual(actual, {"MA": (Decimal("10.10"), "grocery")})
        with self.assertRaisesRegex(AssertionError, "Duplicate state"):
            normalize_results(rows + rows)
        with self.assertRaisesRegex(AssertionError, "mismatch"):
            assert_maximums(actual, {"MA": (Decimal("10.11"), "grocery")}, "test")

    def test_duplicate_transactions_fail_before_wait_completes(self):
        class DuplicateClient:
            def rows(self, sql, **kwargs):
                return [{"rows": 3, "unique_transactions": 2, "unique_row_numbers": 2,
                         "first_row": 1, "last_row": 2, "null_targets": 0}]
        with self.assertRaisesRegex(AssertionError, "Duplicate rows"):
            wait_for_rows(DuplicateClient(), "train", expected=10, timeout=1)

    def test_missing_rows_fail(self):
        class MissingClient:
            def rows(self, sql, **kwargs):
                return [{"rows": 2, "unique_transactions": 2, "unique_row_numbers": 2,
                         "first_row": 1, "last_row": 2, "null_targets": 0}]
        with self.assertRaisesRegex(TimeoutError, "Timed out"):
            wait_for_rows(MissingClient(), "train", expected=3, timeout=0)


if __name__ == "__main__":
    unittest.main()
