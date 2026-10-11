"""Pure standard-library tests; no running broker or Kafka dependency required."""

import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.produce import (
    BASE_COLUMNS, PublishError, ValidationError, normalize_row, preflight_csv,
    run_import, transaction_id, validate_dataset,
)


def sample_row(**updates):
    row = dict(zip(BASE_COLUMNS, (
        "2019-09-14 02:46", "Demo, Café", "grocery_net", "25.79",
        "Test", "Person", "F", "1 Fictional Road", "Demo Town", "NY",
        "00501", "40.8", "-73.0", "1234", "Synthetic role", "40.9", "-73.1",
    )))
    row["target"] = "0"
    row.update(updates)
    return row


def normalized(**updates):
    return normalize_row(sample_row(**updates), dataset="demo", input_sha256="a" * 64, row_number=1)


class NormalizeTests(unittest.TestCase):
    def test_keeps_every_original_field_and_string_postcode(self):
        row = normalized()
        self.assertTrue(set(BASE_COLUMNS) <= set(row))
        self.assertEqual(row["post_code"], "00501")
        self.assertEqual(row["merch"], "Demo, Café")
        self.assertEqual(row["transaction_time"], "2019-09-14 02:46:00")
        self.assertEqual(row["amount"], "25.79")
        self.assertEqual(row["target"], 0)
        self.assertEqual(row["population_city"], 1234)
        self.assertEqual(row["lat"], 40.8)
        self.assertEqual(row["dataset"], "demo")
        self.assertEqual(row["row_number"], 1)
        self.assertEqual(len(row["transaction_id"]), 64)

    def test_exact_amount_format(self):
        for value, expected in (("39.4", "39.40"), ("0", "0.00"), ("-0.00", "0.00"),
                                ("1.2300", "1.23"), ("9999999999.99", "9999999999.99")):
            with self.subTest(value=value):
                self.assertEqual(normalized(amount=value)["amount"], expected)

    def test_rejects_invalid_amount_without_rounding(self):
        for value in ("-0.01", "10000000000", "1.001", "NaN", "sNaN", "Infinity", "-inf", "oops", ""):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                normalized(amount=value)

    def test_utc_assumption_and_explicit_timezone(self):
        self.assertEqual(normalized(transaction_time="2019-01-01T03:05:09+03:00")["transaction_time"],
                         "2019-01-01 00:05:09")
        self.assertEqual(normalized(transaction_time="2019-01-01T00:05:09Z")["transaction_time"],
                         "2019-01-01 00:05:09")

    def test_rejects_invalid_or_lossy_timestamp(self):
        for value in ("2019-02-30 00:00", "2019-01-01", "2019-01-01 24:00", "2019-01-01 00:00:00.1",
                      "1969-12-31 23:59:59", "2107-01-01 00:00"):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                normalized(transaction_time=value)

    def test_nullable_target_and_unlabeled_test_data(self):
        for value, expected in (("0", 0), ("1", 1), ("", None), ("null", None)):
            with self.subTest(value=value):
                self.assertEqual(normalized(target=value)["target"], expected)
        row = sample_row()
        del row["target"]
        self.assertIsNone(normalize_row(row, dataset="test", input_sha256="b" * 64, row_number=2)["target"])

    def test_rejects_invalid_target(self):
        for value in ("2", "-1", "true", "0.0", "NaN"):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                normalized(target=value)

    def test_validates_state_without_silent_normalization(self):
        for value in ("ny", "NY ", "N", "NYY", "12", "ÅÄ"):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                normalized(us_state=value)

    def test_coordinate_bounds_and_finiteness(self):
        for field, value in (("lat", "90.01"), ("lon", "-180.01"), ("merchant_lat", "NaN"),
                             ("merchant_lon", "inf"), ("lat", "abc")):
            with self.subTest(field=field, value=value), self.assertRaises(ValidationError):
                normalized(**{field: value})
        self.assertEqual(normalized(lat="-90", lon="180")["lon"], 180)

    def test_population_uint32(self):
        self.assertEqual(normalized(population_city="4294967295")["population_city"], 4294967295)
        for value in ("-1", "1.2", "NaN", "4294967296", ""):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                normalized(population_city=value)

    def test_validation_error_does_not_echo_private_value(self):
        with self.assertRaises(ValidationError) as raised:
            normalized(amount="private-account-information")
        self.assertNotIn("private-account-information", str(raised.exception))

    def test_dataset_names(self):
        for value in ("train", "test", "demo_2", "a" * 32):
            self.assertEqual(validate_dataset(value), value)
        for value in ("", "1train", "Train", "test-data", "a" * 33, "a\n"):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                validate_dataset(value)

    def test_transaction_identity_is_stable_and_separates_inputs(self):
        expected = hashlib.sha256(("demo\0" + "a" * 64 + "\0" + "1").encode()).hexdigest()
        self.assertEqual(transaction_id("demo", "a" * 64, 1), expected)
        self.assertNotEqual(transaction_id("test", "a" * 64, 1), expected)
        self.assertNotEqual(transaction_id("demo", "b" * 64, 1), expected)
        self.assertNotEqual(transaction_id("demo", "a" * 64, 2), expected)


class FakeProducer:
    """Simulate asynchronous ACKs when flush is called."""
    def __init__(self, config):
        self.config = config
        self.records = []
        self.callbacks = []
        self.flush_result = 0
        self.error = None
        self.acknowledge = True
        self.queue_full_once = False
        self.polls = []

    def produce(self, topic, *, key, value, on_delivery):
        if self.queue_full_once:
            self.queue_full_once = False
            raise BufferError("queue full")
        self.records.append((topic, key, value))
        self.callbacks.append(on_delivery)

    def poll(self, seconds):
        self.polls.append(seconds)

    def flush(self, _timeout):
        if self.acknowledge:
            for callback in self.callbacks:
                callback(self.error, None)
        return self.flush_result


class CsvAndPublishTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.csv_path = self.root / "demo.csv"
        self.manifest_path = self.root / "receipts" / "demo.json"
        self.producers = []

    def write_csv(self, rows, *, header=None):
        fields = list(BASE_COLUMNS) + ["target"] if header is None else header
        with self.csv_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    def factory(self, **attributes):
        def create(config):
            producer = FakeProducer(config)
            for name, value in attributes.items():
                setattr(producer, name, value)
            self.producers.append(producer)
            return producer
        return create

    def test_stream_preflight_hash_keys_unicode_and_cleanup(self):
        self.write_csv([sample_row(), sample_row(amount="39.4", post_code="00001")])
        expected_hash = hashlib.sha256(self.csv_path.read_bytes()).hexdigest()
        with preflight_csv(self.csv_path) as prepared:
            self.assertEqual(prepared.rows, 2)
            self.assertEqual(prepared.input_sha256, expected_hash)
            messages = list(prepared.messages())
            decoded = [json.loads(message) for _key, message in messages]
            self.assertEqual(decoded[0]["merch"], "Demo, Café")
            self.assertEqual(decoded[1]["post_code"], "00001")
            self.assertEqual(decoded[1]["row_number"], 2)
            self.assertEqual(decoded[1]["amount"], "39.40")
            self.assertEqual(messages[1][0], decoded[1]["transaction_id"])
            spool = prepared.spool_path
        self.assertFalse(spool.exists())

    def test_missing_target_in_test_header(self):
        self.write_csv([sample_row()], header=list(BASE_COLUMNS))
        with preflight_csv(self.csv_path) as prepared:
            self.assertIsNone(json.loads(next(prepared.messages())[1])["target"])

    def test_preflight_failure_sends_nothing_and_writes_no_receipt(self):
        self.write_csv([sample_row(), sample_row(amount="-1")])
        with self.assertRaises(ValidationError):
            run_import(self.csv_path, manifest_path=self.manifest_path, producer_factory=self.factory())
        self.assertEqual(self.producers, [])
        self.assertFalse(self.manifest_path.exists())

    def test_limit_only_validates_and_sends_selected_rows(self):
        self.write_csv([sample_row(), sample_row(amount="invalid")])
        manifest = run_import(self.csv_path, limit=1, producer_factory=self.factory())
        self.assertEqual(manifest["rows"], 1)
        self.assertEqual(manifest["delivered"], 1)
        self.assertEqual(manifest["input_sha256"], hashlib.sha256(self.csv_path.read_bytes()).hexdigest())

    def test_limit_does_not_parse_unselected_malformed_csv(self):
        self.write_csv([sample_row()])
        with self.csv_path.open("a", encoding="utf-8") as stream:
            stream.write('"unterminated')
        with preflight_csv(self.csv_path, limit=1) as prepared:
            self.assertEqual(prepared.rows, 1)

    def test_invalid_limits(self):
        self.write_csv([sample_row()])
        for limit in (0, -1, True, 1.2):
            with self.subTest(limit=limit), self.assertRaises(ValidationError):
                with preflight_csv(self.csv_path, limit=limit):
                    pass

    def test_invalid_header_variants(self):
        for header in (list(BASE_COLUMNS[:-1]), list(BASE_COLUMNS) + ["oops"],
                       list(BASE_COLUMNS) + ["merch"]):
            with self.subTest(header=header):
                self.write_csv([sample_row()], header=header)
                with self.assertRaises(ValidationError):
                    with preflight_csv(self.csv_path):
                        pass

    def test_missing_and_extra_field_values(self):
        for suffix in ("one,two\n", ",".join(["value"] * 20) + "\n"):
            self.write_csv([])
            with self.csv_path.open("a", encoding="utf-8") as stream:
                stream.write(suffix)
            with self.subTest(suffix=suffix), self.assertRaises(ValidationError):
                with preflight_csv(self.csv_path):
                    pass

    def test_empty_file_and_header_only(self):
        self.csv_path.write_text("", encoding="utf-8")
        with self.assertRaises(ValidationError):
            with preflight_csv(self.csv_path):
                pass
        self.write_csv([])
        with self.assertRaises(ValidationError):
            with preflight_csv(self.csv_path):
                pass

    def test_snapshot_is_independent_of_later_source_changes(self):
        self.write_csv([sample_row()])
        with preflight_csv(self.csv_path) as prepared:
            old_hash = prepared.input_sha256
            self.csv_path.write_text("changed after snapshot", encoding="utf-8")
            decoded = json.loads(next(prepared.messages())[1])
            self.assertEqual(decoded["amount"], "25.79")
            self.assertEqual(decoded["transaction_id"], transaction_id("demo", old_hash, 1))

    def test_success_receipt_requires_all_acks_and_records_config(self):
        self.write_csv([sample_row(), sample_row()])
        manifest = run_import(self.csv_path, manifest_path=self.manifest_path,
                              producer_factory=self.factory())
        self.assertEqual(json.loads(self.manifest_path.read_text(encoding="utf-8")), manifest)
        self.assertEqual(manifest["delivered"], 2)
        self.assertEqual(manifest["delivery_errors"], 0)
        self.assertEqual(manifest["enqueued"], 2)
        producer = self.producers[0]
        self.assertTrue(producer.config["enable.idempotence"])
        self.assertEqual(producer.config["acks"], "all")
        self.assertEqual(len(producer.records), 2)
        self.assertNotEqual(producer.records[0][1], producer.records[1][1])
        self.assertEqual(list(self.manifest_path.parent.glob("*.tmp")), [])

    def test_delivery_error_creates_no_manifest(self):
        self.write_csv([sample_row()])
        with self.assertRaises(PublishError):
            run_import(self.csv_path, manifest_path=self.manifest_path,
                       producer_factory=self.factory(error="broker failure"))
        self.assertFalse(self.manifest_path.exists())

    def test_unflushed_messages_are_failure(self):
        self.write_csv([sample_row()])
        with self.assertRaises(PublishError):
            run_import(self.csv_path, manifest_path=self.manifest_path,
                       producer_factory=self.factory(flush_result=1))
        self.assertFalse(self.manifest_path.exists())

    def test_missing_callbacks_are_failure_even_if_flush_returns_zero(self):
        self.write_csv([sample_row()])
        with self.assertRaises(PublishError):
            run_import(self.csv_path, producer_factory=self.factory(acknowledge=False))

    def test_queue_backpressure_is_retried(self):
        self.write_csv([sample_row()])
        manifest = run_import(self.csv_path, producer_factory=self.factory(queue_full_once=True))
        self.assertEqual(manifest["delivered"], 1)
        self.assertIn(0.1, self.producers[0].polls)

    def test_failed_retry_preserves_existing_manifest(self):
        self.write_csv([sample_row()])
        run_import(self.csv_path, manifest_path=self.manifest_path, producer_factory=self.factory())
        old_manifest = self.manifest_path.read_bytes()
        with self.assertRaises(PublishError):
            run_import(self.csv_path, manifest_path=self.manifest_path,
                       producer_factory=self.factory(error="failure"))
        self.assertEqual(self.manifest_path.read_bytes(), old_manifest)


if __name__ == "__main__":
    unittest.main()
