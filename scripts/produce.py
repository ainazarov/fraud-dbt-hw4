#!/usr/bin/env python3
"""Validate a CSV completely before streaming its selected rows to Kafka.

The input is snapshotted and hashed on disk, so transaction identities describe
exactly the bytes that were validated even if the original file changes. Both
the snapshot and the validated JSONL spool are temporary; RAM usage is bounded.
Validation and unit tests require only the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, DecimalException
from itertools import islice
from pathlib import Path
from typing import Any, Callable, Iterator


BASE_COLUMNS = (
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
    "gender", "street", "one_city", "us_state", "post_code", "lat", "lon",
    "population_city", "jobs", "merchant_lat", "merchant_lon",
)
DATASET_RE = re.compile(r"[a-z][a-z0-9_]{0,31}\Z")
TIME_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}"
    r"(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+-]\d{2}:\d{2})?\Z"
)
MAX_AMOUNT = Decimal("9999999999.99")
CENT = Decimal("0.01")
UINT32_MAX = 2**32 - 1
UINT64_MAX = 2**64 - 1


class ValidationError(ValueError):
    """The CSV cannot be represented by the destination schema."""


class PublishError(RuntimeError):
    """Kafka has not acknowledged every selected record."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def validate_dataset(dataset: str) -> str:
    if not DATASET_RE.fullmatch(dataset):
        raise ValidationError("dataset must match [a-z][a-z0-9_]{0,31}")
    return dataset


def transaction_id(dataset: str, input_sha256: str, row_number: int) -> str:
    """Stable identity: UTF-8 dataset, NUL, file SHA256, NUL, 1-based row."""
    identity = f"{dataset}\0{input_sha256}\0{row_number}".encode("utf-8")
    return hashlib.sha256(identity).hexdigest()


def normalize_row(
    row: dict[str, str], *, dataset: str, input_sha256: str, row_number: int
) -> dict[str, Any]:
    """Keep original field names, with explicitly typed destination values."""
    if not 1 <= row_number <= UINT64_MAX:
        raise ValidationError("row_number is outside UInt64")
    result: dict[str, Any] = dict(row)

    def invalid(field: str, explanation: str) -> ValidationError:
        # Do not include the source value: CSVs contain personal information.
        return ValidationError(f"row {row_number}, {field}: {explanation}")

    raw_time = row["transaction_time"].strip()
    if not TIME_RE.fullmatch(raw_time):
        raise invalid("transaction_time", "expected an ISO date and time")
    try:
        instant = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=timezone.utc)
        instant = instant.astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise invalid("transaction_time", "invalid date or time") from exc
    if instant.microsecond:
        raise invalid("transaction_time", "subsecond precision is not supported")
    # The destination is DateTime('UTC'), whose underlying value is UInt32.
    if not 0 <= instant.timestamp() <= UINT32_MAX:
        raise invalid("transaction_time", "outside ClickHouse DateTime range")
    result["transaction_time"] = instant.strftime("%Y-%m-%d %H:%M:%S")

    try:
        amount = Decimal(row["amount"].strip())
        if not amount.is_finite() or not 0 <= amount <= MAX_AMOUNT:
            raise invalid("amount", "must be finite and between 0 and 9999999999.99")
        rounded = amount.quantize(CENT)
        if rounded != amount:
            raise invalid("amount", "must have at most two nonzero decimal places")
    except DecimalException as exc:
        raise invalid("amount", "invalid decimal amount") from exc
    result["amount"] = format(abs(rounded) if rounded == 0 else rounded, ".2f")

    if re.fullmatch(r"[A-Z]{2}", row["us_state"]) is None:
        raise invalid("us_state", "expected exactly two uppercase ASCII letters")
    if not row["post_code"]:
        raise invalid("post_code", "must not be empty")

    for field, bound in (("lat", 90), ("lon", 180),
                         ("merchant_lat", 90), ("merchant_lon", 180)):
        try:
            value = float(row[field])
        except (ValueError, OverflowError) as exc:
            raise invalid(field, "expected a numeric coordinate") from exc
        if not math.isfinite(value) or not -bound <= value <= bound:
            raise invalid(field, f"must be finite and in [-{bound}, {bound}]")
        result[field] = value

    raw_population = row["population_city"].strip()
    if re.fullmatch(r"[0-9]+", raw_population) is None:
        raise invalid("population_city", "expected a nonnegative integer")
    try:
        population = int(raw_population)
    except ValueError as exc:
        raise invalid("population_city", "invalid integer") from exc
    if population > UINT32_MAX:
        raise invalid("population_city", "outside UInt32")
    result["population_city"] = population

    target = row.get("target", "").strip()
    if target in ("", "null"):
        result["target"] = None
    elif target in ("0", "1"):
        result["target"] = int(target)
    else:
        raise invalid("target", "expected 0, 1, empty, or null")
    result["dataset"] = dataset
    result["transaction_id"] = transaction_id(dataset, input_sha256, row_number)
    result["row_number"] = row_number
    return result


@dataclass(frozen=True)
class PreparedCsv:
    input_file: Path
    input_sha256: str
    input_bytes: int
    dataset: str
    rows: int
    spool_path: Path

    def messages(self) -> Iterator[tuple[str, bytes]]:
        with self.spool_path.open("rb") as stream:
            for row_number, line in enumerate(stream, start=1):
                key = transaction_id(self.dataset, self.input_sha256, row_number)
                yield key, line.rstrip(b"\n")


@contextmanager
def preflight_csv(
    csv_path: str | Path, *, dataset: str | None = None, limit: int | None = None
) -> Iterator[PreparedCsv]:
    """Snapshot, hash and validate all selected rows before yielding anything."""
    source = Path(csv_path).resolve()
    dataset = validate_dataset(source.stem if dataset is None else dataset)
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 1):
        raise ValidationError("limit must be a positive integer")
    with tempfile.TemporaryDirectory(prefix="hw2-kafka-") as temporary:
        snapshot = Path(temporary) / "input.csv"
        spool = Path(temporary) / "validated.jsonl"
        digest = hashlib.sha256()
        input_bytes = 0
        with source.open("rb") as original, snapshot.open("wb") as destination:
            while chunk := original.read(1024 * 1024):
                digest.update(chunk)
                input_bytes += len(chunk)
                destination.write(chunk)
        input_sha256 = digest.hexdigest()
        count = 0
        try:
            with snapshot.open("r", encoding="utf-8-sig", newline="") as csv_stream, \
                    spool.open("w", encoding="utf-8", newline="\n") as json_stream:
                reader = csv.DictReader(csv_stream, strict=True)
                header = reader.fieldnames
                if not header:
                    raise ValidationError("CSV has no header")
                if len(header) != len(set(header)):
                    raise ValidationError("CSV contains duplicate column names")
                missing = set(BASE_COLUMNS) - set(header)
                extra = set(header) - set(BASE_COLUMNS) - {"target"}
                if missing or extra:
                    raise ValidationError(
                        f"CSV header mismatch: missing={sorted(missing)}, unexpected={sorted(extra)}"
                    )
                selected_rows = reader if limit is None else islice(reader, limit)
                for row_number, row in enumerate(selected_rows, start=1):
                    if None in row or any(value is None for value in row.values()):
                        raise ValidationError(f"row {row_number}: field count differs from header")
                    normalized = normalize_row(
                        row, dataset=dataset, input_sha256=input_sha256, row_number=row_number
                    )
                    json_stream.write(json.dumps(
                        normalized, ensure_ascii=False, allow_nan=False, separators=(",", ":")
                    ) + "\n")
                    count += 1
        except UnicodeError as exc:
            raise ValidationError("CSV must use UTF-8 encoding") from exc
        except csv.Error as exc:
            raise ValidationError("CSV contains invalid quoting or an oversized field") from exc
        if not count:
            raise ValidationError("CSV contains no data rows")
        yield PreparedCsv(source, input_sha256, input_bytes, dataset, count, spool)


def publish_prepared(
    prepared: PreparedCsv, *, bootstrap_servers: str, topic: str,
    producer_factory: Callable[[dict[str, Any]], Any] | None = None,
    timeout_seconds: float = 120,
) -> dict[str, Any]:
    """Return a manifest only after callbacks acknowledge every record."""
    if producer_factory is None:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise PublishError("Install requirements.txt before publishing to Kafka") from exc
        producer_factory = Producer
    if not topic or not bootstrap_servers:
        raise PublishError("Kafka topic and bootstrap servers must not be empty")
    config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": f"hw2-csv-{prepared.dataset}",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 10,
        "queue.buffering.max.messages": 10000,
        "delivery.timeout.ms": int(timeout_seconds * 1000),
        "request.timeout.ms": min(30000, int(timeout_seconds * 1000)),
    }
    delivered = 0
    errors = 0
    first_error: str | None = None

    def delivery_report(error: Any, _message: Any) -> None:
        nonlocal delivered, errors, first_error
        if error is None:
            delivered += 1
        else:
            errors += 1
            if first_error is None:
                first_error = str(error)

    started_utc = utc_now()
    started = time.monotonic()
    producer = producer_factory(config)
    enqueued = 0
    for key, value in prepared.messages():
        deadline = time.monotonic() + timeout_seconds
        while True:
            if errors:
                raise PublishError(f"Kafka delivery failed ({errors} errors): {first_error}")
            try:
                producer.produce(topic, key=key.encode("ascii"), value=value,
                                 on_delivery=delivery_report)
                enqueued += 1
                break
            except BufferError:
                if time.monotonic() >= deadline:
                    raise PublishError("Kafka producer queue remained full until timeout")
                producer.poll(0.1)
        producer.poll(0)
    outstanding = producer.flush(timeout_seconds)
    if outstanding or errors or delivered != prepared.rows:
        raise PublishError(
            f"Kafka delivery incomplete: selected={prepared.rows}, enqueued={enqueued}, "
            f"acknowledged={delivered}, errors={errors}, pending={outstanding}"
            + (f"; first error: {first_error}" if first_error else "")
        )
    return {
        "dataset": prepared.dataset,
        "input_file": str(prepared.input_file),
        "input_sha256": prepared.input_sha256,
        "input_bytes": prepared.input_bytes,
        "rows": prepared.rows,
        "enqueued": enqueued,
        "delivered": delivered,
        "delivery_errors": errors,
        "topic": topic,
        "started_utc": started_utc,
        "finished_utc": utc_now(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "transaction_id_scheme": "sha256(dataset + NUL + input_sha256 + NUL + row_number)",
    }


def write_manifest(path: str | Path, manifest: dict[str, Any]) -> None:
    """Replace the destination atomically, only after successful delivery."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=destination.parent,
            prefix=f".{destination.name}.", suffix=".tmp", delete=False,
        ) as stream:
            temporary = stream.name
            json.dump(manifest, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        # NamedTemporaryFile defaults to 0600; Docker runs as root by default.
        # The receipt contains no credentials and must be readable on the host.
        os.chmod(temporary, 0o644)
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def run_import(
    csv_path: str | Path, *, dataset: str | None = None,
    bootstrap_servers: str = "kafka:9092", topic: str = "transactions",
    limit: int | None = None, manifest_path: str | Path | None = None,
    producer_factory: Callable[[dict[str, Any]], Any] | None = None,
) -> dict[str, Any]:
    with preflight_csv(csv_path, dataset=dataset, limit=limit) as prepared:
        manifest = publish_prepared(
            prepared, bootstrap_servers=bootstrap_servers, topic=topic,
            producer_factory=producer_factory,
        )
    if manifest_path is not None:
        write_manifest(manifest_path, manifest)
    return manifest


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--dataset", help="stable dataset name; defaults to the CSV file stem")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions"))
    parser.add_argument("--limit", type=positive_int, help="publish the first N data rows")
    parser.add_argument("--manifest", type=Path, help="write an atomic JSON receipt after every ACK")
    args = parser.parse_args(argv)
    try:
        manifest = run_import(
            args.csv_path, dataset=args.dataset, bootstrap_servers=args.bootstrap_servers,
            topic=args.topic, limit=args.limit, manifest_path=args.manifest,
        )
    except KeyboardInterrupt:
        print("error: interrupted; delivery is incomplete and no new manifest was written", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(manifest, ensure_ascii=False, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
