"""Independently check the Kafka -> ClickHouse pipeline against the input CSV.

The oracle intentionally imports no producer code. Monetary comparisons use
Decimal throughout, including parsing ClickHouse JSON numbers.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import itertools
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from ch_client import ClickHouseHTTP, QUERY_FILES, read_query


CSV_FIELDS = (
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
    "gender", "street", "one_city", "us_state", "post_code", "lat", "lon",
    "population_city", "jobs", "merchant_lat", "merchant_lon",
)
FLOAT_FIELDS = {"lat", "lon", "merchant_lat", "merchant_lon"}
STRING_FIELDS = set(CSV_FIELDS) - FLOAT_FIELDS - {"transaction_time", "amount", "population_city"}
RESULT_FIELDS = ("us_state", "cat_id", "max_amount")


@dataclass
class Reference:
    row_count: int
    maximums: dict[str, tuple[Decimal, str]]
    null_target_count: int
    samples: dict[int, dict[str, str]]
    source_sha256: str


def dataset_name(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", value):
        raise argparse.ArgumentTypeError("dataset must match [a-z][a-z0-9_]{0,31}")
    return value


def read_reference(path: Path, limit: int | None = None) -> Reference:
    """Build a streaming CSV/Decimal oracle, preserving quoted commas correctly."""
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    maximums: dict[str, tuple[Decimal, str]] = {}
    samples: dict[int, dict[str, str]] = {}
    row_count = null_target_count = 0
    comma_sampled = False
    last_row: dict[str, str] | None = None
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        missing = set(CSV_FIELDS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing columns: {sorted(missing)}")
        selected_rows = itertools.islice(reader, limit) if limit is not None else reader
        for number, row in enumerate(selected_rows, start=1):
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Malformed CSV row {number}")
            try:
                amount = Decimal(row["amount"])
            except InvalidOperation as exc:
                raise ValueError(f"Invalid amount at CSV row {number}") from exc
            if not amount.is_finite() or amount != amount.quantize(Decimal("0.01")):
                raise ValueError(f"Amount cannot be represented in cents at CSV row {number}")
            state, category = row["us_state"], row["cat_id"]
            if not state or not category:
                raise ValueError(f"Empty state/category at CSV row {number}")
            candidate = (amount, category)
            if state not in maximums or candidate > maximums[state]:
                maximums[state] = candidate
            if row.get("target", "").strip() in {"", "null"}:
                null_target_count += 1
            if number in {1, 2, 3, 1000, 10000, 100000}:
                samples[number] = row
            if not comma_sampled and any("," in value for value in row.values()):
                samples[number] = row
                comma_sampled = True
            row_count = number
            last_row = row
    if not row_count:
        raise ValueError("Input CSV contains no data rows")
    assert last_row is not None
    samples[row_count] = last_row
    return Reference(row_count, maximums, null_target_count, samples, digest.hexdigest())


def normalize_results(rows: list[dict[str, Any]]) -> dict[str, tuple[Decimal, str]]:
    result: dict[str, tuple[Decimal, str]] = {}
    for row in rows:
        if set(row) != set(RESULT_FIELDS):
            raise AssertionError(f"Unexpected result columns: {sorted(row)}")
        state = str(row["us_state"])
        if state in result:
            raise AssertionError(f"Duplicate state in SQL result: {state}")
        result[state] = (Decimal(str(row["max_amount"])), str(row["cat_id"]))
    return result


def assert_maximums(actual: dict[str, tuple[Decimal, str]], expected: dict[str, tuple[Decimal, str]], label: str) -> None:
    differences = {
        state: {"expected": expected.get(state), "actual": actual.get(state)}
        for state in sorted(set(actual) | set(expected))
        if actual.get(state) != expected.get(state)
    }
    if differences:
        raise AssertionError(f"{label} result mismatch: {differences}")


def query_maximums(client: ClickHouseHTTP, name: str, dataset: str, expected: dict[str, tuple[Decimal, str]], timeout: float = 0) -> dict[str, tuple[Decimal, str]]:
    # Source and dependent materialized-view inserts can become visible a little
    # apart. Wait for ingestion convergence, never for background part merges.
    deadline = time.monotonic() + timeout
    while True:
        actual = normalize_results(client.rows(read_query(name), dataset=dataset))
        if actual == expected:
            return actual
        if time.monotonic() >= deadline:
            assert_maximums(actual, expected, name)
        time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))


def table_counts(client: ClickHouseHTTP, dataset: str) -> dict[str, dict[str, int]]:
    result = {}
    for table in ("transactions_baseline", "transactions"):
        row = client.rows(
            f"SELECT count() AS rows, uniqExact(transaction_id) AS unique_transactions, "
            f"uniqExact(row_number) AS unique_row_numbers, min(row_number) AS first_row, "
            f"max(row_number) AS last_row, countIf(isNull(target)) AS null_targets "
            f"FROM fraud.{table} WHERE dataset = {{dataset:String}}",
            dataset=dataset,
        )[0]
        result[table] = {key: int(value) for key, value in row.items()}
    return result


def wait_for_rows(client: ClickHouseHTTP, dataset: str, expected: int, timeout: float) -> dict[str, dict[str, int]]:
    deadline = time.monotonic() + timeout
    while True:
        counts = table_counts(client, dataset)
        for table, values in counts.items():
            duplicates = values["rows"] - values["unique_transactions"]
            if duplicates or values["rows"] != values["unique_row_numbers"]:
                raise AssertionError(f"Duplicate rows in {table}: {values}; duplicate transaction IDs={duplicates}")
            if values["rows"] > expected:
                raise AssertionError(f"Unexpected extra rows in {table}: expected {expected}, got {values}")
        if all(values["rows"] >= expected and values["unique_transactions"] >= expected for values in counts.values()):
            return counts
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for {expected} unique rows in both tables: {counts}")
        time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))


def validate_samples(client: ClickHouseHTTP, dataset: str, reference: Reference) -> int:
    """Check all CSV columns for representative rows in each raw data table."""
    numbers = ",".join(str(number) for number in sorted(reference.samples))
    for table in ("transactions_baseline", "transactions"):
        rows = client.rows(
            f"SELECT * FROM fraud.{table} WHERE dataset = {{dataset:String}} "
            f"AND row_number IN ({numbers}) ORDER BY row_number",
            dataset=dataset,
        )
        if len(rows) != len(reference.samples):
            raise AssertionError(f"{table}: missing or duplicated sample rows")
        for row in rows:
            number = int(row["row_number"])
            source = reference.samples[number]
            expected_id = hashlib.sha256(f"{dataset}\0{reference.source_sha256}\0{number}".encode("utf-8")).hexdigest()
            if row["transaction_id"] != expected_id:
                raise AssertionError(f"{table} row {number}: transaction_id does not match the input identity")
            for field in STRING_FIELDS:
                if row[field] != source[field]:
                    raise AssertionError(f"{table} row {number}: {field} differs")
            for field in FLOAT_FIELDS:
                if abs(Decimal(str(row[field])) - Decimal(source[field])) > Decimal("0.000000001"):
                    raise AssertionError(f"{table} row {number}: {field} differs")
            if Decimal(str(row["amount"])) != Decimal(source["amount"]):
                raise AssertionError(f"{table} row {number}: amount differs")
            if int(row["population_city"]) != int(source["population_city"]):
                raise AssertionError(f"{table} row {number}: population_city differs")
            expected_time = datetime.fromisoformat(source["transaction_time"].strip().replace("Z", "+00:00"))
            if expected_time.tzinfo is not None:
                expected_time = expected_time.astimezone(timezone.utc).replace(tzinfo=None)
            if datetime.fromisoformat(str(row["transaction_time"])) != expected_time:
                raise AssertionError(f"{table} row {number}: transaction_time differs")
            target_text = source.get("target", "").strip()
            expected_target = None if target_text in {"", "null"} else int(target_text)
            actual_target = int(row["target"]) if row["target"] is not None else None
            if actual_target != expected_target:
                raise AssertionError(f"{table} row {number}: target differs")
    return len(reference.samples)


def verify(args: argparse.Namespace, client: ClickHouseHTTP, report: dict[str, Any]) -> None:
    reference = read_reference(args.csv_path, args.limit)
    report.update({
        "source_file": args.csv_path.name,
        "source_sha256": reference.source_sha256,
        "source_sha256_scope": "entire input file, including when --limit is used",
        "limit": args.limit,
        "expected_rows": reference.row_count,
        "state_count": len(reference.maximums),
        "expected_null_targets": reference.null_target_count,
        "tie_policy": "largest (amount, cat_id, transaction_id); exported category uses (amount, cat_id)",
    })
    counts = wait_for_rows(client, args.dataset, reference.row_count, args.timeout)
    report["tables"] = counts
    for table, values in counts.items():
        if values["rows"] != reference.row_count or values["unique_transactions"] != reference.row_count:
            raise AssertionError(f"{table}: exact counts failed: {values}")
        if values["first_row"] != 1 or values["last_row"] != reference.row_count:
            raise AssertionError(f"{table}: row_number coverage failed: {values}")
        if values["null_targets"] != reference.null_target_count:
            raise AssertionError(f"{table}: target NULL count failed: {values}")
    report["sample_rows_checked_per_table"] = validate_samples(client, args.dataset, reference)
    results = {}
    for name in QUERY_FILES:
        results[name] = query_maximums(
            client, name, args.dataset, reference.maximums,
            timeout=min(args.timeout, 10) if name == "aggregate" else 0,
        )
    assert_maximums(results["aggregate"], results["raw"], "aggregate versus raw")
    assert_maximums(results["raw"], results["baseline"], "raw versus baseline")
    # Fetch the submitted CSV from ClickHouse itself, then verify that exact export.
    exported = client.query(read_query("aggregate"), dataset=args.dataset, output_format="CSVWithNames")
    assert_maximums(normalize_results(list(csv.DictReader(io.StringIO(exported)))), reference.maximums, "CSV export")
    export_path = args.output_dir / f"max_by_state_{args.dataset}.csv"
    export_path.write_text(exported, encoding="utf-8", newline="")
    reference_path = args.output_dir / f"reference_max_by_state_{args.dataset}.csv"
    with reference_path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(RESULT_FIELDS)
        for state, (amount, category) in sorted(reference.maximums.items()):
            writer.writerow((state, category, f"{amount:.2f}"))
    report.update({
        "queries_verified": list(QUERY_FILES),
        "clickhouse_export": export_path.name,
        "independent_reference_export": reference_path.name,
        "clickhouse_version": client.rows("SELECT version() AS version")[0]["version"],
        "status": "passed",
    })


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--dataset", type=dataset_name)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output-dir", type=Path, default=Path("/results"))
    args = parser.parse_args()
    args.dataset = args.dataset or dataset_name(args.csv_path.stem)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "dataset": args.dataset,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "failed",
    }
    started = time.monotonic()
    try:
        verify(args, ClickHouseHTTP(timeout=min(args.timeout, 120)), report)
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(report["error"], file=sys.stderr)
    finally:
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        path = args.output_dir / f"verification_{args.dataset}.json"
        path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
