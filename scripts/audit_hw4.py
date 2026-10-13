"""Compare every dbt mart group with an independent labelled CSV calculation."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from ch_client import ClickHouseHTTP


CENT = Decimal("0.01")
MODEL_KEYS = {
    "mart_daily_state_metrics": ("transaction_date", "state"),
    "mart_fraud_by_category": ("category",),
    "mart_fraud_by_state": ("state",),
    "mart_customer_risk_profile": ("customer_id",),
    "mart_hourly_fraud_pattern": ("weekday_iso", "transaction_hour"),
    "mart_merchant_analytics": ("merchant",),
}
INTEGER_FIELDS = {
    "transaction_count", "fraud_count", "customer_count", "merchant_count",
    "weekday_iso", "transaction_hour", "is_suspicious", "distinct_ids",
    "distinct_row_numbers", "first_row", "last_row", "invalid_labels",
    "invalid_ids", "wrong_dataset",
}
MONEY_FIELDS = {"total_amount", "fraud_amount", "p95_amount"}
APPROXIMATE_FIELDS = {"avg_amount", "fraud_rate", "large_transaction_share"}


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cleaned(value: str) -> str:
    return value.strip().lower()


def customer_identity(row: dict[str, str]) -> str:
    values = [cleaned(row[name]) for name in ("name_1", "name_2", "street")]
    values.append(row["post_code"].strip())
    encoded = "|".join(f"{len(value)}:{value}" for value in values)
    return hashlib.md5(encoded.encode("utf-8"), usedforsecurity=False).hexdigest()


@dataclass
class Aggregate:
    count: int = 0
    frauds: int = 0
    cents: int = 0
    fraud_cents: int = 0
    large_count: int = 0
    amounts: list[int] = field(default_factory=list)
    customers: set[str] = field(default_factory=set)
    merchants: set[str] = field(default_factory=set)
    first: str | None = None
    last: str | None = None

    def add(self, cents: int, fraud: int, customer: str, merchant: str,
            timestamp: str, *, amounts: bool = False, customers: bool = False,
            merchants: bool = False, times: bool = False) -> None:
        self.count += 1
        self.frauds += fraud
        self.cents += cents
        self.fraud_cents += cents * fraud
        self.large_count += cents >= 50000
        if amounts:
            self.amounts.append(cents)
        if customers:
            self.customers.add(customer)
        if merchants:
            self.merchants.add(merchant)
        if times:
            self.first = timestamp if self.first is None else min(self.first, timestamp)
            self.last = timestamp if self.last is None else max(self.last, timestamp)

    def metrics(self, model: str) -> dict[str, Any]:
        rate = Decimal(100) * self.frauds / self.count
        metrics: dict[str, Any] = {
            "transaction_count": self.count,
            "fraud_count": self.frauds,
            "total_amount": Decimal(self.cents) * CENT,
            "fraud_amount": Decimal(self.fraud_cents) * CENT,
            "fraud_rate": rate,
        }
        if model not in {"mart_fraud_by_category", "mart_fraud_by_state"}:
            metrics["avg_amount"] = Decimal(self.cents) * CENT / self.count
        if model == "mart_daily_state_metrics":
            ordered = sorted(self.amounts)
            metrics["p95_amount"] = Decimal(ordered[min(int(0.95 * self.count), self.count - 1)]) * CENT
            metrics["large_transaction_share"] = Decimal(100) * self.large_count / self.count
        elif model == "mart_fraud_by_state":
            metrics.update(customer_count=len(self.customers), merchant_count=len(self.merchants))
        elif model == "mart_customer_risk_profile":
            metrics.update(first_transaction_time=self.first, last_transaction_time=self.last,
                           risk_level="HIGH" if rate >= 10 else "MEDIUM" if rate >= 1 else "LOW")
        elif model == "mart_merchant_analytics":
            metrics.update(customer_count=len(self.customers),
                           is_suspicious=int(self.frauds >= 2 and rate >= 5))
        return metrics


@dataclass
class Reference:
    sha256: str
    dataset: str
    totals: Aggregate
    groups: dict[str, dict[tuple[Any, ...], Aggregate]]


def read_reference(path: Path, dataset: str) -> Reference:
    initial_hash = file_hash(path)
    totals = Aggregate()
    groups: dict[str, dict[tuple[Any, ...], Aggregate]] = {name: {} for name in MODEL_KEYS}
    required = {"transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
                "street", "post_code", "us_state", "target"}
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source, strict=True)
        header = reader.fieldnames or []
        if not required.issubset(header) or len(header) != len(set(header)):
            raise ValueError("CSV needs unique columns including all labels and identity fields")
        for number, row in enumerate(reader, start=1):
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Malformed CSV at row {number}")
            try:
                amount = Decimal(row["amount"].strip())
                if not amount.is_finite() or amount < 0 or amount != amount.quantize(CENT):
                    raise ValueError("amount")
                label = row["target"].strip()
                if label not in {"0", "1"}:
                    raise ValueError("label")
                instant = datetime.fromisoformat(row["transaction_time"].strip().replace("Z", "+00:00"))
                instant = instant.replace(tzinfo=timezone.utc) if instant.tzinfo is None else instant
                instant = instant.astimezone(timezone.utc)
                if instant.microsecond:
                    raise ValueError("subsecond timestamp")
            except (ValueError, ArithmeticError) as exc:
                raise ValueError(f"Invalid labelled transaction at CSV row {number}") from exc
            cents, fraud = int(amount * 100), int(label)
            customer = customer_identity(row)
            merchant = cleaned(row["merch"]) or "unknown"
            category = cleaned(row["cat_id"]) or "unknown"
            state = row["us_state"].strip().upper()
            timestamp = instant.strftime("%Y-%m-%d %H:%M:%S")
            keys = {
                "mart_daily_state_metrics": (instant.date().isoformat(), state),
                "mart_fraud_by_category": (category,),
                "mart_fraud_by_state": (state,),
                "mart_customer_risk_profile": (customer,),
                "mart_hourly_fraud_pattern": (instant.isoweekday(), instant.hour),
                "mart_merchant_analytics": (merchant,),
            }
            totals.add(cents, fraud, customer, merchant, timestamp)
            for model, key in keys.items():
                aggregate = groups[model].setdefault(key, Aggregate())
                aggregate.add(cents, fraud, customer, merchant, timestamp,
                              amounts=model == "mart_daily_state_metrics",
                              customers=model in {"mart_fraud_by_state", "mart_merchant_analytics"},
                              merchants=model == "mart_fraud_by_state",
                              times=model == "mart_customer_risk_profile")
    if not totals.count:
        raise ValueError("CSV contains no labelled transactions")
    if file_hash(path) != initial_hash:
        raise ValueError("CSV changed while the independent reference was calculated")
    return Reference(initial_hash, dataset, totals, groups)


def equivalent(field_name: str, actual: Any, expected: Any) -> bool:
    if actual is None:
        return expected is None
    try:
        if field_name in INTEGER_FIELDS or field_name in MONEY_FIELDS:
            return Decimal(str(actual)) == Decimal(str(expected))
        if field_name in APPROXIMATE_FIELDS:
            value, reference = Decimal(str(actual)), Decimal(str(expected))
            tolerance = max(Decimal("0.0000001"), abs(reference) * Decimal("0.000000001"))
            return value.is_finite() and abs(value - reference) <= tolerance
        return str(actual) == str(expected)
    except (ValueError, ArithmeticError):
        return False


def audit_relation(client: ClickHouseHTTP, relation: str, label: str,
                   reference: Reference) -> dict[str, Any]:
    actual = client.rows(f"""
        SELECT count() AS transaction_count, uniqExact(transaction_id) AS distinct_ids,
            uniqExact(row_number) AS distinct_row_numbers,
            min(row_number) AS first_row, max(row_number) AS last_row,
            sum(amount) AS total_amount, countIf({label} = 1) AS fraud_count,
            sumIf(amount, {label} = 1) AS fraud_amount,
            countIf(isNull({label}) OR {label} NOT IN (0, 1)) AS invalid_labels,
            countIf(dataset != {{expected_dataset:String}}) AS wrong_dataset,
            countIf(transaction_id != lower(hex(SHA256(concat(dataset, char(0),
                {{source_hash:String}}, char(0), toString(row_number)))))) AS invalid_ids,
            toString(sumWithOverflow(cityHash64(transaction_id))) AS id_checksum
        FROM {relation}
    """, parameters={"expected_dataset": reference.dataset, "source_hash": reference.sha256})[0]
    count = reference.totals.count
    expected = {
        "transaction_count": count, "distinct_ids": count, "distinct_row_numbers": count,
        "first_row": 1, "last_row": count, "total_amount": Decimal(reference.totals.cents) * CENT,
        "fraud_count": reference.totals.frauds,
        "fraud_amount": Decimal(reference.totals.fraud_cents) * CENT,
        "invalid_labels": 0, "wrong_dataset": 0, "invalid_ids": 0,
    }
    mismatches = [name for name, value in expected.items() if not equivalent(name, actual.get(name), value)]
    return {"passed": not mismatches, "mismatched_fields": mismatches,
            "observed": actual, "expected": expected}


def audit_model(client: ClickHouseHTTP, schema: str, model: str,
                groups: dict[tuple[Any, ...], Aggregate]) -> dict[str, Any]:
    rows = client.rows(f"SELECT * FROM {schema}.{model}")
    keys = MODEL_KEYS[model]
    observed: dict[tuple[Any, ...], dict[str, Any]] = {}
    duplicate_keys = 0
    for row in rows:
        key = tuple(int(row[name]) if name in INTEGER_FIELDS else str(row[name]) for name in keys)
        duplicate_keys += key in observed
        observed[key] = row
    expected_keys, observed_keys = set(groups), set(observed)
    mismatches: dict[str, int] = {}
    compared = 0
    for key in expected_keys & observed_keys:
        expected = groups[key].metrics(model)
        actual = observed[key]
        unexpected_columns = set(actual) - set(keys) - set(expected)
        for name in unexpected_columns:
            mismatches[f"unverified_column:{name}"] = mismatches.get(f"unverified_column:{name}", 0) + 1
        for name, value in expected.items():
            compared += 1
            if not equivalent(name, actual.get(name), value):
                mismatches[name] = mismatches.get(name, 0) + 1
    missing, extra = len(expected_keys - observed_keys), len(observed_keys - expected_keys)
    return {"passed": not (duplicate_keys or missing or extra or mismatches),
            "expected_groups": len(groups), "observed_groups": len(rows),
            "missing_groups": missing, "extra_groups": extra, "duplicate_keys": duplicate_keys,
            "metrics_compared": compared, "mismatched_metric_counts": mismatches}


def self_test() -> None:
    daily = Aggregate()
    for number in range(1, 21):
        daily.add(number * 100, int(number == 20), "a", "shop", "2020-01-01 00:00:00", amounts=True)
    expected = daily.metrics("mart_daily_state_metrics")
    assert expected["p95_amount"] == Decimal("20.00")
    assert expected["total_amount"] == Decimal("210.00")
    assert expected["avg_amount"] == Decimal("10.50")
    assert expected["fraud_rate"] == 5
    for count, frauds, level in ((100, 0, "LOW"), (100, 1, "MEDIUM"), (100, 10, "HIGH")):
        assert Aggregate(count=count, frauds=frauds).metrics("mart_customer_risk_profile")["risk_level"] == level
    assert Aggregate(count=40, frauds=2).metrics("mart_merchant_analytics")["is_suspicious"] == 1
    assert Aggregate(count=41, frauds=2).metrics("mart_merchant_analytics")["is_suspicious"] == 0
    assert Aggregate(count=10, frauds=1).metrics("mart_merchant_analytics")["is_suspicious"] == 0
    assert equivalent("total_amount", "12.34", Decimal("12.34"))
    assert not equivalent("total_amount", "12.35", Decimal("12.34"))
    assert not equivalent("fraud_rate", "NaN", Decimal("1"))
    assert customer_identity({"name_1": " Ann ", "name_2": "LEE", "street": "1 Main", "post_code": "12345"}) == hashlib.md5(b"3:ann|3:lee|6:1 main|5:12345", usedforsecurity=False).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", nargs="?", type=Path)
    parser.add_argument("--dataset")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--schema", default=os.environ.get("DBT_SCHEMA", "fraud_analytics"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        print("Independent audit reference self-tests passed")
        return 0
    if args.csv_path is None:
        parser.error("csv_path is required unless --self-test is used")
    dataset = args.dataset or args.csv_path.stem
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", dataset):
        parser.error("dataset must match [a-z][a-z0-9_]{0,31}")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.schema):
        parser.error("schema must be a simple SQL identifier")
    report: dict[str, Any] = {"passed": False, "dataset": dataset}
    try:
        reference = read_reference(args.csv_path, dataset)
        client = ClickHouseHTTP(timeout=300)
        report.update(input_sha256=reference.sha256, rows=reference.totals.count,
                      fraud_count=reference.totals.frauds,
                      total_amount=Decimal(reference.totals.cents) * CENT,
                      fraud_amount=Decimal(reference.totals.fraud_cents) * CENT,
                      approximate_absolute_tolerance="1e-7", approximate_relative_tolerance="1e-9")
        report["raw"] = audit_relation(client, "transactions_db.transactions", "target", reference)
        report["staging"] = audit_relation(client, f"{args.schema}.stg_transactions", "is_fraud", reference)
        report["marts"] = {model: audit_model(client, args.schema, model, groups)
                           for model, groups in reference.groups.items()}
        report["passed"] = report["raw"]["passed"] and report["staging"]["passed"] and all(
            result["passed"] for result in report["marts"].values())
        if file_hash(args.csv_path) != reference.sha256:
            raise ValueError("CSV changed during the audit")
    except Exception as exc:
        report.update(passed=False, audit_error=type(exc).__name__)
        print(f"Audit failed: {exc}", file=sys.stderr)
    encoded = json.dumps(report, ensure_ascii=False, indent=2, default=str, allow_nan=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
