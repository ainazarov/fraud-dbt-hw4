#!/usr/bin/env python3
"""Validate a labelled CSV, then atomically replace the active raw snapshot."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from scripts.ch_client import ClickHouseHTTP
from scripts.produce import PreparedCsv, ValidationError, preflight_csv, utc_now, write_manifest


TABLE = "transactions_db.transactions"
COLUMNS = """
    transaction_id String,
    dataset LowCardinality(String),
    row_number UInt64,
    transaction_time DateTime('UTC'),
    merch String,
    cat_id LowCardinality(String),
    amount Decimal(12, 2),
    name_1 String,
    name_2 String,
    gender LowCardinality(String),
    street String,
    one_city String,
    us_state LowCardinality(String),
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target UInt8
"""


@contextmanager
def import_lock(path: Path, timeout: float = 600) -> Iterator[None]:
    """Serialize loaders sharing the project's results volume, including crashes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        if os.name == "nt":
            import msvcrt

            if stream.tell() == 0:
                stream.write(b"\0")
                stream.flush()

            def acquire() -> None:
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)

            def release() -> None:
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            def acquire() -> None:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)

            def release() -> None:
                fcntl.flock(stream, fcntl.LOCK_UN)

        deadline = time.monotonic() + timeout
        while True:
            try:
                acquire()
                break
            except (BlockingIOError, PermissionError):
                if time.monotonic() >= deadline:
                    raise TimeoutError("Another CSV import holds the project import lock") from None
                time.sleep(0.1)
        try:
            yield
        finally:
            release()


def labelled_totals(prepared: PreparedCsv) -> dict[str, Any]:
    amount = Decimal(0)
    fraud = 0
    for row_number, (_, value) in enumerate(prepared.messages(), start=1):
        row = json.loads(value)
        if row["target"] not in (0, 1):
            raise ValidationError(f"row {row_number}, target: a 0/1 label is required for HW4")
        amount += Decimal(row["amount"])
        fraud += row["target"]
    return {"rows": prepared.rows, "amount": format(amount, ".2f"), "fraud": fraud}


def create_table(client: ClickHouseHTTP, table: str) -> None:
    client.query(
        f"CREATE TABLE IF NOT EXISTS {table} ({COLUMNS}) "
        "ENGINE = MergeTree ORDER BY (transaction_time, transaction_id)",
        output_format=None,
    )


def publish_snapshot(
    client: ClickHouseHTTP, prepared: PreparedCsv, expected: dict[str, Any],
    *, batch_rows: int = 5000,
) -> dict[str, Any]:
    if batch_rows < 1:
        raise ValueError("batch_rows must be positive")
    staging = f"transactions_db._load_{uuid.uuid4().hex}"
    client.query("CREATE DATABASE IF NOT EXISTS transactions_db ENGINE = Atomic", output_format=None)
    databases = client.rows("SELECT engine FROM system.databases WHERE name = 'transactions_db'")
    if not databases or databases[0]["engine"] != "Atomic":
        raise RuntimeError("transactions_db must use the Atomic database engine")
    create_table(client, TABLE)
    cleanup_warning = None
    try:
        create_table(client, staging)
        batch: list[str] = []
        for _, line in prepared.messages():
            batch.append(line.decode("utf-8"))
            if len(batch) == batch_rows:
                client.query(
                    f"INSERT INTO {staging} FORMAT JSONEachRow\n" + "\n".join(batch),
                    output_format=None,
                )
                batch.clear()
        if batch:
            client.query(
                f"INSERT INTO {staging} FORMAT JSONEachRow\n" + "\n".join(batch),
                output_format=None,
            )
        actual = client.rows(
            "SELECT count() AS rows, uniqExact(transaction_id) AS unique_ids, "
            f"toString(sum(amount)) AS amount, sum(target) AS fraud FROM {staging}"
        )[0]
        if (int(actual["rows"]) != expected["rows"]
                or int(actual["unique_ids"]) != expected["rows"]
                or Decimal(actual["amount"]) != Decimal(expected["amount"])
                or int(actual["fraud"]) != expected["fraud"]):
            raise RuntimeError("Scratch-table totals differ from the validated CSV")
        client.query(f"EXCHANGE TABLES {TABLE} AND {staging}", output_format=None)
    finally:
        try:
            client.query(f"DROP TABLE IF EXISTS {staging} SYNC", output_format=None)
        except Exception as exc:
            cleanup_warning = f"Scratch cleanup failed for {staging}: {type(exc).__name__}"
            print(cleanup_warning, file=sys.stderr)
    return {**expected, "unique_ids": expected["rows"], "cleanup_warning": cleanup_warning}


def run_import(
    csv_path: Path, *, dataset: str | None = None,
    manifest_path: Path | None = None, lock_path: Path = Path("/results/.load.lock"),
    client: ClickHouseHTTP | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    started_utc = utc_now()
    with import_lock(lock_path):
        with preflight_csv(csv_path, dataset=dataset) as prepared:
            expected = labelled_totals(prepared)
            loaded = publish_snapshot(client or ClickHouseHTTP(), prepared, expected)
            manifest = {
                "dataset": prepared.dataset,
                "input_sha256": prepared.input_sha256,
                "input_bytes": prepared.input_bytes,
                "source_table": TABLE,
                "mode": "atomic_snapshot_replace",
                **loaded,
                "started_utc": started_utc,
                "finished_utc": utc_now(),
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
        if manifest_path is not None:
            write_manifest(manifest_path, manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--dataset")
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    try:
        receipt = run_import(args.csv_path, dataset=args.dataset, manifest_path=args.manifest)
    except KeyboardInterrupt:
        print("error: import interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(receipt, ensure_ascii=False, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
