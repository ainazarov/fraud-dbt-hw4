#!/usr/bin/env python3
"""Exercise incremental maxima and exact replay through the live Kafka pipeline.

Uses an isolated dataset namespace and leaves an audit record in /results.
No merges, services, tables, or existing datasets are stopped or reset.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import sys
import tempfile
import time
import uuid
from decimal import Decimal
from pathlib import Path

from ch_client import ClickHouseHTTP, read_query
from produce import transaction_id


ROOT = Path(__file__).resolve().parents[1]
COUNTS_SQL = """
SELECT 'raw' AS storage, count() AS physical_rows,
       uniqExact(transaction_id) AS distinct_transactions
FROM fraud.transactions WHERE dataset = {dataset:String}
UNION ALL
SELECT 'baseline' AS storage, count() AS physical_rows,
       uniqExact(transaction_id) AS distinct_transactions
FROM fraud.transactions_baseline WHERE dataset = {dataset:String}
"""


def write_batch(path: Path, template: dict[str, str], values: list[tuple[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(template))
        writer.writeheader()
        for amount, category in values:
            writer.writerow(dict(template, us_state="TX", amount=amount, cat_id=category))


def publish(path: Path, dataset: str) -> dict:
    completed = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "produce.py"), str(path), "--dataset", dataset],
        check=False, text=True, capture_output=True,
    )
    if completed.returncode:
        raise RuntimeError(f"Producer failed: {completed.stderr[-4000:]}")
    return json.loads(completed.stdout)


def wait_for_counts(
    client: ClickHouseHTTP, dataset: str, physical: int, distinct: int, timeout: float
) -> list[dict]:
    deadline = time.monotonic() + timeout
    while True:
        counts = client.rows(COUNTS_SQL, dataset=dataset)
        if len(counts) == 2 and all(
            int(row["physical_rows"]) == physical
            and int(row["distinct_transactions"]) == distinct for row in counts
        ):
            return counts
        if any(int(row["physical_rows"]) > physical for row in counts):
            raise AssertionError(f"Unexpected extra delivery in isolated {dataset}: {counts}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Expected physical={physical}, distinct={distinct}; got {counts}")
        time.sleep(0.2)


def assert_winner(
    client: ClickHouseHTTP, dataset: str, category: str, expected_id: str, timeout: float
) -> dict:
    expected = [("TX", category, Decimal("200.00"))]
    # The destination insert and its dependent views may be briefly observable
    # at different stages. Wait for the expected winner, never for part merging.
    deadline = time.monotonic() + timeout
    while True:
        outputs = {}
        for name in ("baseline", "raw", "aggregate"):
            rows = client.rows(read_query(name), dataset=dataset)
            outputs[name] = [
                (row["us_state"], row["cat_id"], Decimal(str(row["max_amount"])))
                for row in rows
            ]
        # Check the third tie-break component, omitted from the submission CSV,
        # in the same eventual-ingestion loop as the public query outputs.
        winner = client.rows(
            "SELECT tupleElement(argMaxMerge(winner), 3) AS transaction_id "
            "FROM fraud.state_max WHERE dataset = {dataset:String} GROUP BY us_state",
            dataset=dataset,
        )
        if (all(value == expected for value in outputs.values())
                and winner == [{"transaction_id": expected_id}]):
            break
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"Expected {expected} and ID {expected_id}; got {outputs}, {winner}"
            )
        time.sleep(0.2)
    return {"us_state": "TX", "cat_id": category, "max_amount": "200.00",
            "transaction_id": expected_id}


def run(output_dir: Path, timeout: float) -> dict:
    dataset = "tie_" + uuid.uuid4().hex[:24]
    client = ClickHouseHTTP(timeout=min(timeout, 30))
    with (ROOT / "data" / "demo.csv").open(encoding="utf-8", newline="") as stream:
        template = next(csv.DictReader(stream))

    phases = []
    with tempfile.TemporaryDirectory(prefix="hw2-stream-test-") as temporary:
        first = Path(temporary) / "first.csv"
        second = Path(temporary) / "second.csv"
        # First, amount must outrank category: 'a' wins despite 'z' sorting later.
        write_batch(first, template, [("100.00", "z"), ("200.00", "a")])
        # Later equal-amount messages must update the winner to 'z'. Two z rows
        # also force the stable transaction-ID tie-breaker across messages.
        write_batch(second, template, [("200.00", "z"), ("200.00", "z")])

        first_hash = hashlib.sha256(first.read_bytes()).hexdigest()
        second_hash = hashlib.sha256(second.read_bytes()).hexdigest()
        first_id = transaction_id(dataset, first_hash, 2)
        second_id = max(transaction_id(dataset, second_hash, n) for n in (1, 2))

        manifest = publish(first, dataset)
        counts = wait_for_counts(client, dataset, 2, 2, timeout)
        winner = assert_winner(client, dataset, "a", first_id, timeout)
        phases.append({"phase": "first_batch", "manifest": manifest,
                       "counts": counts, "winner": winner})

        manifest = publish(second, dataset)
        counts = wait_for_counts(client, dataset, 4, 4, timeout)
        winner = assert_winner(client, dataset, "z", second_id, timeout)
        phases.append({"phase": "later_tied_batch", "manifest": manifest,
                       "counts": counts, "winner": winner})

        # Producer idempotence is scoped to its session. An explicit new import
        # adds physical rows, but max/argMax must remain invariant to replay.
        manifest = publish(second, dataset)
        counts = wait_for_counts(client, dataset, 6, 4, timeout)
        winner = assert_winner(client, dataset, "z", second_id, timeout)
        phases.append({"phase": "explicit_replay", "manifest": manifest,
                       "counts": counts, "winner": winner})

    result = {"passed": True, "dataset": dataset, "phases": phases,
              "checks": ["amount_precedes_category", "later_tie_updates_winner",
                         "transaction_id_breaks_full_tie", "all_three_queries_agree",
                         "explicit_replay_preserves_max", "replay_is_not_exactly_once"],
              "merge_control": "No OPTIMIZE or merge waiting; queried as soon as ingestion completed."}
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / f"stream_regression_{dataset}.json"
    destination.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    result["report"] = str(destination)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("/results"))
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    try:
        result = run(args.output_dir, args.timeout)
    except Exception as exc:
        print(f"Stream regression failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
