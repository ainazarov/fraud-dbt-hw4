"""Compare baseline, sorted raw storage and aggregate-state queries fairly.

This is a warm-cache, single-node microbenchmark. It does not drop global caches.
Server measurements come from QueryFinish records for unique query IDs.
"""

from __future__ import annotations

import argparse
import csv
import json
import platform
import random
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from ch_client import ClickHouseHTTP, QUERY_FILES, read_query
from verify import assert_maximums, dataset_name, normalize_results


SETTINGS = {
    "max_threads": 2,
    "use_query_cache": 0,
    "log_queries": 1,
    "log_queries_probability": 1,
    "log_queries_min_query_duration_ms": 0,
}
TABLES = ("transactions_baseline", "transactions", "state_max")
SHUFFLE_SEED = 20261005


def measured_query(client: ClickHouseHTTP, name: str, dataset: str, query_id: str) -> tuple[float, dict[str, tuple[Decimal, str]]]:
    started = time.perf_counter_ns()
    payload = client.query(read_query(name), dataset=dataset, settings=SETTINGS, query_id=query_id)
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    rows = [json.loads(line, parse_float=Decimal) for line in payload.splitlines() if line.strip()]
    return elapsed_ms, normalize_results(rows)


def collect_query_log(client: ClickHouseHTTP, ids: list[str]) -> dict[str, dict[str, Any]]:
    # These IDs are generated internally from a UUID, fixed query names and integers.
    id_sql = ",".join(f"'{query_id}'" for query_id in ids)
    deadline = time.monotonic() + 20
    while True:
        client.query("SYSTEM FLUSH LOGS", output_format=None)
        rows = client.rows(
            "SELECT query_id, query_duration_ms AS duration_ms, read_rows, read_bytes, "
            "memory_usage, result_rows, result_bytes FROM system.query_log "
            f"WHERE type = 'QueryFinish' AND is_initial_query = 1 AND query_id IN ({id_sql})"
        )
        logs = {str(row["query_id"]): row for row in rows}
        if len(rows) != len(logs):
            raise AssertionError("Multiple QueryFinish rows for an allegedly unique query ID")
        if set(logs) == set(ids):
            return logs
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Missing QueryFinish records for: {sorted(set(ids) - set(logs))}")
        time.sleep(1)


def collect_storage(client: ClickHouseHTTP, dataset: str) -> dict[str, Any]:
    rows = client.rows(
        "SELECT table, sum(rows) AS physical_rows, sum(data_compressed_bytes) AS data_compressed_bytes, "
        "sum(data_uncompressed_bytes) AS data_uncompressed_bytes, sum(bytes_on_disk) AS bytes_on_disk, "
        "count() AS active_parts FROM system.parts WHERE active AND database = 'fraud' "
        "AND table IN ('transactions_baseline', 'transactions', 'state_max') GROUP BY table ORDER BY table"
    )
    tables = {
        str(row["table"]): {key: int(value) for key, value in row.items() if key != "table"}
        for row in rows
    }
    if set(tables) != set(TABLES):
        raise AssertionError(f"Expected nonempty active parts for {TABLES}, found {sorted(tables)}")
    for table in TABLES:
        count = client.rows(
            f"SELECT count() AS rows FROM fraud.{table} WHERE dataset = {{dataset:String}}", dataset=dataset
        )[0]["rows"]
        tables[table]["rows_for_selected_dataset"] = int(count)
    return {
        "scope": "active parts for whole tables, including all loaded datasets; physical aggregate rows may merge",
        "tables": tables,
    }


def median_ratio(numerator: float, denominator: float) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


def benchmark(args: argparse.Namespace) -> dict[str, Any]:
    client = ClickHouseHTTP(timeout=600)
    run_id = uuid.uuid4().hex
    report: dict[str, Any] = {
        "dataset": args.dataset,
        "run_id": run_id,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "repetitions_per_query": args.repetitions,
        "warmup_runs_per_query": 1,
        "random_order_seed": SHUFFLE_SEED,
        "query_settings": SETTINGS,
        "method": "warm-cache single-node; sequential queries in a deterministically shuffled order each round",
        "global_caches_dropped": False,
        "compacted_before_run": args.compact,
        "compaction_scope": [f"fraud.{table}" for table in TABLES] if args.compact else [],
        "client_python": platform.python_version(),
        "client_platform": platform.platform(),
        "caveats": [
            "Duration depends on hardware, other workloads, merges and the loaded datasets.",
            "Server query_duration_ms has millisecond resolution; very small timings may be zero.",
            "Client elapsed_ms includes HTTP transport; it is reported separately from server duration_ms.",
            "The aggregate table accelerates this specific query and adds storage and ingestion work.",
        ],
    }
    report["server"] = client.rows("SELECT version() AS version, hostName() AS hostname, timezone() AS timezone")[0]
    report["server_resource_metrics"] = client.rows(
        "SELECT metric, value FROM system.asynchronous_metrics "
        "WHERE metric IN ('CGroupMaxCPU', 'CGroupMemoryTotal', 'MemoryTotal', 'NumberOfPhysicalCores', "
        "'NumberOfCPUCores', 'OSMemoryTotal', 'OSMemoryAvailable') ORDER BY metric"
    )
    if args.compact:
        print("Compacting only fraud.transactions_baseline, fraud.transactions and fraud.state_max", flush=True)
        for table in TABLES:
            client.query(f"OPTIMIZE TABLE fraud.{table} FINAL", output_format=None, settings={"max_threads": 2})
    report["storage_before"] = collect_storage(client, args.dataset)
    for name in QUERY_FILES:
        explain = client.query(
            "EXPLAIN indexes = 1\n" + read_query(name), dataset=args.dataset,
            settings={"max_threads": 2, "use_query_cache": 0}, output_format="TabSeparatedRaw",
        )
        (args.output_dir / f"explain_{name}_{args.dataset}.txt").write_text(explain, encoding="utf-8")
    expected: dict[str, tuple[Decimal, str]] | None = None
    for name in QUERY_FILES:
        _, result = measured_query(client, name, args.dataset, f"hw2-{run_id}-warmup-{name}")
        if not result:
            raise AssertionError(f"{name} returned no states for dataset {args.dataset!r}")
        if expected is None:
            expected = result
        assert_maximums(result, expected, f"warmup {name}")
    assert expected is not None
    report["state_count"] = len(expected)
    samples: list[dict[str, Any]] = []
    rng = random.Random(SHUFFLE_SEED)
    for repetition in range(1, args.repetitions + 1):
        order = list(QUERY_FILES)
        rng.shuffle(order)
        for position, name in enumerate(order, start=1):
            query_id = f"hw2-{run_id}-{name}-{repetition}"
            elapsed, result = measured_query(client, name, args.dataset, query_id)
            assert_maximums(result, expected, f"measured {name} repetition {repetition}")
            samples.append({
                "query": name, "repetition": repetition, "order_within_round": position,
                "query_id": query_id, "elapsed_ms": round(elapsed, 6),
            })
        print(f"Completed benchmark round {repetition}/{args.repetitions}", flush=True)
    logs = collect_query_log(client, [sample["query_id"] for sample in samples])
    for sample in samples:
        sample.update({key: int(value) for key, value in logs[sample["query_id"]].items() if key != "query_id"})
    sample_file = args.output_dir / f"benchmark_samples_{args.dataset}.csv"
    with sample_file.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(samples[0]))
        writer.writeheader()
        writer.writerows(samples)
    report["samples_file"] = sample_file.name
    report["summary"] = {}
    metrics = ("duration_ms", "elapsed_ms", "read_rows", "read_bytes", "memory_usage")
    for name in QUERY_FILES:
        group = [sample for sample in samples if sample["query"] == name]
        report["summary"][name] = {
            "samples": len(group),
            **{
                metric: {
                    "median": statistics.median(sample[metric] for sample in group),
                    "min": min(sample[metric] for sample in group),
                    "max": max(sample[metric] for sample in group),
                }
                for metric in metrics
            },
        }
    report["improvement_vs_baseline"] = {}
    baseline = report["summary"]["baseline"]
    for name in ("raw", "aggregate"):
        optimized = report["summary"][name]
        report["improvement_vs_baseline"][name] = {
            f"{metric}_ratio_baseline_over_optimized": median_ratio(baseline[metric]["median"], optimized[metric]["median"])
            for metric in ("duration_ms", "elapsed_ms", "read_rows", "read_bytes")
        }
    report["storage_after"] = collect_storage(client, args.dataset)
    report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    report["status"] = "passed"
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=dataset_name, default="train")
    parser.add_argument("--repetitions", type=int, default=9)
    parser.add_argument("--output-dir", type=Path, default=Path("/results"))
    parser.add_argument("--compact", action="store_true", help="OPTIMIZE FINAL only this project's three data tables before measurement")
    args = parser.parse_args()
    if args.repetitions < 3:
        parser.error("--repetitions must be at least 3")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    try:
        report = benchmark(args)
    except Exception as exc:
        report = {
            "status": "failed", "dataset": args.dataset,
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "error": f"{type(exc).__name__}: {exc}",
        }
        print(report["error"], file=sys.stderr)
    path = args.output_dir / f"benchmark_{args.dataset}.json"
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
