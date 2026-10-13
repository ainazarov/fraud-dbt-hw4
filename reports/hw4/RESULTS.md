# HW4 verification

Verified on 2026-10-13 from a fresh Git bundle clone of implementation commit `af9850089122b2b2ffc4cda228bd596e3d1c7d05`. The unchanged HW2 baseline is `158e00e3aba27cb76180379971a845f24d7bbf1f`.

Runtime: dbt-core **1.10.15**, dbt-clickhouse **1.9.8**, ClickHouse **25.3.6.56**, SQLFluff **3.4.2**, pre-commit **4.3.0**, Docker Engine **29.8.1**. Packages: dbt_utils **1.3.0**, dbt_date **0.16.1**, dbt_expectations **0.10.10**.

## Clean runs and repeated imports

The verification host had no running containers when inspected. A stop-all command was also executed before verification. Existing project files and all 46 pre-existing Docker volumes were preserved. Only this project's containers, network and `fraud-dbt-hw4_clickhouse_data` volume were created and reset.

The committed code was cloned into a new directory. `make demo` ran twice on a new project volume. That volume was removed with `make reset`, then `make full` ran twice on another new project volume. Each complete run passed model builds, dbt tests, SQLFluff, Python tests and independent CSV reconciliation. The full input was copied into the clone's own `data/train.csv` before execution.

| Dataset | Transactions | Distinct transaction IDs | Frauds | Total amount | Fraud amount |
|---|---:|---:|---:|---:|---:|
| Included demo | 24 | 24 | 5 | 9,136.35 | 5,300.65 |
| Labelled train | 786,431 | 786,431 | 4,504 | 55,239,932.49 | 2,386,632.63 |

The complete audit JSON was identical before and after each repeat import. Counts, amounts, fraud counts, transaction identities and every mart group remained unchanged. Train input SHA256: `2fd291f0d66cbb1f3c3e442396f3725d4cbde650d5dd0c16322c5230d3776b99`.

## Marts and independent results

The audit reads the original CSV and computes grouping, exact monetary totals, distinct counts, percentile, risk classifications and suspicious flags in Python. It compares complete key sets and all metrics in ClickHouse. Money and P95 must match exactly; averages and percentages use absolute tolerance `1e-7` or relative tolerance `1e-9`, whichever is greater.

| Mart | Demo groups | Train groups |
|---|---:|---:|
| `mart_daily_state_metrics` | 24 | 21,386 |
| `mart_fraud_by_category` | 11 | 14 |
| `mart_fraud_by_state` | 6 | 51 |
| `mart_customer_risk_profile` | 24 | 965 |
| `mart_hourly_fraud_pattern` | 24 | 168 |
| `mart_merchant_analytics` | 20 | 693 |

All **809 demo** and **186,752 train** metric comparisons passed. Detailed results: [demo audit](audit_demo.json), [train audit](audit_train.json).

## Tests, lint and import failures

Every complete run passed **166 dbt tests**: **158 data tests** and **8 native unit tests**, with zero failures, warnings or skips. Data tests include nulls, uniqueness, accepted values, state relationships, composite-key uniqueness, dbt_expectations ranges and six singular SQL tests for negative amounts, rate bounds, source/staging reconciliation, mart conservation, business rules and customer-key collisions.

The native unit tests cover staging normalization, amount thresholds, customer identity, UTC boundaries, exact P95, decimal category sums, state distinct counts, customer risk thresholds, weekday/hour grain and merchant suspicion thresholds. All six marts are exercised. **45 Python tests** passed per run, including importer failure, repeat-import and locking cases. The independent audit's reference self-tests also passed.

SQLFluff passed for compiled model/singular-test SQL and separately for raw SQL unit fixtures. ClickHouse's case-sensitive function names are exempt from function-casing enforcement. A single annotated parametric-aggregate expression uses a narrow parser-rule exception; its P95 behavior is checked by native unit tests and the CSV audit. All **three pre-commit hooks** passed.

A live ClickHouse failure check rejected a missing label in the last CSV row. A second check inserted a real scratch-table batch, then injected a client error before the next insert. Both checks preserved the active source fingerprint, and no scratch tables remained. See [import safety evidence](import_safety.json).

The concise [test log](tests.log) and [machine-readable verification](verification.json) retain the successful outcomes.

## Generated documentation

`dbt docs generate` produced `index.html`, `manifest.json` and `catalog.json`. All seven models and their columns have descriptions. The catalog contains eight dbt relations (seven models and the state seed) and one raw source. Artifact hashes are in the verification JSON. `make docs-serve` ran `dbt docs serve` and returned HTTP 200.

The screenshot below is from the generated full-data dbt Docs. The resource filter selects models and sources so the raw → staging → six-mart DAG is readable.

![dbt Docs source, staging and six marts](dbt-docs-dag.jpg)

## Reproduce

From a fresh clone, run `make demo` twice, `make precommit`, then `make reset`. Put the labelled course dataset at `data/train.csv` and run `make full` twice. Compare `results/audit_demo.json` or `results/audit_train.json` across repeats. `make docs-serve` opens the generated documentation at `http://localhost:8080`.
