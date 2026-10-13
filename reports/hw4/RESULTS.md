# HW4 results

Verified on 2026-10-13 at implementation commit `af98500`.

Runtime: dbt-core **1.10.15**, dbt-clickhouse **1.9.8**, ClickHouse **25.3.6.56**, SQLFluff **3.4.2**, pre-commit **4.3.0**, Docker Engine **29.8.1**. Packages: dbt_utils **1.3.0**, dbt_date **0.16.1**, dbt_expectations **0.10.10**.

## Runs and repeated imports

From a fresh clone, `make demo` ran twice on a new project volume. After `make reset`, `make full` ran twice on another new volume using `data/train.csv`. All four runs passed model builds, dbt tests, SQLFluff, Python tests and CSV reconciliation.

| Dataset | Transactions | Distinct transaction IDs | Frauds | Total amount | Fraud amount |
|---|---:|---:|---:|---:|---:|
| Included demo | 24 | 24 | 5 | 9,136.35 | 5,300.65 |
| Labelled train | 786,431 | 786,431 | 4,504 | 55,239,932.49 | 2,386,632.63 |

Audit results were identical after repeat imports, including transaction identities, counts, amounts and all mart groups. Input checksums are recorded in [verification.json](verification.json).

## Aggregate checks

The Python audit recomputes every mart from the original CSV and compares all keys and metrics with ClickHouse. Money and P95 match exactly. Averages and percentages use absolute tolerance `1e-7` or relative tolerance `1e-9`, whichever is greater.

| Mart | Demo groups | Train groups |
|---|---:|---:|
| `mart_daily_state_metrics` | 24 | 21,386 |
| `mart_fraud_by_category` | 11 | 14 |
| `mart_fraud_by_state` | 6 | 51 |
| `mart_customer_risk_profile` | 24 | 965 |
| `mart_hourly_fraud_pattern` | 24 | 168 |
| `mart_merchant_analytics` | 20 | 693 |

All **809 demo** and **186,752 train** metric comparisons passed. Detailed results: [demo audit](audit_demo.json), [train audit](audit_train.json).

## Tests and lint

Each run passed **166 dbt tests**: **158 data tests** and **8 native unit tests**, with zero failures, warnings or skips. Data tests cover nulls, unique and composite keys, accepted values, state relationships and numerical bounds. Six singular tests check amounts, fraud rates, source/staging reconciliation, mart totals, business rules and customer-key collisions.

Native unit tests cover staging and all six marts, including normalization, identity, UTC boundaries, exact P95, decimal sums, distinct counts and risk thresholds. **45 Python tests** and the audit reference checks passed per run.

SQLFluff passed for compiled model and singular-test SQL and for raw unit fixtures. Configuration preserves ClickHouse function casing and exempts one parametric-aggregate expression from unsupported parser rules. All **three pre-commit hooks** passed.

Live ClickHouse checks rejected a missing label and simulated a failure after a scratch-table insert. Both preserved the active source, with no scratch tables remaining. See [import safety results](import_safety.json).

Results: [test log](tests.log), [verification JSON](verification.json).

## Generated documentation

`dbt docs generate` produced `index.html`, `manifest.json` and `catalog.json`, with descriptions for all seven models and their columns. The catalog includes the state seed and raw source. `make docs-serve` returned HTTP 200.

Full-data dbt Docs, filtered to models and sources:

![dbt Docs source, staging and six marts](dbt-docs-dag.jpg)

## Reproduce

From a fresh clone, run `make demo` twice, `make precommit`, then `make reset`. Put the labelled course dataset at `data/train.csv` and run `make full` twice. Compare `results/audit_demo.json` or `results/audit_train.json` across repeats. `make docs-serve` opens the generated documentation at `http://localhost:8080`.
