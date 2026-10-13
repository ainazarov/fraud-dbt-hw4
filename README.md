# Fraud analytics with dbt

HW4 extends the HW2 Git baseline with a dbt pipeline:

```text
labelled CSV → transactions_db.transactions → stg_transactions → six marts
```

Docker Compose starts the project's ClickHouse server, network and named volume. A separate tools container runs the loader, dbt, SQLFluff and tests. HW2's CSV validation and HTTP client are reused. The HW4 loader inserts directly into ClickHouse; Kafka is not needed for this batch pipeline.

## Run

Requirements: Docker Engine, Docker Compose v2, GNU Make and Bash (on Windows, use WSL). Run commands from the repository root.

```bash
make demo
```

This builds the tools image, starts ClickHouse, loads the included synthetic `data/demo.csv`, installs dbt packages, loads the state seed, builds staging and all six marts, runs data and unit tests, generates documentation, lints SQL and checks aggregates against the CSV.

For the full labelled dataset, download `train.csv` from the [course Kaggle dataset](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394), sign in and accept the competition terms if requested. Place the file at `data/train.csv`, then run:

```bash
make full
```

Use the course CSV with the `target` column. The expected header is shown in `data/demo.csv`. The unlabelled `test.csv` is unsuitable for fraud-rate calculations and is rejected. Large input files are excluded from Git.

Each load validates the complete file, inserts into a scratch table and atomically exchanges it with the active source. A successful load replaces the active dataset: demo and train are evaluated separately. Repeating an import does not append transactions. Invalid or incomplete loads leave the active source intact. Transaction identities retain HW2's SHA256 of dataset, input-file SHA256 and row number. Run one pipeline at a time; the loader serializes imports and Make executes its stages sequentially.

```bash
make docs-serve  # open http://localhost:8080
make down       # stop this project; keep its data
make reset      # remove this project's containers and volume
```

The default Compose project name is `fraud-dbt-hw4`. Set `COMPOSE_PROJECT_NAME` to an unused name to start another isolated copy. ClickHouse is reachable within the Compose network. Only the documentation server needs a host port.

## Models and definitions

| Model | Grain | Metrics |
|---|---|---|
| `stg_transactions` | Transaction | Normalized strings, typed amounts and labels, UTC date/hour/weekday, customer surrogate, amount bucket |
| `mart_daily_state_metrics` | Date, state | Counts, total/fraud amount, fraud rate, average amount, exact P95, large-transaction share |
| `mart_fraud_by_category` | Category | Counts, total/fraud amount, fraud rate |
| `mart_fraud_by_state` | State | Counts, total/fraud amount, fraud rate, unique customers and merchants |
| `mart_customer_risk_profile` | Customer surrogate | Counts, amounts, fraud rate, average amount, first/last transaction, risk level |
| `mart_hourly_fraud_pattern` | ISO weekday, UTC hour | Counts, amounts, fraud rate, average amount |
| `mart_merchant_analytics` | Normalized merchant | Counts, amounts, fraud rate, average amount, customers, suspicious flag |

All fraud rates and large-transaction shares are percentages from 0 to 100. Monetary inputs and sums use two-decimal fixed-point values. P95 uses ClickHouse `quantileExact(0.95)`: the sorted observation at zero-based index `min(floor(0.95 × n), n − 1)`.

`amount_bucket` assigns SMALL below 100, MEDIUM from 100 to below 500, and LARGE at 500 or above. Customer risk is HIGH at fraud rate ≥10%, MEDIUM at ≥1% and below 10%, otherwise LOW. A merchant is suspicious when it has at least two frauds and a fraud rate ≥5%. These are descriptive homework rules, not a trained prediction model.

The CSV has no customer identifier. `customer_key` hashes length-prefixed, normalized first name, last name, street and postal code. This prevents delimiter ambiguity but cannot distinguish people sharing these attributes or connect address changes. Merchant names are trimmed and lowercased. All times are interpreted in UTC; weekday 1 is Monday. The state seed contains the 50 states and Washington, DC.

## Tests and development

Pinned runtime versions are in `requirements.txt`: dbt-core 1.10.15, dbt-clickhouse 1.9.8, SQLFluff 3.4.2 and pre-commit 4.3.0. dbt packages and their resolved versions are in `dbt/packages.yml` and `dbt/package-lock.yml`.

- `dbt_utils` checks combinations of mart keys.
- `dbt_date` derives ISO weekdays through a ClickHouse dispatch macro.
- `dbt_expectations` validates numerical bounds.
- Generic tests cover nulls, uniqueness, labels, categories, states and buckets.
- Singular SQL tests cover business bounds and conservation between layers.
- Native dbt unit tests use fixed inputs and expected results for normalization, bucket/risk boundaries, aggregate grains, P95 and suspicious merchants.
- Python tests cover inherited CSV validation and the atomic loader. The independent audit recomputes every mart group from the CSV.

| Command | Action |
|---|---|
| `make up` | Build tools and start ClickHouse |
| `make load-demo` / `make load-full` | Replace the source with the selected validated CSV |
| `make deps seed run` | Install packages, load states and build models |
| `make test` | Run all dbt data and unit tests |
| `make unit-test` / `make data-test` | Run the selected dbt test type |
| `make python-test` | Run Python regression tests |
| `make lint` | Lint project SQL with SQLFluff's dbt templater |
| `make precommit` | Run configured pre-commit hooks |
| `make verify DATASET=demo` | Compare all mart groups with `data/demo.csv` |
| `make verify DATASET=train` | Compare all mart groups with `data/train.csv` |
| `make docs` | Generate dbt Docs in `dbt/target/` |

To install the Git hook on a development host, install `pre-commit==4.3.0` in a Python virtual environment and run `pre-commit install` after `make demo`. The hooks invoke the Docker tools container. The containerized `make precommit` command runs the same checks without host Python dependencies.

Model and column descriptions are included in dbt Docs. Verification evidence and the Docs DAG screenshot are in [reports/hw4](reports/hw4/RESULTS.md). Runtime receipts and audit outputs are written to `results/`; generated dbt artifacts are ignored by Git. The other files in `reports/` are inherited HW2 benchmark evidence.
