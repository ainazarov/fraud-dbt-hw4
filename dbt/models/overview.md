{% docs __overview__ %}

# Fraud analytics HW4

The project loads one labelled transaction snapshot into `transactions_db.transactions`,
normalizes it in `stg_transactions`, and rebuilds six analytical tables in
`fraud_analytics`. Seeds contain the 50 US states and DC.

Money is stored and summed as decimals. Fractions named `fraud_rate` and
`large_transaction_share` are percentages (0–100). Dates, ISO weekdays and hours
are interpreted in UTC. Timestamps without an offset are treated as UTC.

Customer identity is a proxy from normalized first name, last name, street and
postal code. Character-length prefixes make the encoded tuple unambiguous;
`dbt_utils` hashes the encoding. The source does not provide a reliable customer ID.

The amount bands are SMALL (<100), MEDIUM (100 to <500), LARGE (>=500).
Customer risk bands are LOW (<1% observed fraud), MEDIUM (1% to <10%), HIGH (>=10%).
A merchant is suspicious when it has at least two fraudulent transactions and
at least 5% observed fraud. Risk levels and flags use observed fraud labels.

P95 uses `quantileExact(0.95)`: sorted index `min(floor(0.95*N), N-1)`, counting
from zero. Other sources may use different percentile interpolation.

The project uses `dbt_utils` for surrogate keys and composite-key tests,
`dbt_date.day_of_week` with a local ClickHouse dispatch implementation, and
`dbt_expectations` for numeric ranges. Data tests cover domains, keys, state
relationships, totals, and business rules. Native dbt unit tests use fixed
fixtures for transformations, thresholds, exact aggregation, and UTC boundaries.

{% enddocs %}
