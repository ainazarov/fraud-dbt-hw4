select
    transaction_date,
    state,
    count() as transaction_count,
    countIf(is_fraud = 1) as fraud_count,
    sum(amount) as total_amount,
    sumIf(amount, is_fraud = 1) as fraud_amount,
    100.0 * countIf(is_fraud = 1) / count() as fraud_rate,
    avg(amount) as avg_amount,
    100.0 * countIf(amount_bucket = 'LARGE') / count() as large_transaction_share,
    -- SQLFluff 3.4.2 does not parse ClickHouse parametric aggregates.
    quantileExact(0.95)(amount) as p95_amount -- noqa: AL03, PRS
from {{ ref('stg_transactions') }}
group by transaction_date, state
