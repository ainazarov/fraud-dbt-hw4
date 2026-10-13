with merchant_metrics as (
    select
        merchant,
        count() as transaction_count,
        countIf(is_fraud = 1) as fraud_count,
        sum(amount) as total_amount,
        sumIf(amount, is_fraud = 1) as fraud_amount,
        100.0 * countIf(is_fraud = 1) / count() as fraud_rate,
        avg(amount) as avg_amount,
        uniqExact(customer_id) as customer_count
    from {{ ref('stg_transactions') }}
    group by merchant
)

select
    merchant,
    transaction_count,
    fraud_count,
    total_amount,
    fraud_amount,
    fraud_rate,
    avg_amount,
    customer_count,
    toUInt8(fraud_count >= 2 and fraud_rate >= 5) as is_suspicious
from merchant_metrics
