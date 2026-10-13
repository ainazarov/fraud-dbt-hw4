with customer_metrics as (
    select
        customer_id,
        count() as transaction_count,
        countIf(is_fraud = 1) as fraud_count,
        sum(amount) as total_amount,
        sumIf(amount, is_fraud = 1) as fraud_amount,
        100.0 * countIf(is_fraud = 1) / count() as fraud_rate,
        avg(amount) as avg_amount,
        min(transaction_time) as first_transaction_time,
        max(transaction_time) as last_transaction_time
    from {{ ref('stg_transactions') }}
    group by customer_id
)

select
    customer_id,
    transaction_count,
    fraud_count,
    total_amount,
    fraud_amount,
    fraud_rate,
    avg_amount,
    first_transaction_time,
    last_transaction_time,
    case
        when fraud_rate >= 10 then 'HIGH'
        when fraud_rate >= 1 then 'MEDIUM'
        else 'LOW'
    end as risk_level
from customer_metrics
