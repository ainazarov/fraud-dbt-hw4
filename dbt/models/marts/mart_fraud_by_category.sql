select
    category,
    count() as transaction_count,
    countIf(is_fraud = 1) as fraud_count,
    sum(amount) as total_amount,
    sumIf(amount, is_fraud = 1) as fraud_amount,
    100.0 * countIf(is_fraud = 1) / count() as fraud_rate
from {{ ref('stg_transactions') }}
group by category
