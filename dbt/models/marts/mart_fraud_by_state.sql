select
    state,
    count() as transaction_count,
    countIf(is_fraud = 1) as fraud_count,
    sum(amount) as total_amount,
    sumIf(amount, is_fraud = 1) as fraud_amount,
    100.0 * countIf(is_fraud = 1) / count() as fraud_rate,
    uniqExact(customer_id) as customer_count,
    uniqExact(merchant) as merchant_count
from {{ ref('stg_transactions') }}
group by state
