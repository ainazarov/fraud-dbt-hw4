with source_totals as (
    select
        count() as transaction_count,
        countIf(target = 1) as fraud_count,
        sum(amount) as total_amount,
        sumIf(amount, target = 1) as fraud_amount
    from {{ source('transactions_db', 'transactions') }}
),

staging_totals as (
    select
        count() as transaction_count,
        countIf(is_fraud = 1) as fraud_count,
        sum(amount) as total_amount,
        sumIf(amount, is_fraud = 1) as fraud_amount
    from {{ ref('stg_transactions') }}
)

select source_totals.transaction_count
from source_totals
cross join staging_totals
where
    source_totals.transaction_count = 0
    or source_totals.transaction_count != staging_totals.transaction_count
    or source_totals.fraud_count != staging_totals.fraud_count
    or source_totals.total_amount != staging_totals.total_amount
    or source_totals.fraud_amount != staging_totals.fraud_amount
