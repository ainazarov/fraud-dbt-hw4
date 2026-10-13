select
    'customer' as model_name,
    customer_id as entity_id
from {{ ref('mart_customer_risk_profile') }}
where
    first_transaction_time > last_transaction_time
    or risk_level != multiIf(fraud_rate >= 10, 'HIGH', fraud_rate >= 1, 'MEDIUM', 'LOW')

union all

select
    'merchant' as model_name,
    merchant as entity_id
from {{ ref('mart_merchant_analytics') }}
where is_suspicious != toUInt8(fraud_count >= 2 and fraud_rate >= 5)
