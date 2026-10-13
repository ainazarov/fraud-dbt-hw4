{% set marts = [
    'mart_daily_state_metrics',
    'mart_fraud_by_category',
    'mart_fraud_by_state',
    'mart_customer_risk_profile',
    'mart_hourly_fraud_pattern',
    'mart_merchant_analytics'
] %}

{% for mart in marts %}
    select
        '{{ mart }}' as model_name,
        transaction_count,
        fraud_count,
        fraud_rate
    from {{ ref(mart) }}
    where
        not isFinite(fraud_rate)
        or fraud_rate < 0
        or fraud_rate > 100
        or fraud_count > transaction_count
        or abs(fraud_rate - 100.0 * fraud_count / transaction_count) > 0.000001
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
