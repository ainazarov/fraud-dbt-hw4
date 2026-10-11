-- Category of the largest INDIVIDUAL transaction, not the largest category sum.
-- Ties: largest category lexicographically, then largest transaction_id.
-- Pass --param_dataset=train to clickhouse-client (or HTTP param_dataset).
SELECT
    us_state,
    tupleElement(maximum, 1) AS cat_id,
    tupleElement(maximum, 2) AS max_amount
FROM
(
    SELECT us_state, argMax(
        tuple(cat_id, amount, transaction_id),
        tuple(amount, cat_id, transaction_id)
    ) AS maximum
    FROM fraud.transactions_baseline
    WHERE dataset = {dataset:String}
    GROUP BY us_state
)
ORDER BY us_state;
