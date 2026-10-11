-- Same exact semantics over the optimized, unaggregated detail table.
SELECT
    us_state,
    tupleElement(maximum, 1) AS cat_id,
    tupleElement(maximum, 2) AS max_amount
FROM
(
    SELECT us_state, argMax(
        tuple(CAST(cat_id AS String), amount, transaction_id),
        tuple(amount, CAST(cat_id AS String), transaction_id)
    ) AS maximum
    FROM fraud.transactions
    WHERE dataset = {dataset:String}
    GROUP BY us_state
)
ORDER BY us_state;
