-- Fast final query: merge winner states across ALL parts before extracting.
-- Correct immediately after inserts, including before any background merge.
SELECT
    us_state,
    tupleElement(maximum, 1) AS cat_id,
    tupleElement(maximum, 2) AS max_amount
FROM
(
    SELECT us_state, argMaxMerge(winner) AS maximum
    FROM fraud.state_max
    WHERE dataset = {dataset:String}
    GROUP BY us_state
)
ORDER BY us_state;
