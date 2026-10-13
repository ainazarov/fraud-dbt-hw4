select
    'CA' as state,
    toDate('2024-01-01') as transaction_date,
    toDecimal64(number * 100, 2) as amount,
    toUInt8(number = 19) as is_fraud,
    multiIf(number < 1, 'SMALL', number < 5, 'MEDIUM', 'LARGE') as amount_bucket
from numbers(20)
