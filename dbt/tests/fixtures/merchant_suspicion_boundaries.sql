select
    'at_threshold' as merchant,
    'customer' as customer_id,
    toDecimal64(10, 2) as amount,
    toUInt8(number < 2) as is_fraud
from numbers(40)

union all

select
    'below_threshold' as merchant,
    'customer' as customer_id,
    toDecimal64(20, 2) as amount,
    toUInt8(number < 2) as is_fraud
from numbers(41)

union all

select
    'one_fraud' as merchant,
    'customer' as customer_id,
    toDecimal64(30, 2) as amount,
    toUInt8(1) as is_fraud

union all

select
    'two_frauds' as merchant,
    'customer' as customer_id,
    toDecimal64(40, 2) as amount,
    toUInt8(1) as is_fraud
from numbers(2)
