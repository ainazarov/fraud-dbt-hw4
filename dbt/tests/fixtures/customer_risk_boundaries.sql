select
    'high' as customer_id,
    toDecimal64(10, 2) as amount,
    toUInt8(number = 0) as is_fraud,
    toDateTime('2024-01-01 00:00:00', 'UTC') + toIntervalHour(number) as transaction_time
from numbers(10)

union all

select
    'medium' as customer_id,
    toDecimal64(20, 2) as amount,
    toUInt8(number = 0) as is_fraud,
    toDateTime('2024-01-01 00:00:00', 'UTC') + toIntervalHour(number) as transaction_time
from numbers(100)

union all

select
    'low' as customer_id,
    toDecimal64(30, 2) as amount,
    toUInt8(number = 0) as is_fraud,
    toDateTime('2024-01-01 00:00:00', 'UTC') + toIntervalHour(number) as transaction_time
from numbers(101)

union all

select
    'just_below_high' as customer_id,
    toDecimal64(40, 2) as amount,
    toUInt8(number < 10) as is_fraud,
    toDateTime('2024-01-01 00:00:00', 'UTC') + toIntervalHour(number) as transaction_time
from numbers(101)

union all

select
    'no_fraud' as customer_id,
    toDecimal64(0, 2) as amount,
    toUInt8(0) as is_fraud,
    toDateTime('2024-01-01 00:00:00', 'UTC') as transaction_time
