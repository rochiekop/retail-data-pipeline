{{ config(materialized='table', engine='MergeTree()', order_by='calendar_date') }}

select
    toDate('2016-01-01') + number as calendar_date,
    toYear(calendar_date) as year,
    toMonth(calendar_date) as month,
    toDayOfMonth(calendar_date) as day_of_month,
    toDayOfWeek(calendar_date) as day_of_week,
    toDayOfWeek(calendar_date) >= 6 as is_weekend
from numbers(toUInt64(dateDiff('day', toDate('2016-01-01'), toDate('2018-12-31')) + 1))
