{{ config(order_by='(business_date, entity, record_key)') }}

-- The full rejected record stays in Bronze; (entity, record_key, loaded_at) points to it.
select
    {{ business_date() }} as business_date,
    'order' as entity,
    ifNull(raw_order_id, '(missing)') as record_key,
    failure_reason,
    loaded_at
from {{ ref('orders_checked') }}
where failure_reason is not null

union all

select
    {{ business_date() }},
    'order_item',
    concat(ifNull(raw_order_id, '?'), '/', ifNull(raw_order_item_id, '?')),
    failure_reason,
    loaded_at
from {{ ref('order_items_checked') }}
where failure_reason is not null

union all

select
    {{ business_date() }},
    'payment',
    concat(ifNull(raw_order_id, '?'), '/', ifNull(raw_payment_sequential, '?')),
    failure_reason,
    loaded_at
from {{ ref('payments_checked') }}
where failure_reason is not null
