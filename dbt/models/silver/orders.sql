{{ config(order_by='(business_date, order_id)') }}

select
    {{ business_date() }} as business_date,
    assumeNotNull(raw_order_id) as order_id,
    assumeNotNull(raw_customer_id) as customer_id,
    assumeNotNull(raw_order_status) as order_status,
    purchased_at,
    estimated_delivery_at,
    delivered_at
from {{ ref('orders_checked') }}
where failure_reason is null
