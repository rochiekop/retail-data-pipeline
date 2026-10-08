{{ config(materialized='ephemeral') }}

-- Source text is exposed as raw_*; never alias an expression to an input column name (ClickHouse).
with orders_src as (
    {{ latest_bronze_load('orders') }}
),

orders_typed as (
    select
        order_id as raw_order_id,
        customer_id as raw_customer_id,
        order_status as raw_order_status,
        toDateTimeOrNull(order_purchase_timestamp) as purchased_at,
        toDateTimeOrNull(order_estimated_delivery_date) as estimated_delivery_at,
        toDateTimeOrNull(order_delivered_customer_date) as delivered_at,
        _loaded_at as loaded_at,
        row_number() over (partition by order_id order by order_status, order_purchase_timestamp) as occurrence
    from orders_src
)

select
    *,
    case
        when raw_order_id is null then 'missing order_id'
        when occurrence > 1 then 'duplicate order_id'
        when raw_customer_id is null then 'missing customer_id'
        when purchased_at is null then 'invalid order_purchase_timestamp'
        when raw_order_status is null then 'missing order_status'
        when raw_order_status not in (
            'created', 'approved', 'invoiced', 'processing', 'shipped', 'delivered', 'canceled', 'unavailable'
        ) then 'unknown order_status'
    end as failure_reason
from orders_typed
