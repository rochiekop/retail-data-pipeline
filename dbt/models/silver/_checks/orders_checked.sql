{{ config(materialized='ephemeral') }}

-- Source text is exposed as raw_*; never alias an expression to an input column name (ClickHouse).
{% set order_statuses -%}
    ('created', 'approved', 'invoiced', 'processing', 'shipped', 'delivered', 'canceled', 'unavailable')
{%- endset %}
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
        -- Among copies of one key, a copy that passes its own checks ranks first, so a bad copy can't push out
        -- a good one; every column then breaks ties, so a rerun keeps the same copy.
        row_number() over (
            partition by order_id
            order by
                customer_id is null or toDateTimeOrNull(order_purchase_timestamp) is null
                    or order_status is null or order_status not in {{ order_statuses }},
                order_status, order_purchase_timestamp, customer_id, order_approved_at,
                order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
        ) as occurrence
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
        when raw_order_status not in {{ order_statuses }} then 'unknown order_status'
    end as failure_reason
from orders_typed
