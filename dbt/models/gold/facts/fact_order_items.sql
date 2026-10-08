{{ config(order_by='(business_date, order_id, order_item_id)') }}

-- Grain: one Order Item. counts_as_revenue applies the Revenue rule from CONTEXT.md.
select
    business_date,
    order_id,
    order_item_id,
    product_id,
    seller_id,
    order_status,
    price,
    freight,
    order_status not in ('canceled', 'unavailable') as counts_as_revenue
from {{ ref('order_items') }} as fact_items
inner join (
    select business_date, order_id, order_status
    from {{ ref('orders') }}
    where business_date = {{ business_date() }}
) as fact_orders using (business_date, order_id)
where business_date = {{ business_date() }}
