{{ config(materialized='ephemeral') }}

with items_src as (
    {{ latest_bronze_load('order_items') }}
),

items_typed as (
    select
        order_id as raw_order_id,
        order_item_id as raw_order_item_id,
        product_id as raw_product_id,
        seller_id as raw_seller_id,
        toInt32OrNull(order_item_id) as item_number,
        toDecimal64OrNull(price, 2) as price_amount,
        toDecimal64OrNull(freight_value, 2) as freight_amount,
        _loaded_at as loaded_at,
        -- Duplicates are judged on the parsed number, so "01" and "1" are the same Order Item. A copy that
        -- passes its own checks ranks first (the Order and Product lookups come later); every column then
        -- breaks ties, so a rerun keeps the same copy.
        row_number() over (
            partition by order_id, toInt32OrNull(order_item_id)
            order by
                product_id is null
                    or toDecimal64OrNull(price, 2) is null or toDecimal64OrNull(price, 2) < 0
                    or toDecimal64OrNull(freight_value, 2) is null or toDecimal64OrNull(freight_value, 2) < 0,
                product_id, price, freight_value, seller_id, shipping_limit_date, order_item_id
        ) as occurrence
    from items_src
),

item_parent_orders as (
    select raw_order_id as parent_order_id, failure_reason as parent_failure_reason
    from {{ ref('orders_checked') }}
    where occurrence = 1
)

select
    t.*,
    case
        when t.raw_order_id is null then 'missing order_id'
        when t.item_number is null then 'invalid order_item_id'
        when t.occurrence > 1 then 'duplicate order item'
        when o.parent_order_id is null then 'unknown order'
        when o.parent_failure_reason is not null then 'parent order quarantined'
        when t.raw_product_id is null then 'missing product_id'
        when p.product_id is null then 'unknown product'
        when t.price_amount is null then 'invalid price'
        when t.price_amount < 0 then 'negative price'
        when t.freight_amount is null then 'invalid freight_value'
        when t.freight_amount < 0 then 'negative freight_value'
    end as failure_reason
from items_typed t
left join item_parent_orders o on o.parent_order_id = t.raw_order_id
left join {{ ref('products') }} p on p.product_id = t.raw_product_id
