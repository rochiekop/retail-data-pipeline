{{ config(order_by='(business_date, order_id, order_item_id)') }}

select
    {{ business_date() }} as business_date,
    assumeNotNull(raw_order_id) as order_id,
    assumeNotNull(item_number) as order_item_id,
    assumeNotNull(raw_product_id) as product_id,
    raw_seller_id as seller_id,
    assumeNotNull(price_amount) as price,
    assumeNotNull(freight_amount) as freight
from {{ ref('order_items_checked') }}
where failure_reason is null
