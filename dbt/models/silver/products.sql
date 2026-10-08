{{ config(materialized='ephemeral') }}

with products_src as (
    {{ latest_catalog_load('products') }}
),

products_ranked as (
    select
        assumeNotNull(product_id) as catalog_product_id,
        nullIf(trimBoth(product_category_name), '') as category_name,
        row_number() over (partition by product_id order by product_category_name) as occurrence
    from products_src
    where product_id is not null
)

select
    catalog_product_id as product_id,
    category_name as product_category_name
from products_ranked
where occurrence = 1
