{{ config(materialized='table', engine='MergeTree()', order_by='product_id') }}

-- Star, not snowflake: Product Category is folded into the product dimension (ADR 0004).
select
    product_id,
    product_category_name,
    coalesce(product_category_name_english, product_category_name, 'Uncategorized') as product_category
from {{ ref('products') }}
left join {{ ref('product_categories') }} using (product_category_name)
