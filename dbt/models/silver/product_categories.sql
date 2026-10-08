{{ config(materialized='ephemeral') }}

with categories_src as (
    {{ latest_catalog_load('category_translation') }}
),

categories_ranked as (
    select
        product_category_name as category_name,
        nullIf(trimBoth(product_category_name_english), '') as english_name,
        row_number() over (partition by product_category_name order by product_category_name_english) as occurrence
    from categories_src
    where product_category_name is not null
)

select
    category_name as product_category_name,
    english_name as product_category_name_english
from categories_ranked
where occurrence = 1
