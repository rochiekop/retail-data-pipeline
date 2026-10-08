{{ config(materialized='view') }}

-- A view over the star, so it always reflects the current catalog (no category history).
-- Left join: a Product that has since left the catalog keeps its Revenue, as Uncategorized.
-- The category is renamed in the outer select: aliasing coalesce(product_category, ...) to its own
-- input name would break the ClickHouse alias rule.
select
    business_date,
    report_category as product_category,
    revenue,
    order_count,
    order_item_count
from (
    select
        business_date,
        -- A view runs with its reader's settings, so a join miss may be '' rather than NULL.
        coalesce(nullIf(product_category, ''), 'Uncategorized') as report_category,
        sum(price) as revenue,
        uniqExact(order_id) as order_count,
        count() as order_item_count
    from {{ ref('fact_order_items') }} as report_items
    left join {{ ref('dim_product') }} as report_products using (product_id)
    where counts_as_revenue = 1
    group by business_date, report_category
) as report_totals
