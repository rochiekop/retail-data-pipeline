select business_date, product_category
from {{ ref('daily_revenue_by_category') }}
group by business_date, product_category
having count() > 1
