select order_id, payment_sequential
from {{ ref('payments') }}
group by order_id, payment_sequential
having count() > 1
