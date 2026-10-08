{{ config(order_by='(business_date, order_id, payment_sequential)') }}

select
    {{ business_date() }} as business_date,
    assumeNotNull(raw_order_id) as order_id,
    assumeNotNull(sequence_number) as payment_sequential,
    raw_payment_type as payment_type,
    installments as payment_installments,
    assumeNotNull(amount) as payment_value
from {{ ref('payments_checked') }}
where failure_reason is null
