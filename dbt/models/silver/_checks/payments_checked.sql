{{ config(materialized='ephemeral') }}

with payments_src as (
    {{ latest_bronze_load('order_payments') }}
),

payments_typed as (
    select
        order_id as raw_order_id,
        payment_sequential as raw_payment_sequential,
        payment_type as raw_payment_type,
        toInt32OrNull(payment_sequential) as sequence_number,
        toInt32OrNull(payment_installments) as installments,
        toDecimal64OrNull(payment_value, 2) as amount,
        _loaded_at as loaded_at,
        -- Duplicates are judged on the parsed number, so "01" and "1" are the same Payment. A copy that
        -- passes its own checks ranks first; every column then breaks ties, so a rerun keeps the same copy.
        row_number() over (
            partition by order_id, toInt32OrNull(payment_sequential)
            order by
                toDecimal64OrNull(payment_value, 2) is null or toDecimal64OrNull(payment_value, 2) < 0,
                payment_type, payment_value, payment_installments, payment_sequential
        ) as occurrence
    from payments_src
),

payment_parent_orders as (
    select raw_order_id as parent_order_id, failure_reason as parent_failure_reason
    from {{ ref('orders_checked') }}
    where occurrence = 1
)

select
    t.*,
    case
        when t.raw_order_id is null then 'missing order_id'
        when t.sequence_number is null then 'invalid payment_sequential'
        when t.occurrence > 1 then 'duplicate payment'
        when o.parent_order_id is null then 'unknown order'
        when o.parent_failure_reason is not null then 'parent order quarantined'
        when t.amount is null then 'invalid payment_value'
        when t.amount < 0 then 'negative payment_value'
    end as failure_reason
from payments_typed t
left join payment_parent_orders o on o.parent_order_id = t.raw_order_id
