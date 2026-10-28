with orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(*) as order_count,
        sum(amount) filter (where status <> 'cancelled') as lifetime_value
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.customer_name,
    c.email,
    c.country,
    c.created_at,
    o.first_order_date,
    o.last_order_date,
    coalesce(o.order_count, 0) as order_count,
    coalesce(o.lifetime_value, 0) as lifetime_value
from {{ ref('stg_customers') }} c
left join orders o using (customer_id)
