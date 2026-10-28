select
    order_date,
    count(*) as orders,
    sum(amount) as revenue,
    round(avg(amount), 2) as avg_order_value
from {{ ref('fct_orders') }}
where status <> 'cancelled'
group by order_date
