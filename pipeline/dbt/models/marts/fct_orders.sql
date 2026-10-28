select
    o.order_id,
    o.customer_id,
    c.country,
    o.order_date,
    o.status,
    o.amount
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c using (customer_id)
