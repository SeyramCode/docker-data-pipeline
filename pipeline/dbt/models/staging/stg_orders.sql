select
    id as order_id,
    customer_id,
    cast(order_date as date) as order_date,
    lower(status) as status,
    cast(amount as numeric(18, 2)) as amount,
    updated_at
from {{ source('raw', 'orders') }}
