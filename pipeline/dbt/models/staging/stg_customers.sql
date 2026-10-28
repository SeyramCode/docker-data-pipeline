select
    id as customer_id,
    name as customer_name,
    lower(email) as email,
    country,
    created_at,
    updated_at
from {{ source('raw', 'customers') }}
