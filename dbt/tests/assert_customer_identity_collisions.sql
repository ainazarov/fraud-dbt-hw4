select customer_id
from {{ ref('stg_transactions') }}
group by customer_id
having uniqExact(tuple(first_name, last_name, street, postal_code)) > 1
