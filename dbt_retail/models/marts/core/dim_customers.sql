select distinct
  customer_id,
  customer_name,
  customer_city,
  country_fr
from {{ ref('stg_sales') }}
where customer_id is not null
