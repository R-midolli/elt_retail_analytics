select
  invoice_no,
  stock_code,
  customer_id,
  invoice_date,
  quantity,
  unit_price,
  line_amount
from {{ ref('stg_sales') }}
where
  is_cancellation = false
  and is_return = false
  and quantity > 0
  and unit_price > 0
