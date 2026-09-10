-- Staging: 1:1 with the source, but renamed, typed, and lightly cleaned.
-- Staging models are the ONLY place that reads sources directly.

select
    order_date,
    lower(product_category)             as product_category,
    orders,
    units,
    revenue,
    loaded_at
from {{ source('raw', 'raw_daily_sales') }}
where order_date is not null
  and revenue >= 0
