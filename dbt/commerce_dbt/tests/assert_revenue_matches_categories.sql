-- Singular test: category shares must sum to ~1.0 for every day that has revenue.
-- A failing day means double-counting or a broken window function.

with shares as (
    select order_date, sum(category_revenue_share) as total_share
    from {{ ref('fct_daily_revenue') }}
    group by 1
)

select *
from shares
where total_share is not null
  and abs(total_share - 1.0) > 0.001
