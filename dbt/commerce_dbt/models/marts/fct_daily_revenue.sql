-- Mart: daily revenue with 7-day rolling trend and category share.
-- This is the table the Tableau revenue view sits on.

with sales as (
    select * from {{ ref('stg_daily_sales') }}
),

daily as (
    select
        order_date,
        product_category,
        orders,
        units,
        revenue,
        revenue / nullif(sum(revenue) over (partition by order_date), 0)
            as category_revenue_share
    from sales
)

select
    *,
    round(
        avg(revenue) over (
            partition by product_category
            order by order_date
            rows between 6 preceding and current row
        ), 2
    ) as revenue_7d_avg
from daily
