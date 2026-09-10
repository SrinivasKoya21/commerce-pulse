-- Mart: funnel step-through and end-to-end conversion rates per day.

select
    event_date,
    page_views,
    product_views,
    add_to_carts,
    checkouts,
    purchases,
    round(product_views / nullif(page_views, 0), 4)   as view_rate,
    round(add_to_carts / nullif(product_views, 0), 4) as cart_rate,
    round(checkouts    / nullif(add_to_carts, 0), 4)  as checkout_rate,
    round(purchases    / nullif(checkouts, 0), 4)     as purchase_rate,
    round(purchases    / nullif(page_views, 0), 4)    as end_to_end_conversion
from {{ ref('stg_funnel_daily') }}
