select
    event_date,
    page_views,
    product_views,
    add_to_carts,
    checkouts,
    purchases,
    loaded_at
from {{ source('raw', 'raw_funnel_daily') }}
where event_date is not null
