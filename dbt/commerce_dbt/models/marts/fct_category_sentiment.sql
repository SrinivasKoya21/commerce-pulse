-- Mart: per-category sentiment health — joins the AI layer to commerce data.
-- "Which categories make money but disappoint customers?" is the money slide.

with sentiment as (
    select * from {{ ref('stg_review_sentiment') }}
),

by_category as (
    select
        product_category,
        count(*)                                            as reviews,
        avg(star_rating)                                    as avg_stars,
        avg(sentiment_score)                                as avg_sentiment,
        sum(iff(sentiment_label = 'POSITIVE', 1, 0))
            / nullif(count(*), 0)                           as pct_positive
    from sentiment
    group by 1
),

revenue as (
    select product_category, sum(revenue) as total_revenue
    from {{ ref('stg_daily_sales') }}
    group by 1
)

select
    c.product_category,
    c.reviews,
    round(c.avg_stars, 2)        as avg_stars,
    round(c.avg_sentiment, 3)    as avg_sentiment,
    round(c.pct_positive, 3)     as pct_positive,
    r.total_revenue,
    case
        when c.avg_sentiment >= 0.5  then 'HEALTHY'
        when c.avg_sentiment >= 0    then 'WATCH'
        else 'AT RISK'
    end as sentiment_status
from by_category c
left join revenue r using (product_category)
