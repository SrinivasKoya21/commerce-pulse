select
    review_id,
    lower(product_category)                as product_category,
    star_rating,
    review_text,
    upper(sentiment_label)                 as sentiment_label,
    sentiment_score,
    review_date,
    loaded_at
from {{ source('raw', 'raw_review_sentiment') }}
where review_id is not null
  and star_rating between 1 and 5
