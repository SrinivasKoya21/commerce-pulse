select
    user_id,
    sessions_30d,
    events_30d,
    cart_adds_30d,
    purchases_prior,
    propensity_score,
    upper(score_band)     as score_band,
    model_version,
    scored_at,
    loaded_at
from {{ source('raw', 'raw_user_propensity') }}
where user_id is not null
  and propensity_score between 0 and 1
