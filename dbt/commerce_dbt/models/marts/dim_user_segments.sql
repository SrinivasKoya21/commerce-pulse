-- Mart: user segments combining behavior and the ML propensity score.
-- Marketing-ready: who should get the win-back email vs the VIP offer?

select
    user_id,
    sessions_30d,
    events_30d,
    cart_adds_30d,
    purchases_prior,
    propensity_score,
    score_band,
    case
        when purchases_prior >= 3                       then 'LOYAL'
        when purchases_prior >= 1                       then 'CUSTOMER'
        when score_band = 'HIGH'                        then 'HOT_PROSPECT'
        when cart_adds_30d > 0                          then 'CART_ABANDONER'
        else 'BROWSER'
    end as segment,
    model_version,
    scored_at
from {{ ref('stg_user_propensity') }}
