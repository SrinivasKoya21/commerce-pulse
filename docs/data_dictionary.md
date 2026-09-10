# CommercePulse — Data Dictionary

## Lake (Delta on ADLS Gen2)

| Layer | Table | Grain | Key columns |
|---|---|---|---|
| bronze | `events` | 1 row per Kafka record | topic, partition, offset, value (raw bytes), ingested_at |
| silver | `events` | 1 row per unique event | event_id (PK), event_type, event_ts, user_id, session_id, device, product_id, product_category |
| silver | `orders` | 1 row per order | order_id (PK), event_date, user_id, quantity, unit_price, order_total, payment_method |
| raw | `reviews` | 1 row per sampled review | review_id, product_category, star_rating, review_text, review_date |
| gold | `daily_sales` | day × category | orders, units, revenue |
| gold | `funnel_daily` | day | page_views, product_views, add_to_carts, checkouts, purchases |
| gold | `user_features` | user | sessions_30d, events_30d, cart_adds_30d, purchases_prior, did_purchase |
| gold | `review_sentiment` | review | sentiment_label (POSITIVE/NEGATIVE), sentiment_score ∈ [−1, 1] |
| gold | `user_propensity` | user | propensity_score ∈ [0, 1], score_band, model_version, scored_at |

## Snowflake

RAW schema mirrors the gold exports (`RAW_DAILY_SALES`, `RAW_FUNNEL_DAILY`,
`RAW_REVIEW_SENTIMENT`, `RAW_USER_PROPENSITY`), each with a `LOADED_AT` audit
column that also powers dbt source-freshness checks.

ANALYTICS is dbt-managed: `*_staging` views (`stg_*`) and `*_marts` tables
(`fct_daily_revenue`, `fct_funnel_conversion`, `fct_category_sentiment`,
`dim_user_segments`) — see `dbt/commerce_dbt/models/marts/_marts.yml` for
column-level docs and tests.

## Event contract (Kafka topics `clickstream`, `orders`)

```json
{
  "event_id":  "uuid — idempotency key, dedup anchor",
  "event_type": "page_view | product_view | add_to_cart | checkout | purchase",
  "event_ts":  "ISO-8601 UTC",
  "user_id":   "u000001..u005000 — also the Kafka message key (partition affinity)",
  "session_id": "uuid",
  "device":    "mobile | desktop | tablet",
  "product_id": "ELEC-0001 style",
  "product_category": "electronics | apparel | home_kitchen | beauty | sports | books",
  "order":     "nullable struct — only on purchase events"
}
```
