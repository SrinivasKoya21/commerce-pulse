# Tableau — CommercePulse Executive Dashboard

## Setup (free options)

**Option A — Tableau Desktop 14-day trial (recommended).** Native Snowflake
connector, live or extract. Download from tableau.com, pick "Snowflake" under
Connect → To a Server.

**Option B — Tableau Public (free forever).** No Snowflake connector; export the
four marts to CSV first:

```sql
-- run in a Snowflake worksheet, then download each result as CSV
SELECT * FROM COMMERCEPULSE.ANALYTICS_MARTS.FCT_DAILY_REVENUE;
SELECT * FROM COMMERCEPULSE.ANALYTICS_MARTS.FCT_FUNNEL_CONVERSION;
SELECT * FROM COMMERCEPULSE.ANALYTICS_MARTS.FCT_CATEGORY_SENTIMENT;
SELECT * FROM COMMERCEPULSE.ANALYTICS_MARTS.DIM_USER_SEGMENTS;
```

Publishing to Tableau Public gives you a **shareable dashboard URL for your
resume** — that alone is worth doing.

## Connect (Option A)

Server: `<account_identifier>.snowflakecomputing.com` · Role `CP_TRANSFORM_ROLE`
· Warehouse `CP_WH` · Database `COMMERCEPULSE` · Schema `ANALYTICS_MARTS`.
Use **Extract** mode so the X-Small warehouse suspends between refreshes.

## Build 4 worksheets

1. **Revenue Trend** — `fct_daily_revenue`: Columns `ORDER_DATE` (continuous day),
   Rows `SUM(REVENUE)`, Color `PRODUCT_CATEGORY`; add `REVENUE_7D_AVG` as a
   dual-axis line, synchronized axes.
2. **Conversion Funnel** — `fct_funnel_conversion`: Measure Names/Values bar chart
   of `PAGE_VIEWS → PRODUCT_VIEWS → ADD_TO_CARTS → CHECKOUTS → PURCHASES`,
   sorted descending; label each bar with % of previous step.
3. **Sentiment vs Revenue** — `fct_category_sentiment`: scatter with
   `TOTAL_REVENUE` (X), `AVG_SENTIMENT` (Y), size `REVIEWS`, color
   `SENTIMENT_STATUS`, label `PRODUCT_CATEGORY`. The "high revenue, low
   sentiment" quadrant is your headline insight.
4. **ML Segments** — `dim_user_segments`: bar of user counts by `SEGMENT`,
   color by `SCORE_BAND`; add average `PROPENSITY_SCORE` labels.

## Assemble the dashboard

New Dashboard (1200×800, fixed) → title "CommercePulse — Executive Overview" →
Revenue Trend across the top, the other three below → make
`PRODUCT_CATEGORY` a global filter (Apply to All Using This Data Source) →
add a date-range filter on the trend.

Screenshot the finished dashboard into `docs/img/dashboard.png` — it goes in
the PDF, your GitHub README, and LinkedIn.
