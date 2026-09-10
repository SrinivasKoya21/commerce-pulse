-- =============================================================================
-- CommercePulse — Snowflake bootstrap
-- Run in a Snowflake worksheet as ACCOUNTADMIN (free 30-day trial, $400 credits)
-- =============================================================================

-- 1. Compute: one X-Small warehouse, aggressive auto-suspend = near-zero cost
CREATE WAREHOUSE IF NOT EXISTS CP_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60          -- seconds idle before suspend
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- 2. Database + schemas (RAW = loaded from Azure, ANALYTICS = dbt-managed)
CREATE DATABASE IF NOT EXISTS COMMERCEPULSE;
CREATE SCHEMA IF NOT EXISTS COMMERCEPULSE.RAW;
CREATE SCHEMA IF NOT EXISTS COMMERCEPULSE.ANALYTICS;

-- 3. Role for dbt / pipelines (least privilege, resume-worthy habit)
CREATE ROLE IF NOT EXISTS CP_TRANSFORM_ROLE;
GRANT USAGE ON WAREHOUSE CP_WH                          TO ROLE CP_TRANSFORM_ROLE;
GRANT USAGE ON DATABASE COMMERCEPULSE                   TO ROLE CP_TRANSFORM_ROLE;
GRANT ALL   ON SCHEMA COMMERCEPULSE.RAW                 TO ROLE CP_TRANSFORM_ROLE;
GRANT ALL   ON SCHEMA COMMERCEPULSE.ANALYTICS           TO ROLE CP_TRANSFORM_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA COMMERCEPULSE.RAW TO ROLE CP_TRANSFORM_ROLE;
GRANT ROLE CP_TRANSFORM_ROLE TO USER "<YOUR_SNOWFLAKE_USER>";   -- <-- edit

-- 4. Raw landing tables (Gold-layer exports from Databricks land here)
USE SCHEMA COMMERCEPULSE.RAW;

CREATE TABLE IF NOT EXISTS RAW_DAILY_SALES (
  ORDER_DATE        DATE,
  PRODUCT_CATEGORY  VARCHAR,
  ORDERS            NUMBER,
  UNITS             NUMBER,
  REVENUE           NUMBER(18,2),
  LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_FUNNEL_DAILY (
  EVENT_DATE   DATE,
  PAGE_VIEWS   NUMBER,
  PRODUCT_VIEWS NUMBER,
  ADD_TO_CARTS NUMBER,
  CHECKOUTS    NUMBER,
  PURCHASES    NUMBER,
  LOADED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_REVIEW_SENTIMENT (
  REVIEW_ID        VARCHAR,
  PRODUCT_CATEGORY VARCHAR,
  STAR_RATING      NUMBER,
  REVIEW_TEXT      VARCHAR,
  SENTIMENT_LABEL  VARCHAR,
  SENTIMENT_SCORE  FLOAT,
  REVIEW_DATE      DATE,
  LOADED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_USER_PROPENSITY (
  USER_ID            VARCHAR,
  SESSIONS_30D       NUMBER,
  EVENTS_30D         NUMBER,
  CART_ADDS_30D      NUMBER,
  PURCHASES_PRIOR    NUMBER,
  PROPENSITY_SCORE   FLOAT,
  SCORE_BAND         VARCHAR,       -- HIGH / MEDIUM / LOW
  MODEL_VERSION      VARCHAR,
  SCORED_AT          TIMESTAMP_NTZ,
  LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 5. (Optional path B) External stage on ADLS for COPY INTO loading.
--    The primary load path in this project is the Spark–Snowflake connector,
--    but recruiters love seeing you know stages + COPY too. See 02_stage_copy.sql.
