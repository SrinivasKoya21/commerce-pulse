-- =============================================================================
-- CommercePulse — alternative load path: external stage + COPY INTO + Snowpipe
-- Demonstrates the classic Snowflake ingestion patterns interviewers ask about.
-- =============================================================================
USE SCHEMA COMMERCEPULSE.RAW;

-- 1. File format for the Parquet exports Databricks writes to the 'gold' container
CREATE FILE FORMAT IF NOT EXISTS FF_PARQUET TYPE = PARQUET;

-- 2. Storage integration (the production-grade way: no keys in SQL).
--    Requires ACCOUNTADMIN + granting the generated app access in Azure AD.
CREATE STORAGE INTEGRATION IF NOT EXISTS CP_AZURE_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<YOUR_AZURE_TENANT_ID>'                     -- az account show
  STORAGE_ALLOWED_LOCATIONS = ('azure://<STORAGE_ACCOUNT>.blob.core.windows.net/gold/');

-- Run DESC STORAGE INTEGRATION CP_AZURE_INT; then follow AZURE_CONSENT_URL and
-- grant the multi-tenant app 'Storage Blob Data Reader' on the storage account.

-- 3. External stage over the gold container
CREATE STAGE IF NOT EXISTS STG_GOLD
  URL = 'azure://<STORAGE_ACCOUNT>.blob.core.windows.net/gold/'
  STORAGE_INTEGRATION = CP_AZURE_INT
  FILE_FORMAT = FF_PARQUET;

-- 4. Manual batch load (idempotent — Snowflake tracks loaded files)
COPY INTO RAW_DAILY_SALES (ORDER_DATE, PRODUCT_CATEGORY, ORDERS, UNITS, REVENUE)
FROM (
  SELECT $1:order_date::DATE,
         $1:product_category::VARCHAR,
         $1:orders::NUMBER,
         $1:units::NUMBER,
         $1:revenue::NUMBER(18,2)
  FROM @STG_GOLD/daily_sales/
)
FILE_FORMAT = (FORMAT_NAME = FF_PARQUET)
ON_ERROR = 'ABORT_STATEMENT';

-- 5. Snowpipe: auto-ingest on file arrival (Event Grid notifications)
CREATE PIPE IF NOT EXISTS PIPE_DAILY_SALES
  AUTO_INGEST = TRUE
  INTEGRATION = 'CP_NOTIFICATION_INT'   -- create a NOTIFICATION INTEGRATION first
AS
COPY INTO RAW_DAILY_SALES (ORDER_DATE, PRODUCT_CATEGORY, ORDERS, UNITS, REVENUE)
FROM (
  SELECT $1:order_date::DATE, $1:product_category::VARCHAR,
         $1:orders::NUMBER, $1:units::NUMBER, $1:revenue::NUMBER(18,2)
  FROM @STG_GOLD/daily_sales/
)
FILE_FORMAT = (FORMAT_NAME = FF_PARQUET);

-- Check pipe status / loaded files:
--   SELECT SYSTEM$PIPE_STATUS('PIPE_DAILY_SALES');
--   SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--     TABLE_NAME=>'RAW_DAILY_SALES', START_TIME=>DATEADD(hour,-24,CURRENT_TIMESTAMP())));
