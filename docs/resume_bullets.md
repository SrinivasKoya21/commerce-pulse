# Resume Bullets & Interview Prep — CommercePulse

## Project header for your resume

**CommercePulse — Real-Time E-Commerce Analytics & ML Platform** *(Personal project)*
*Azure · Event Hubs (Kafka) · Databricks · PySpark · Delta Lake · Snowflake · dbt · MLflow · HuggingFace · Tableau · GitHub Actions*

## Bullets (pick 4–6; keep numbers, recruiters scan for them)

- Designed and built an end-to-end streaming data platform on Azure, ingesting simulated e-commerce clickstream and order events through **Azure Event Hubs' Kafka API** with an idempotent, keyed **confluent-kafka** producer (SASL_SSL, acks=all, gzip batching).
- Implemented a **medallion lakehouse (bronze/silver/gold)** on **Delta Lake/ADLS Gen2** using **PySpark Structured Streaming** with checkpointed exactly-once processing and **MERGE-based idempotent upserts** for deduplication.
- Ran **HuggingFace DistilBERT sentiment inference at scale** over real Amazon review data using **iterator pandas UDFs** (model loaded once per worker, Arrow-vectorized batches), validating predictions at ~90%+ agreement against star ratings.
- Trained and registered a **LightGBM purchase-propensity model with MLflow** (experiment tracking, model registry, batch scoring of 5K users), surfacing HIGH/MEDIUM/LOW segments to marketing dashboards.
- Modeled the warehouse in **Snowflake with dbt** (staging → marts, 20+ data tests, source-freshness SLAs, generated docs/lineage) and loaded it via both the **Spark–Snowflake connector** and **stage + COPY INTO/Snowpipe** patterns.
- Shipped **CI/CD with GitHub Actions**: ruff linting, **pytest unit tests running PySpark locally**, **dbt slim CI** building only changed models per PR, and automated **Databricks Asset Bundle** deployments.
- Delivered an executive **Tableau** dashboard (revenue trends, funnel conversion, sentiment-vs-revenue quadrant, ML segments) on dbt marts.

## 30-second pitch (say this when asked "tell me about your project")

"I built CommercePulse, a real-time e-commerce analytics platform. Clickstream
and order events flow through Azure Event Hubs — which I consume over the
Kafka protocol — into Databricks, where PySpark Structured Streaming lands
them in a Delta Lake medallion architecture. I enrich the data with two ML
layers: DistilBERT sentiment on real Amazon reviews, and a LightGBM
purchase-propensity model tracked in MLflow. Gold tables load into Snowflake,
dbt models them into tested marts, and Tableau sits on top. Everything is
unit-tested and deployed through GitHub Actions CI/CD. I ran it all inside an
Azure free trial, which forced me to think about cost the way a real team
would."

## Interview Q&A by technology

### Kafka / Event Hubs
- **Why Event Hubs instead of self-hosted Kafka?** Managed service, native Azure
  integration, and it exposes the actual Kafka wire protocol — my producer is
  standard confluent-kafka code that would point at any Kafka cluster by
  changing the bootstrap server and auth. Tradeoffs: 24h–90d retention limits,
  no log compaction, throughput units instead of broker sizing.
- **Why key messages by user_id?** Kafka guarantees order only within a
  partition. Keying by user keeps each user's funnel events in sequence, which
  silver-layer sessionization depends on.
- **At-least-once vs exactly-once here?** Producer is idempotent; delivery to
  Spark is at-least-once; end-to-end effective exactly-once comes from
  checkpointing plus MERGE on event_id at the silver layer.

### PySpark / Databricks / Delta
- **Why a medallion architecture?** Bronze preserves replayable raw truth
  (Kafka retention is only 24h — the lake becomes the system of record);
  silver applies quality gates once for all consumers; gold is cheap to
  recompute business logic. Schema changes never force re-ingestion.
- **What does the checkpoint actually store?** Kafka offsets processed per
  micro-batch plus stream state — after a crash, Spark resumes from the last
  committed batch instead of reprocessing or skipping.
- **`trigger(availableNow=True)` vs `processingTime`?** availableNow drains the
  backlog then stops — streaming semantics at batch cost, ideal for a
  credit-limited demo and for many real "hourly stream" workloads.
- **Why pandas UDFs for the transformer model?** A plain Python UDF crosses the
  JVM↔Python boundary per row and would reload the model constantly. The
  iterator pandas UDF form amortizes model load to once per worker and feeds
  it Arrow batches — orders of magnitude faster.

### Snowflake / dbt
- **Why both Databricks AND Snowflake?** Different strengths: Spark for
  streaming + ML on semi-structured data; Snowflake for concurrent BI SQL,
  cheap storage-compute separation, and easy analyst access. Common real-world
  pairing; dbt owns everything inside Snowflake.
- **Warehouse vs database vs schema in Snowflake?** Warehouse = compute
  (resizable, auto-suspend, per-second billing); database/schema = logical
  storage. Compute and storage scale independently.
- **What do your dbt tests catch?** Nulls/dupes on keys, enum drift
  (accepted_values), range violations on rates and scores, a singular test
  proving category revenue shares sum to 1, and source freshness SLAs that
  catch a silent upstream pipeline failure.
- **Staging vs marts?** Staging: 1:1 with sources, rename/cast only, views,
  the only place `source()` is called. Marts: business logic, tested, table-
  materialized, what BI reads. Keeps lineage clean and rework local.

### MLflow / ML
- **Walk me through your MLOps loop.** Features from gold → train/validation
  split → LightGBM → params/metrics/model logged to MLflow → registered
  version in the Model Registry → batch scoring loads `models:/name/latest` →
  scores land in gold → Snowflake → dbt segments → Tableau. Retraining is
  rerunning one job; every run is comparable in the MLflow UI.
- **Why AUC and average precision?** Purchase is the minority class; accuracy
  is misleading. AUC measures ranking quality; average precision focuses on
  the positive class under imbalance (class_weight=balanced during training).

### CI/CD
- **What runs on a PR?** Lint (ruff), PySpark unit tests against a local
  SparkSession (no cluster needed — transforms are pure functions), dbt parse,
  and dbt slim CI that builds only `state:modified+` models into a disposable
  PR schema.
- **How do secrets flow?** Nothing in git. GitHub Actions secrets for
  CI (Snowflake password, Databricks token); Databricks secret scopes for
  runtime (storage key, Event Hubs connection string, Snowflake creds).

### Design/judgment questions
- **Biggest thing you'd change for production?** Service-principal/OAuth auth
  instead of account keys, Unity Catalog governance, Terraform instead of CLI
  scripts, autoscaling multi-node clusters, DLQ topic for poison messages,
  and alerting (job failure → Slack/PagerDuty) on freshness breaches.
- **How would this scale 100×?** More Event Hubs throughput units/partitions
  (or Premium), multi-node autoscaling clusters, Z-ORDER/liquid clustering on
  hot Delta tables, incremental dbt models instead of full refreshes, GPU
  inference for the transformer.
