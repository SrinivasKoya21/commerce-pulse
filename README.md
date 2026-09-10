# CommercePulse — Real-Time E-Commerce Analytics & ML Platform

An end-to-end, production-style data platform built on **Azure + Databricks + Kafka (Event Hubs) + Snowflake + dbt + PySpark + Tableau**, with an AI/ML layer (HuggingFace transformers + MLflow) and full CI/CD via GitHub Actions.

```
                        ┌──────────────────────────────  AZURE  ─────────────────────────────┐
 Python Event           │                                                                    │
 Generator ──Kafka──►  Event Hubs  ──►  Databricks (PySpark Structured Streaming)            │
 (clickstream,          │ (Kafka API)         │                                              │
  orders)               │              Delta Lake on ADLS Gen2                               │
                        │              Bronze ─► Silver ─► Gold  (medallion)                 │
 HuggingFace            │                        │                                           │
 Amazon Reviews ──batch─┼────────────────────────┤                                           │
                        │              ┌─────────┴──────────┐                                │
                        │        HuggingFace           MLflow model                          │
                        │        sentiment UDF         (purchase propensity)                 │
                        └─────────────────────┬──────────────────────────────────────────────┘
                                              │ Snowflake Spark connector / COPY INTO
                                              ▼
                                         SNOWFLAKE  ──►  dbt Core (staging → marts, tests, docs)
                                              │
                                              ▼
                                          TABLEAU  (executive dashboard)

                    CI/CD: GitHub Actions — pytest + chispa, dbt slim CI, Databricks Asset Bundles
```

## The story (for recruiters)

CommercePulse simulates a mid-size e-commerce company that needs:

1. **Real-time behavioral analytics** — what are users doing *right now*? (clickstream via Kafka/Event Hubs → Spark Structured Streaming)
2. **Reliable batch analytics** — daily revenue, funnel conversion, product performance (Delta Lake medallion → Snowflake → dbt marts)
3. **AI-driven insight** — customer sentiment from product reviews (HuggingFace DistilBERT at scale) and purchase-propensity scoring (MLflow-tracked LightGBM)
4. **Trustworthy delivery** — unit-tested transforms, dbt data tests, CI/CD deploys, documented lineage

## Repo layout

| Path | What lives here |
|---|---|
| `infra/` | Azure CLI provisioning + teardown scripts, Snowflake bootstrap SQL |
| `producers/` | Event generator + Kafka producer (Event Hubs Kafka endpoint) |
| `databricks/` | PySpark jobs (bronze/silver/gold, exports), Asset Bundle config, unit tests |
| `ml/` | HuggingFace sentiment inference, MLflow training pipeline |
| `snowflake/` | Warehouse DDL, stages, COPY INTO / Snowpipe |
| `dbt/commerce_dbt/` | dbt Core project: sources, staging, marts, tests |
| `tableau/` | Dashboard build guide |
| `.github/workflows/` | CI (lint, pytest, dbt) and CD (Asset Bundle deploy) |
| `docs/` | Architecture diagrams, data dictionary, resume bullets, interview prep |

## Quick start

1. `infra/azure_setup.sh` — provisions everything in Azure (~10 min)
2. `snowflake/01_setup.sql` — bootstrap Snowflake trial account
3. `producers/producer_kafka.py` — start streaming events
4. Run Databricks jobs 01→03 (streaming) and 04→07 (batch + ML)
5. `dbt build` in `dbt/commerce_dbt/`
6. Connect Tableau to Snowflake marts → build dashboard
7. **`infra/teardown.sh` when done — protects your $200 credit**

Full step-by-step: see the PDF guide (`docs/CommercePulse_Implementation_Guide.pdf`).

## Cost guardrails (Azure $200 free trial)

- Event Hubs **Standard** (needed for Kafka API): ~$0.03/hr → run only while streaming
- Databricks single-node `Standard_DS3_v2` clusters with 15-min auto-terminate
- ADLS Gen2: pennies at this scale
- Snowflake: separate free trial ($400 credits) — X-Small warehouse, auto-suspend 60 s
- Tableau: Desktop 14-day trial or Tableau Public (free)
- `infra/teardown.sh` deletes the entire resource group in one command
