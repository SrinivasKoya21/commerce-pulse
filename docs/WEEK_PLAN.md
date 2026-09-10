# CommercePulse — 7-Day Build Plan (checkpoint checklist)

Companion to the PDF guide. Each day ends with a git commit and a "Day N done"
message in the Claude chat, where we review together before moving on.

## Day 1 (Thu) — Accounts, tools, repo on GitHub
- [ ] Azure free account created ($200 credit) — `az login` works
- [ ] Snowflake trial created (Enterprise edition, on Azure) — account identifier saved
- [ ] HuggingFace account created
- [ ] Local tools installed: Python 3.11, azure-cli, git, pip packages
- [ ] GitHub repo `commerce-pulse` created, project code pushed
- **Verify:** `az account show` works; repo visible on github.com; CI workflow ran
- **Commit:** "Day 1: project scaffold, accounts ready"

## Day 2 (Fri) — Azure infra + Kafka producer (PDF Phases 1–2)
- [ ] `bash infra/azure_setup.sh` completed; `.env.local` values saved
- [ ] `python producer_kafka.py --eps 20 --duration 900` ran clean
- [ ] Event Hubs portal Metrics show Incoming Messages ~20/sec
- **Commit:** "Day 2: infra provisioned, events streaming through Kafka/Event Hubs"
- Optional: `teardown.sh` if not continuing tomorrow morning

## Day 3 (Sat) — Databricks medallion (PDF Phases 3–4)
- [ ] Databricks workspace open; single-node cluster with 15-min auto-terminate
- [ ] Secret scope `commercepulse` populated (storage_key, eh_connection_string)
- [ ] Repo cloned into Databricks Repos; config.py env vars set
- [ ] Jobs 01 → 02 → 03 ran; sanity queries show events, orders, daily_sales
- **Commit:** "Day 3: bronze/silver/gold medallion live on Delta Lake"

## Day 4 (Sun) — AI layer part 1: sentiment (PDF Phase 5)
- [ ] `datasets`, `transformers`, `torch` installed on cluster
- [ ] Job 04: reviews landed from HuggingFace (~30k rows across 6 categories)
- [ ] Job 05: sentiment scored; model/star agreement printed (~90%)
- **Commit:** "Day 4: DistilBERT sentiment at scale via pandas UDFs"

## Day 5 (Mon) — AI layer part 2 + Snowflake (PDF Phases 6–7)
- [ ] Switched to ML runtime cluster; `lightgbm` installed
- [ ] Job 06: model trained, visible in MLflow UI + Model Registry
- [ ] Snowflake `01_setup.sql` run; snowflake_* secrets added
- [ ] Job 07: all four RAW_ tables loaded (check row counts in Snowflake)
- **Commit:** "Day 5: MLflow propensity model + Snowflake loads"

## Day 6 (Tue) — dbt (PDF Phase 8)
- [ ] `~/.dbt/profiles.yml` configured; `dbt debug` passes
- [ ] `dbt deps && dbt build` — all models green, all tests pass
- [ ] `dbt source freshness` passes
- [ ] `dbt docs generate && dbt docs serve` — lineage graph screenshotted
- **Commit:** "Day 6: dbt marts modeled, 20+ tests passing"

## Day 7 (Wed) — Tableau + CI/CD + polish (PDF Phases 9–10)
- [ ] Tableau connected (Desktop trial or Public via CSV); 4 worksheets + dashboard
- [ ] Dashboard screenshot in docs/img/; added to README
- [ ] GitHub secrets set (DATABRICKS_HOST/TOKEN, SNOWFLAKE_PASSWORD); Actions green
- [ ] README polished: architecture PNG top, screenshots, quick start
- [ ] Repo pinned on GitHub profile
- [ ] `infra/teardown.sh` run — credits protected
- **Commit:** "Day 7: dashboard, CI/CD green, project complete"

## After the week
- Practice the 30-second pitch and whiteboard drill (PDF final chapters)
- Add resume bullets from docs/resume_bullets.md
