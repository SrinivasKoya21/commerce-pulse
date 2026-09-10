# Databricks job 07 — push Gold tables (and ML outputs) into Snowflake RAW.
#
# Primary load path: the Spark–Snowflake connector (ships with Databricks
# runtimes as `net.snowflake.spark.snowflake`, source name "snowflake").
# The alternative stage/COPY INTO/Snowpipe path lives in snowflake/02_stage_copy.sql.

from config import spark_conf_for_adls, snowflake_options, GOLD

spark_conf_for_adls(spark, dbutils)  # noqa: F821
sf_opts = snowflake_options(dbutils)  # noqa: F821

EXPORTS = {
    # delta path                      -> snowflake table
    f"{GOLD}/daily_sales":            "RAW_DAILY_SALES",
    f"{GOLD}/funnel_daily":           "RAW_FUNNEL_DAILY",
    f"{GOLD}/review_sentiment":       "RAW_REVIEW_SENTIMENT",
    f"{GOLD}/user_propensity":        "RAW_USER_PROPENSITY",
}

for path, table in EXPORTS.items():
    try:
        df = spark.read.format("delta").load(path)  # noqa: F821
    except Exception as exc:  # table may not exist until ML jobs have run
        print(f"skip {table}: {exc}")
        continue

    (df.write.format("snowflake")
       .options(**sf_opts)
       .option("dbtable", table)
       .mode("overwrite")          # full refresh of small gold tables;
       .save())                    # incremental loads are dbt's job downstream
    print(f"loaded {table}: {df.count():,} rows")

print("Snowflake export complete.")
