# Databricks job 03 — Gold: business-level aggregates, written as Delta AND
# as Parquet exports (the Parquet copies feed Snowflake COPY INTO / Snowpipe).

from config import spark_conf_for_adls, SILVER, GOLD
from transforms import daily_sales, funnel_daily, user_features

spark_conf_for_adls(spark, dbutils)  # noqa: F821

events = spark.read.format("delta").load(f"{SILVER}/events")   # noqa: F821
orders = spark.read.format("delta").load(f"{SILVER}/orders")   # noqa: F821

tables = {
    "daily_sales": daily_sales(orders),
    "funnel_daily": funnel_daily(events),
    "user_features": user_features(events, orders),
}

for name, df in tables.items():
    df.write.format("delta").mode("overwrite").save(f"{GOLD}/{name}")
    # coalesce(1): these aggregates are tiny; one file keeps Snowflake loads tidy
    (df.coalesce(1)
       .write.mode("overwrite")
       .parquet(f"{GOLD}/exports/{name}"))
    print(f"gold.{name}: {df.count():,} rows")

print("Gold build complete.")
