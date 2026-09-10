# Databricks job 01 — Bronze: stream Event Hubs (Kafka API) into Delta Lake.
#
# Pattern: land the raw bytes untouched. Bronze is the "replayable truth" —
# if silver logic changes next month, you re-derive everything from bronze
# without re-reading Kafka (whose retention is only 24h here).

from config import BRONZE, CHECKPOINTS, kafka_options, spark_conf_for_adls

spark_conf_for_adls(spark, dbutils)  # noqa: F821 (spark/dbutils injected by Databricks)

raw_stream = (
    spark.readStream                       # noqa: F821
    .format("kafka")
    .options(**kafka_options(dbutils))     # noqa: F821
    .load()
)

# Keep Kafka metadata — offset/partition are your audit trail & dedup anchor.
bronze = raw_stream.selectExpr(
    "topic", "partition", "offset",
    "timestamp",
    "CAST(key AS STRING) AS key",
    "value",                                # keep raw bytes verbatim
    "current_timestamp() AS ingested_at",
)

query = (
    bronze.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINTS}/bronze_events")
    .partitionBy("topic")
    .trigger(availableNow=True)   # batch-drain mode: process backlog, then stop.
                                  # Swap to .trigger(processingTime="30 seconds")
                                  # for an always-on demo — mind the credits!
    .start(f"{BRONZE}/events")
)
query.awaitTermination()
print("Bronze ingest complete.")
