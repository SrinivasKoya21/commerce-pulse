# Databricks job 02 — Silver: parse, validate, dedupe bronze into typed tables.
#
# Runs as an incremental stream too (readStream on the bronze Delta table),
# so bronze->silver keeps exactly-once semantics via the checkpoint,
# and foreachBatch + MERGE makes re-runs idempotent.

from delta.tables import DeltaTable

from config import spark_conf_for_adls, BRONZE, SILVER, CHECKPOINTS
from transforms import parse_bronze_events, clean_events, split_orders

spark_conf_for_adls(spark, dbutils)  # noqa: F821

bronze_stream = spark.readStream.format("delta").load(f"{BRONZE}/events")  # noqa: F821

EVENTS_PATH = f"{SILVER}/events"
ORDERS_PATH = f"{SILVER}/orders"


def upsert_batch(batch_df, batch_id: int) -> None:
    """MERGE each micro-batch — replays after failure can't create duplicates."""
    events = clean_events(parse_bronze_events(batch_df)).cache()

    if not DeltaTable.isDeltaTable(spark, EVENTS_PATH):          # noqa: F821
        events.write.format("delta").partitionBy("event_date").save(EVENTS_PATH)
    else:
        (DeltaTable.forPath(spark, EVENTS_PATH).alias("t")       # noqa: F821
         .merge(events.alias("s"), "t.event_id = s.event_id")
         .whenNotMatchedInsertAll()
         .execute())

    orders = split_orders(events)
    if not DeltaTable.isDeltaTable(spark, ORDERS_PATH):          # noqa: F821
        orders.write.format("delta").partitionBy("event_date").save(ORDERS_PATH)
    else:
        (DeltaTable.forPath(spark, ORDERS_PATH).alias("t")       # noqa: F821
         .merge(orders.alias("s"), "t.order_id = s.order_id")
         .whenNotMatchedInsertAll()
         .execute())

    events.unpersist()


query = (
    bronze_stream.writeStream
    .foreachBatch(upsert_batch)
    .option("checkpointLocation", f"{CHECKPOINTS}/silver_events")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
print("Silver build complete.")
