"""
Pure PySpark transformation functions for CommercePulse.

Every function here takes DataFrames in and returns DataFrames out — no I/O,
no Databricks-only APIs. That makes them unit-testable on a laptop or in
GitHub Actions with a local SparkSession (see databricks/tests/).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ------------------------------ schemas --------------------------------------

ORDER_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("unit_price", T.DoubleType()),
    T.StructField("order_total", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("payment_method", T.StringType()),
])

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("session_id", T.StringType()),
    T.StructField("device", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("product_category", T.StringType()),
    T.StructField("order", ORDER_SCHEMA),
])

VALID_EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "checkout", "purchase"]


# ------------------------------ bronze -> silver ------------------------------

def parse_bronze_events(bronze: DataFrame) -> DataFrame:
    """Turn raw Kafka records (binary value + metadata) into typed columns."""
    return (
        bronze
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
        )
        .select("topic", "partition", "offset", "kafka_ts", "e.*")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
    )


def clean_events(parsed: DataFrame) -> DataFrame:
    """Silver-quality gate: drop malformed rows, enforce enums, dedupe.

    Dedup rationale: the producer is idempotent but Kafka delivery is
    at-least-once end-to-end, so the same event_id can land twice.
    """
    return (
        parsed
        .filter(F.col("event_id").isNotNull() & F.col("event_ts").isNotNull())
        .filter(F.col("user_id").isNotNull() & F.col("session_id").isNotNull())
        .filter(F.col("event_type").isin(VALID_EVENT_TYPES))
        .dropDuplicates(["event_id"])
    )


def split_orders(silver_events: DataFrame) -> DataFrame:
    """Flatten purchase events into an orders table."""
    return (
        silver_events
        .filter(F.col("event_type") == "purchase")
        .filter(F.col("order").isNotNull())
        .select(
            F.col("order.order_id").alias("order_id"),
            "event_ts", "event_date", "user_id", "session_id", "device",
            "product_id", "product_category",
            F.col("order.quantity").alias("quantity"),
            F.col("order.unit_price").alias("unit_price"),
            F.col("order.order_total").alias("order_total"),
            F.col("order.currency").alias("currency"),
            F.col("order.payment_method").alias("payment_method"),
        )
        .filter(F.col("order_total") > 0)
        .dropDuplicates(["order_id"])
    )


# ------------------------------ silver -> gold --------------------------------

def daily_sales(orders: DataFrame) -> DataFrame:
    return (
        orders
        .groupBy(F.col("event_date").alias("order_date"), "product_category")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
            F.round(F.sum("order_total"), 2).alias("revenue"),
        )
    )


def funnel_daily(events: DataFrame) -> DataFrame:
    return (
        events
        .groupBy(F.col("event_date"))
        .pivot("event_type", VALID_EVENT_TYPES)
        .agg(F.count("event_id"))
        .na.fill(0)
        .withColumnRenamed("page_view", "page_views")
        .withColumnRenamed("product_view", "product_views")
        .withColumnRenamed("add_to_cart", "add_to_carts")
        .withColumnRenamed("checkout", "checkouts")
        .withColumnRenamed("purchase", "purchases")
    )


def user_features(events: DataFrame, orders: DataFrame) -> DataFrame:
    """Per-user behavioral features — training input for the propensity model."""
    behavior = (
        events.groupBy("user_id")
        .agg(
            F.countDistinct("session_id").alias("sessions_30d"),
            F.count("event_id").alias("events_30d"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))
             .alias("cart_adds_30d"),
        )
    )
    purchases = (
        orders.groupBy("user_id")
        .agg(F.countDistinct("order_id").alias("purchases_prior"))
    )
    return (
        behavior.join(purchases, "user_id", "left")
        .na.fill({"purchases_prior": 0})
        .withColumn("did_purchase", (F.col("purchases_prior") > 0).cast("int"))
    )
