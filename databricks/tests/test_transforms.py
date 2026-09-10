"""
Unit tests for the pure PySpark transforms — run locally & in CI, no cloud needed.

    pip install pyspark==3.5.* pytest
    pytest databricks/tests -q
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from transforms import (
    clean_events,
    daily_sales,
    funnel_daily,
    parse_bronze_events,
    split_orders,
    user_features,
)


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .appName("cp-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
    yield s
    s.stop()


def _kafka_row(payload: dict, offset: int = 0):
    return {
        "topic": "clickstream",
        "partition": 0,
        "offset": offset,
        "timestamp": datetime.now(timezone.utc),
        "value": bytearray(json.dumps(payload).encode()),
    }


def _event(event_id="e1", event_type="page_view", user="u1", order=None):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_ts": "2026-09-01T12:00:00+00:00",
        "user_id": user,
        "session_id": "s1",
        "device": "mobile",
        "product_id": "ELEC-0001",
        "product_category": "electronics",
        "order": order,
    }


def test_parse_extracts_typed_columns(spark):
    bronze = spark.createDataFrame([_kafka_row(_event())])
    out = parse_bronze_events(bronze).collect()[0]
    assert out.event_id == "e1"
    assert out.event_type == "page_view"
    assert str(out.event_date) == "2026-09-01"


def test_clean_drops_bad_rows_and_dupes(spark):
    rows = [
        _kafka_row(_event("e1"), 0),
        _kafka_row(_event("e1"), 1),                      # duplicate event_id
        _kafka_row(_event("e2", event_type="hack"), 2),   # invalid enum
        _kafka_row({**_event("e3"), "user_id": None}, 3), # null user
    ]
    cleaned = clean_events(parse_bronze_events(spark.createDataFrame(rows)))
    assert cleaned.count() == 1
    assert cleaned.first().event_id == "e1"


def _order(order_id="o1", total=59.98):
    return {"order_id": order_id, "quantity": 2, "unit_price": 29.99,
            "order_total": total, "currency": "USD", "payment_method": "card"}


def test_split_orders_flattens_and_filters(spark):
    rows = [
        _kafka_row(_event("e1", "purchase", order=_order("o1")), 0),
        _kafka_row(_event("e2", "purchase", order=_order("o2", total=0.0)), 1),
        _kafka_row(_event("e3", "page_view"), 2),
    ]
    silver = clean_events(parse_bronze_events(spark.createDataFrame(rows)))
    orders = split_orders(silver)
    assert orders.count() == 1
    row = orders.first()
    assert row.order_id == "o1" and row.order_total == 59.98


def test_daily_sales_aggregates(spark):
    rows = [
        _kafka_row(_event("e1", "purchase", order=_order("o1", 10.0)), 0),
        _kafka_row(_event("e2", "purchase", order=_order("o2", 30.0)), 1),
    ]
    orders = split_orders(clean_events(parse_bronze_events(spark.createDataFrame(rows))))
    agg = daily_sales(orders).first()
    assert agg.orders == 2 and float(agg.revenue) == 40.0


def test_funnel_daily_pivots_all_steps(spark):
    rows = [_kafka_row(_event(f"e{i}", t), i) for i, t in enumerate(
        ["page_view", "page_view", "product_view", "add_to_cart"])]
    silver = clean_events(parse_bronze_events(spark.createDataFrame(rows)))
    f = funnel_daily(silver).first()
    assert f.page_views == 2 and f.product_views == 1
    assert f.add_to_carts == 1 and f.purchases == 0


def test_user_features_labels_purchasers(spark):
    rows = [
        _kafka_row(_event("e1", "page_view", user="u1"), 0),
        _kafka_row(_event("e2", "purchase", user="u1", order=_order("o1")), 1),
        _kafka_row(_event("e3", "page_view", user="u2"), 2),
    ]
    silver = clean_events(parse_bronze_events(spark.createDataFrame(rows)))
    feats = {r.user_id: r for r in
             user_features(silver, split_orders(silver)).collect()}
    assert feats["u1"].did_purchase == 1
    assert feats["u2"].did_purchase == 0
