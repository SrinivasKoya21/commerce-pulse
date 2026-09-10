"""
CommercePulse — Kafka producer targeting Azure Event Hubs' Kafka endpoint.

This is the piece that proves the "Kafka" line on your resume: Event Hubs
Standard tier speaks the Kafka wire protocol, so a bone-stock
confluent-kafka producer works against it with SASL_SSL/PLAIN auth.

Routing:
  purchase events           -> topic 'orders'
  everything else           -> topic 'clickstream'
Key = user_id, so one user's events stay in one partition (ordering!).

Usage:
  export EH_BOOTSTRAP="<namespace>.servicebus.windows.net:9093"
  export EH_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKey=..."
  python producers/producer_kafka.py --eps 20 --duration 900
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time

from confluent_kafka import Producer

from event_generator import event_stream

TOPIC_CLICKSTREAM = "clickstream"
TOPIC_ORDERS = "orders"


def build_producer() -> Producer:
    bootstrap = os.environ["EH_BOOTSTRAP"]
    conn_str = os.environ["EH_CONNECTION_STRING"]
    conf = {
        "bootstrap.servers": bootstrap,
        # --- Event Hubs Kafka auth: username is literally '$ConnectionString'
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "$ConnectionString",
        "sasl.password": conn_str,
        # --- sensible production-ish settings
        "acks": "all",                    # durability over latency
        "enable.idempotence": True,       # exactly-once producer semantics
        "compression.type": "gzip",
        "linger.ms": 50,                  # micro-batching for throughput
        "client.id": "commercepulse-producer",
    }
    return Producer(conf)


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"DELIVERY FAILED: {err}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eps", type=float, default=20, help="target events/sec")
    parser.add_argument("--duration", type=int, default=900, help="seconds to run")
    args = parser.parse_args()

    producer = build_producer()
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sent, t0 = 0, time.time()
    print(f"Producing ~{args.eps} events/sec for {args.duration}s. Ctrl-C to stop.")

    for event in event_stream(sessions_per_second=args.eps / 3.0):
        if not running or time.time() - t0 > args.duration:
            break
        topic = TOPIC_ORDERS if event["event_type"] == "purchase" else TOPIC_CLICKSTREAM
        producer.produce(
            topic=topic,
            key=event["user_id"].encode(),
            value=json.dumps(event).encode(),
            on_delivery=delivery_report,
        )
        producer.poll(0)  # serve delivery callbacks
        sent += 1
        if sent % 500 == 0:
            print(f"  sent={sent}  elapsed={time.time() - t0:.0f}s")

    producer.flush(30)
    print(f"Done. {sent} events in {time.time() - t0:.0f}s.")


if __name__ == "__main__":
    main()
