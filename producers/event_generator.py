"""
CommercePulse — synthetic e-commerce event generator.

Produces realistic clickstream and order events for a fictional store.
Sessions follow a real funnel: page_view -> product_view -> add_to_cart
-> checkout -> purchase, with realistic drop-off at every step.

This is deliberately dependency-light (stdlib only) so it runs anywhere.
"""
from __future__ import annotations

import random
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone

CATEGORIES = ["electronics", "apparel", "home_kitchen", "beauty", "sports", "books"]

PRODUCTS = {  # category -> (product_id prefix, price range)
    "electronics":  ("ELEC", (25.0, 900.0)),
    "apparel":      ("APRL", (10.0, 120.0)),
    "home_kitchen": ("HOME", (15.0, 300.0)),
    "beauty":       ("BEAU", (8.0, 80.0)),
    "sports":       ("SPRT", (12.0, 250.0)),
    "books":        ("BOOK", (7.0, 45.0)),
}

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.62, 0.31, 0.07]

# Funnel advance probabilities (step -> chance the session continues)
FUNNEL = [
    ("page_view", 1.00),
    ("product_view", 0.72),
    ("add_to_cart", 0.34),
    ("checkout", 0.55),
    ("purchase", 0.78),
]

USER_POOL_SIZE = 5_000  # repeat users make user-level ML features meaningful


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pick_product() -> tuple[str, str, float]:
    category = random.choice(CATEGORIES)
    prefix, (lo, hi) = PRODUCTS[category]
    product_id = f"{prefix}-{random.randint(1, 400):04d}"
    price = round(random.uniform(lo, hi), 2)
    return category, product_id, price


def generate_session() -> Iterator[dict]:
    """Yield the events of ONE user session walking down the funnel."""
    user_id = f"u{random.randint(1, USER_POOL_SIZE):06d}"
    session_id = str(uuid.uuid4())
    device = random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]
    category, product_id, price = _pick_product()
    quantity = random.choices([1, 2, 3], weights=[0.8, 0.15, 0.05], k=1)[0]

    for step, (event_type, p_advance) in enumerate(FUNNEL):
        if step > 0 and random.random() > p_advance:
            return  # user dropped out of the funnel

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_ts": _now_iso(),
            "user_id": user_id,
            "session_id": session_id,
            "device": device,
            "product_id": product_id,
            "product_category": category,
        }

        if event_type == "purchase":
            event["order"] = {
                "order_id": f"ord-{uuid.uuid4().hex[:12]}",
                "quantity": quantity,
                "unit_price": price,
                "order_total": round(price * quantity, 2),
                "currency": "USD",
                "payment_method": random.choice(["card", "paypal", "apple_pay"]),
            }
        yield event


def event_stream(sessions_per_second: float = 3.0) -> Iterator[dict]:
    """Endless stream of events with jittered pacing."""
    while True:
        yield from generate_session()
        time.sleep(max(0.0, random.gauss(1.0 / sessions_per_second, 0.05)))


if __name__ == "__main__":
    import itertools
    import json
    for e in itertools.islice(event_stream(sessions_per_second=50), 10):
        print(json.dumps(e))
