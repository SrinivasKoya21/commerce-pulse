# Databricks job 04 — load a real HuggingFace review dataset into the lake.
#
# Dataset: McAuley-Lab/Amazon-Reviews-2023 — millions of genuine Amazon
# reviews, free to use for research/learning. We sample a manageable slice
# per category so the free-trial cluster finishes in minutes, then map it
# onto CommercePulse's product categories.
#
# Cluster needs: pip install datasets  (add as a job library or %pip)

import re

from pyspark.sql import functions as F

from config import spark_conf_for_adls, RAW

spark_conf_for_adls(spark, dbutils)  # noqa: F821

# HF subset name -> CommercePulse category
CATEGORY_MAP = {
    "raw_review_Electronics": "electronics",
    "raw_review_Amazon_Fashion": "apparel",
    "raw_review_Home_and_Kitchen": "home_kitchen",
    "raw_review_All_Beauty": "beauty",
    "raw_review_Sports_and_Outdoors": "sports",
    "raw_review_Books": "books",
}
SAMPLE_PER_CATEGORY = 5_000   # bump to 50k+ later to show scale; start small


def load_category(hf_subset: str, category: str):
    """Stream a slice of one category from HuggingFace (no full download)."""
    from datasets import load_dataset  # imported lazily: driver-only dependency

    ds = load_dataset(
        "McAuley-Lab/Amazon-Reviews-2023",
        hf_subset,
        split="full",
        streaming=True,          # <- streams over HTTP, no 20GB downloads
        trust_remote_code=True,
    )
    rows = []
    for i, r in enumerate(ds):
        if i >= SAMPLE_PER_CATEGORY:
            break
        text = (r.get("text") or "").strip()
        if not text:
            continue
        rows.append({
            "review_id": f"{category}-{i}",
            "product_category": category,
            "star_rating": int(r.get("rating") or 0),
            "review_title": (r.get("title") or "")[:200],
            "review_text": re.sub(r"\s+", " ", text)[:2000],
            "review_ts": int(r.get("timestamp") or 0),
        })
    return rows


all_rows = []
for subset, category in CATEGORY_MAP.items():
    part = load_category(subset, category)
    all_rows.extend(part)
    print(f"{category}: {len(part):,} reviews")

reviews = (
    spark.createDataFrame(all_rows)  # noqa: F821
    .withColumn("review_date", F.to_date(F.from_unixtime(F.col("review_ts") / 1000)))
    .drop("review_ts")
    .filter(F.col("star_rating").between(1, 5))
)

reviews.write.format("delta").mode("overwrite") \
    .partitionBy("product_category").save(f"{RAW}/reviews")
print(f"Landed {reviews.count():,} reviews in raw/reviews.")
