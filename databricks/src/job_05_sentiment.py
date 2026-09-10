# Databricks job 05 — HuggingFace sentiment inference at scale with PySpark.
#
# The interview-worthy pattern here is the *pandas UDF*: instead of calling
# the model row-by-row (agonizingly slow), Spark hands each worker whole
# Arrow batches, and each worker loads DistilBERT ONCE per partition and
# runs vectorized batch inference.
#
# Cluster needs: pip install transformers torch  (job library or %pip)

from collections.abc import Iterator

import pandas as pd
from config import GOLD, RAW, spark_conf_for_adls
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark_conf_for_adls(spark, dbutils)  # noqa: F821

MODEL_NAME = "distilbert-base-uncased-finetuned-sst-2-english"

RESULT_SCHEMA = T.StructType([
    T.StructField("sentiment_label", T.StringType()),
    T.StructField("sentiment_score", T.DoubleType()),
])


@F.pandas_udf(RESULT_SCHEMA)
def sentiment_udf(texts: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    """Iterator-of-series form: model loads once per worker, not once per batch."""
    from transformers import pipeline  # worker-side import

    clf = pipeline(
        "sentiment-analysis",
        model=MODEL_NAME,
        truncation=True,
        max_length=256,
        batch_size=32,
        device=-1,          # CPU; set 0 if you attach a GPU node type
    )
    for batch in texts:
        preds = clf(batch.fillna("").tolist())
        yield pd.DataFrame({
            "sentiment_label": [p["label"] for p in preds],
            "sentiment_score": [
                # signed score: POSITIVE keeps its prob, NEGATIVE goes negative
                p["score"] if p["label"] == "POSITIVE" else -p["score"]
                for p in preds
            ],
        })


reviews = spark.read.format("delta").load(f"{RAW}/reviews")  # noqa: F821

scored = (
    reviews
    .repartition(8)  # more partitions than cores => steady worker utilization
    .withColumn("s", sentiment_udf(F.col("review_text")))
    .select(
        "review_id", "product_category", "star_rating", "review_text",
        F.col("s.sentiment_label").alias("sentiment_label"),
        F.round(F.col("s.sentiment_score"), 4).alias("sentiment_score"),
        "review_date",
    )
)

scored.write.format("delta").mode("overwrite").save(f"{GOLD}/review_sentiment")

# Sanity metric: model vs stars agreement (interview gold — you VALIDATED the model)
agreement = (
    scored.filter(F.col("star_rating").isin(1, 2, 4, 5))
    .withColumn("stars_positive", (F.col("star_rating") >= 4).cast("int"))
    .withColumn("model_positive", (F.col("sentiment_label") == "POSITIVE").cast("int"))
    .agg(F.avg((F.col("stars_positive") == F.col("model_positive")).cast("double")))
    .first()[0]
)
print(f"Sentiment complete. Model/star agreement on polarized reviews: {agreement:.1%}")
