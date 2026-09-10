# Databricks job 06 — train a purchase-propensity model with MLflow.
#
# Full MLOps story in one job:
#   1. features from the gold layer  ->  2. train/validate LightGBM
#   3. log params/metrics/artifacts to MLflow  ->  4. register the model
#   5. batch-score every user  ->  6. write scores to gold for Snowflake/Tableau
#
# Cluster needs: pip install lightgbm  (mlflow + sklearn ship in ML runtimes;
# use a "15.4 LTS ML" runtime for this job)

import mlflow
import mlflow.sklearn
import pandas as pd
from lightgbm import LGBMClassifier
from pyspark.sql import functions as F
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split

from config import spark_conf_for_adls, GOLD

spark_conf_for_adls(spark, dbutils)  # noqa: F821

MODEL_NAME = "cp_purchase_propensity"
FEATURES = ["sessions_30d", "events_30d", "cart_adds_30d"]
TARGET = "did_purchase"

# ---------------------------------------------------------------- features --
pdf: pd.DataFrame = (
    spark.read.format("delta").load(f"{GOLD}/user_features").toPandas()  # noqa: F821
)
X_train, X_val, y_train, y_val = train_test_split(
    pdf[FEATURES], pdf[TARGET], test_size=0.25, random_state=42,
    stratify=pdf[TARGET],
)

# ------------------------------------------------------------------- train --
mlflow.set_experiment("/Shared/commercepulse-propensity")

with mlflow.start_run(run_name="lgbm_baseline") as run:
    params = dict(
        n_estimators=300, learning_rate=0.05, num_leaves=31,
        class_weight="balanced", random_state=42,
    )
    model = LGBMClassifier(**params).fit(X_train, y_train)

    proba = model.predict_proba(X_val)[:, 1]
    metrics = {
        "roc_auc": roc_auc_score(y_val, proba),
        "avg_precision": average_precision_score(y_val, proba),
        "positive_rate": float(y_val.mean()),
    }

    mlflow.log_params(params)
    mlflow.log_metrics(metrics)
    mlflow.sklearn.log_model(
        model, artifact_path="model",
        input_example=X_train.head(3),
        registered_model_name=MODEL_NAME,      # -> Model Registry, version++
    )
    print(f"run={run.info.run_id}  AUC={metrics['roc_auc']:.3f}  "
          f"AP={metrics['avg_precision']:.3f}")

# -------------------------------------------------------------- batch score --
latest = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/latest")
pdf["propensity_score"] = latest.predict(pdf[FEATURES])
# pyfunc on a classifier returns labels; use the sklearn flavor for probabilities:
skl = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/latest")
pdf["propensity_score"] = skl.predict_proba(pdf[FEATURES])[:, 1]

pdf["score_band"] = pd.cut(
    pdf["propensity_score"], bins=[-0.01, 0.33, 0.66, 1.0],
    labels=["LOW", "MEDIUM", "HIGH"],
).astype(str)

scores = (
    spark.createDataFrame(  # noqa: F821
        pdf[["user_id", *FEATURES, "purchases_prior",
             "propensity_score", "score_band"]]
    )
    .withColumn("model_version", F.lit("latest"))
    .withColumn("scored_at", F.current_timestamp())
)
scores.write.format("delta").mode("overwrite").save(f"{GOLD}/user_propensity")
print(f"Scored {scores.count():,} users -> gold/user_propensity.")
