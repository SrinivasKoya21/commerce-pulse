"""
Central configuration for all CommercePulse Databricks jobs.

Secrets come from a Databricks secret scope named 'commercepulse':
  databricks secrets create-scope commercepulse
  databricks secrets put-secret commercepulse eh_connection_string
  databricks secrets put-secret commercepulse storage_key
  databricks secrets put-secret commercepulse snowflake_password
"""
import os

STORAGE_ACCOUNT = os.environ.get("CP_STORAGE_ACCOUNT", "cpdatalakeXXXX")  # set in job env
EH_NAMESPACE = os.environ.get("CP_EH_NAMESPACE", "cp-events-XXXX")

ABFSS = f"abfss://{{container}}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

BRONZE = ABFSS.format(container="bronze")
SILVER = ABFSS.format(container="silver")
GOLD = ABFSS.format(container="gold")
RAW = ABFSS.format(container="raw")
CHECKPOINTS = ABFSS.format(container="checkpoints")

KAFKA_BOOTSTRAP = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
TOPICS = "clickstream,orders"


def spark_conf_for_adls(spark, dbutils) -> None:
    """Authorize Spark to the data lake with the account key from secrets.
    (Production would use a service principal + OAuth; key auth keeps the
    free-trial setup simple. Both are explained in the PDF guide.)"""
    key = dbutils.secrets.get("commercepulse", "storage_key")
    spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", key
    )


def kafka_options(dbutils) -> dict:
    """Options for reading Event Hubs through Spark's native Kafka source."""
    conn = dbutils.secrets.get("commercepulse", "eh_connection_string")
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
        f'required username="$ConnectionString" password="{conn}";'
    )
    return {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        "subscribe": TOPICS,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",   # Event Hubs 24h retention can expire offsets
    }


def snowflake_options(dbutils) -> dict:
    return {
        "sfUrl": dbutils.secrets.get("commercepulse", "snowflake_account_url"),
        "sfUser": dbutils.secrets.get("commercepulse", "snowflake_user"),
        "sfPassword": dbutils.secrets.get("commercepulse", "snowflake_password"),
        "sfDatabase": "COMMERCEPULSE",
        "sfSchema": "RAW",
        "sfWarehouse": "CP_WH",
        "sfRole": "CP_TRANSFORM_ROLE",
    }
