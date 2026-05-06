"""Feast feature definitions — @batch_feature_view edition.

Pipeline:
  Raw Parquet (s3a://smartshop-raw/raw/reviews/)
      |  SparkComputeEngine (RAPIDS GPU)
  @batch_feature_view transformation UDFs
      |
  Redis online store  (online=True)
  (no write-back to raw source -- offline=False)

Eliminates the separate SparkApplication for feature engineering.
Feast reads raw S3 data, applies PySpark UDFs, and writes to Redis directly.
"""

from datetime import timedelta

from feast import Entity
from feast.batch_feature_view import batch_feature_view
from feast.field import Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.transformation.mode import TransformationMode
from feast.types import Float32, Float64, Int64, String
from feast.value_type import ValueType

# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------

user = Entity(
    name="user_id",
    value_type=ValueType.STRING,
    description="Unique user identifier from Amazon Reviews",
)

item = Entity(
    name="item_id",
    value_type=ValueType.STRING,
    description="Product ASIN identifier (parent_asin)",
)

# ---------------------------------------------------------------------------
# Raw source — reads all parquet files under raw/reviews/ directly.
# `query=` does an inline CAST(timestamp / 1000 AS TIMESTAMP) so Feast's
# SQL time-range filter works without any separate preprocessing step.
# ---------------------------------------------------------------------------

raw_reviews_source = SparkSource(
    name="raw_reviews_source",
    query=(
        "SELECT *, CAST(timestamp / 1000 AS TIMESTAMP) AS event_timestamp "
        "FROM parquet.`s3a://smartshop-raw/raw/reviews/*/`"
    ),
    timestamp_field="event_timestamp",
)


# ---------------------------------------------------------------------------
# @batch_feature_view — user_features
#
# Reads raw reviews -> computes per-user aggregates -> writes to Redis.
# All imports MUST be inside the function body (dill serialization).
# ---------------------------------------------------------------------------


@batch_feature_view(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="user_avg_rating", dtype=Float64),
        Field(name="user_review_count", dtype=Int64),
        Field(name="user_unique_items", dtype=Int64),
        Field(name="user_avg_review_length", dtype=Float64),
        Field(name="user_category_count", dtype=Int64),
        Field(name="user_tenure_days", dtype=Int64),
    ],
    source=raw_reviews_source,
    mode=TransformationMode.PYTHON,
    online=True,
    offline=False,
)
def user_features(df):
    from pyspark.sql import functions as F

    if "asin" in df.columns and "parent_asin" not in df.columns:
        df = df.withColumnRenamed("asin", "parent_asin")
    if "helpful_vote" not in df.columns:
        df = df.withColumn("helpful_vote", F.lit(0))
    if "category" not in df.columns:
        df = df.withColumn("category", F.input_file_name())

    return (
        df.groupBy("user_id")
        .agg(
            F.avg("rating").alias("user_avg_rating"),
            F.count("*").alias("user_review_count"),
            F.countDistinct("parent_asin").alias("user_unique_items"),
            F.avg(F.length("text")).alias("user_avg_review_length"),
            F.collect_set("category").alias("user_categories"),
            F.max("timestamp").alias("user_last_active"),
            F.min("timestamp").alias("user_first_active"),
        )
        .withColumn("user_category_count", F.size("user_categories"))
        .withColumn(
            "user_tenure_days",
            (
                (F.col("user_last_active") - F.col("user_first_active"))
                / 86_400_000
            ).cast("int"),
        )
        .drop("user_categories", "user_last_active", "user_first_active")
        .withColumn("event_timestamp", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# @batch_feature_view — item_features
#
# Reads raw reviews -> computes per-item aggregates -> writes to Redis.
# Metadata join (title, brand, category, price) done inline via
# spark.read.parquet inside the UDF.
# ---------------------------------------------------------------------------


@batch_feature_view(
    name="item_features",
    entities=[item],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="item_avg_rating", dtype=Float64),
        Field(name="item_rating_stddev", dtype=Float64),
        Field(name="item_review_count", dtype=Int64),
        Field(name="item_total_helpful_votes", dtype=Int64),
        Field(name="item_avg_review_length", dtype=Float64),
        Field(name="item_price", dtype=Float32),
        Field(name="item_title", dtype=String),
        Field(name="item_brand", dtype=String),
        Field(name="item_category", dtype=String),
    ],
    source=raw_reviews_source,
    mode=TransformationMode.PYTHON,
    online=True,
    offline=False,
)
def item_features(df):
    from pyspark.sql import functions as F

    if "asin" in df.columns and "parent_asin" not in df.columns:
        df = df.withColumnRenamed("asin", "parent_asin")
    if "helpful_vote" not in df.columns:
        df = df.withColumn("helpful_vote", F.lit(0))

    review_aggs = df.groupBy("parent_asin").agg(
        F.avg("rating").alias("item_avg_rating"),
        F.stddev("rating").alias("item_rating_stddev"),
        F.count("*").alias("item_review_count"),
        F.sum(F.col("helpful_vote").cast("int")).alias("item_total_helpful_votes"),
        F.avg(F.length("text")).alias("item_avg_review_length"),
    )

    try:
        spark = df.sparkSession
        metadata_df = spark.read.parquet("s3a://smartshop-raw/raw/metadata/")
        item_feats = review_aggs.join(
            metadata_df.select(
                F.col("parent_asin"),
                F.col("price").cast("float").alias("item_price"),
                F.substring(F.col("title"), 1, 120).alias("item_title"),
                F.col("store").alias("item_brand"),
                F.col("main_category").alias("item_category"),
            ),
            on="parent_asin",
            how="left",
        )
    except Exception:
        item_feats = (
            review_aggs
            .withColumn("item_price", F.lit(None).cast("float"))
            .withColumn("item_title", F.lit(None).cast("string"))
            .withColumn("item_brand", F.lit(None).cast("string"))
            .withColumn("item_category", F.lit(None).cast("string"))
        )

    return (
        item_feats
        .withColumnRenamed("parent_asin", "item_id")
        .withColumn("event_timestamp", F.current_timestamp())
    )
