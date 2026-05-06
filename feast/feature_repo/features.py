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
        import os, boto3, pyarrow.parquet as pq, io, sys

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get(
                "FEAST_S3_ENDPOINT_URL", "http://minio.smartshop.svc.cluster.local:9000"
            ),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minio"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minio123"),
        )
        meta_tables = []
        for key in [
            "raw/metadata/Electronics_meta.parquet",
            "raw/metadata/Books_meta.parquet",
            "raw/metadata/Home_and_Kitchen_meta.parquet",
        ]:
            try:
                obj = s3.get_object(Bucket="smartshop-raw", Key=key)
                tbl = pq.read_table(
                    io.BytesIO(obj["Body"].read()),
                    columns=["parent_asin", "title", "main_category", "price", "store", "author"],
                )
                meta_tables.append(tbl)
            except Exception:
                pass

        if meta_tables:
            import pyarrow as pa

            meta_arrow = pa.concat_tables(meta_tables, promote_options="permissive")
            meta_pdf = meta_arrow.to_pandas()
            meta_pdf["item_brand"] = meta_pdf["store"].fillna(meta_pdf["author"])
            meta_pdf["item_price"] = meta_pdf["price"].astype(str)
            meta_pdf = meta_pdf[["parent_asin", "title", "main_category", "item_price", "item_brand"]]
            meta_pdf.columns = ["parent_asin", "item_title", "item_category", "item_price", "item_brand"]

            spark = df.sparkSession
            metadata_sdf = spark.createDataFrame(meta_pdf)
            print(f"[item_features] metadata loaded: {len(meta_pdf)} rows via boto3", file=sys.stderr)

            item_feats = review_aggs.join(
                metadata_sdf,
                on="parent_asin",
                how="left",
            )
        else:
            raise RuntimeError("No metadata files found in S3")

    except Exception as e:
        import sys
        print(f"[item_features] metadata join failed: {e}", file=sys.stderr)
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
