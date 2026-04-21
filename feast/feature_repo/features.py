"""Feast feature definitions for SmartShop AI.

Defines three feature views:
  1. user_features   — per-user aggregates from review history
  2. item_features   — per-item aggregates from reviews + metadata
  3. review_embeddings — vector embeddings for RAG similarity search

Offline store: SparkOfflineStore (reads Parquet from MinIO via s3a://).
  - Materialization: SparkComputeEngine local[*] inside feast-spark-server pod → Redis
  - Training:        feast client in rec-trainer pod, same SparkSource → get_historical_features()
Registry: file-backed PVC (feast server) OR remote gRPC client (training pod).
Online store: Redis (sub-ms lookups at serving time).

SparkSource uses s3a:// (hadoop-aws); FileSource used s3:// (pyarrow/fsspec).
The feast-spark-server image adds pyspark==4.0.0 + hadoop-aws JARs on top of
quay.io/feastdev/feature-server:0.62.0.

See docs/FEAST-SPARK.md for the full architecture and upgrade notes.
"""

from datetime import timedelta

from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.types import Array, Float32, Float64, Int64, String
from feast.value_type import ValueType

# s3a:// is required for SparkSource — Spark uses hadoop-aws (S3AFileSystem).
# The endpoint and credentials come from spark_conf in feature_store.yaml
# (spark.hadoop.fs.s3a.endpoint + EnvironmentVariableCredentialsProvider).
_S3A = "s3a://smartshop-features"
_S3A_EMB = "s3a://smartshop-embeddings"

# -- Entities --

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

review = Entity(
    name="review_id",
    value_type=ValueType.STRING,
    description="Unique review identifier for embedding lookup",
)

# -- Data Sources (MinIO / S3 via SparkSource) --
# Paths written by Spark ETL (spark-application-rapids.yaml).
# feast apply registers schema; feast materialize-incremental reads + writes to Redis.
# Training: get_historical_features() uses the same SparkSource with Spark local[*].

user_features_source = SparkSource(
    name="user_features_source",
    path=f"{_S3A}/user_features/",
    file_format="parquet",
    timestamp_field="event_timestamp",
)

item_features_source = SparkSource(
    name="item_features_source",
    path=f"{_S3A}/item_features/",
    file_format="parquet",
    timestamp_field="event_timestamp",
)

review_embeddings_source = SparkSource(
    name="review_embeddings_source",
    path=f"{_S3A_EMB}/review_embeddings/",
    file_format="parquet",
    timestamp_field="event_timestamp",
)

# -- Feature Views --

user_features_view = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[
        Field(name="user_avg_rating", dtype=Float64),
        Field(name="user_review_count", dtype=Int64),
        Field(name="user_unique_items", dtype=Int64),
        Field(name="user_avg_review_length", dtype=Float64),
        Field(name="user_category_count", dtype=Int64),
        Field(name="user_tenure_days", dtype=Int64),
    ],
    source=user_features_source,
    online=True,
)

item_features_view = FeatureView(
    name="item_features",
    entities=[item],
    ttl=timedelta(days=30),
    schema=[
        Field(name="item_avg_rating", dtype=Float64),
        Field(name="item_rating_stddev", dtype=Float64),
        Field(name="item_review_count", dtype=Int64),
        Field(name="item_total_helpful_votes", dtype=Int64),
        Field(name="item_avg_review_length", dtype=Float64),
        Field(name="item_price", dtype=Float32),
        # item_price_bucket and category excluded — string fields, not usable
        # as float32 tensor inputs in TwoTowerModel (item_feat_dim=6 numeric only)
    ],
    source=item_features_source,
    online=True,
)

review_embeddings_view = FeatureView(
    name="review_embeddings",
    entities=[review],
    ttl=timedelta(days=90),
    schema=[
        Field(name="item_id", dtype=String),
        Field(name="user_id", dtype=String),
        Field(name="rating", dtype=Float64),
        Field(name="review_title", dtype=String),
        Field(name="embed_text", dtype=String),
        Field(name="embedding", dtype=Array(Float32)),
    ],
    source=review_embeddings_source,
    online=True,
)
