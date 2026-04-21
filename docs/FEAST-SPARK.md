# Feast SparkOfflineStore + SparkComputeEngine — Architecture Findings

**Date:** 2026-04-08  
**Repo investigated:** `/Users/abdhumal/Dev/RedHatDev/feast` — ODH midstream fork  
**Branch:** `midstream/master` at commit `e39b6c2` (`opendatahub-io/feast`)  
**Feast version deployed by RHOAI:** `0.62.0`  
**Reference:** [Feast Production Deployment Topologies](https://github.com/ntkathole/feast/blob/prod_deploy/docs/how-to-guides/production-deployment-topologies.md)

---

## TL;DR

The RHOAI Feast Operator **supports** `SparkOfflineStore` and `SparkComputeEngine` at the CR level, but the **default feature-server image** (`quay.io/feastdev/feature-server:0.62.0`) ships only `feast[minimal]` — no `pyspark`. A custom image is required. SparkComputeEngine runs Spark **in-process inside the feast pod** (not via external SparkApplication CRDs). Upgrading from `dask` → `spark` is the correct architecture for >100M rows.

---

## What the Local ODH Feast Repo Offers

### Repository Details

| Property | Value |
|----------|-------|
| **Remote** | `midstream` → `https://github.com/opendatahub-io/feast.git` |
| **Commit** | `e39b6c2eccbcdb35291e672836dc0c353e7491e0` |
| **Feast version** | `0.62.0` |
| **Base image** | `registry.access.redhat.com/ubi9/python-312-minimal` (Python 3.12) |
| **Deployed image** | `quay.io/feastdev/feature-server:0.62.0` |
| **RHOAI params.env** | `RELATED_IMAGE_FEATURE_SERVER=quay.io/feastdev/feature-server:0.62.0` |

### Key Source Locations

| Component | Path |
|-----------|------|
| `SparkOfflineStore` | `sdk/python/feast/infra/offline_stores/contrib/spark_offline_store/spark.py` |
| `SparkSource` | `sdk/python/feast/infra/offline_stores/contrib/spark_offline_store/spark_source.py` |
| `SparkComputeEngine` | `sdk/python/feast/infra/compute_engines/spark/compute.py` |
| `SparkMaterializationJob` | `sdk/python/feast/infra/compute_engines/spark/job.py` |
| Spark init template | `sdk/python/feast/templates/spark/` |
| Operator API types | `infra/feast-operator/api/v1/featurestore_types.go` |
| Operator repo config | `infra/feast-operator/internal/controller/services/repo_config.go` |
| RHOAI overlay | `infra/feast-operator/config/overlays/rhoai/` |

---

## FeatureStore CR — Available Fields for Spark

### `spec.batchEngine` — inject SparkComputeEngine

```go
type BatchEngineConfig struct {
    ConfigMapRef *corev1.LocalObjectReference `json:"configMapRef,omitempty"`
    ConfigMapKey string                        `json:"configMapKey,omitempty"` // default: "config"
}
```

The operator reads the ConfigMap and injects it into `feature_store.yaml` as:

```yaml
batch_engine:
  type: spark.engine
  spark_conf: { ... }
  staging_location: "s3a://..."
  partitions: 0
```

The `type` field must be `"spark.engine"` (the `SparkComputeEngineConfig` literal).

### `spec.services.offlineStore.persistence.store.type` — supported values

The CRD enum explicitly includes **`spark`** alongside `snowflake.offline`, `bigquery`, `redshift`, `postgres`, `trino`, `athena`, `mssql`, `couchbase.offline`, `clickhouse`, `ray`, `oracle`.

```go
// +kubebuilder:validation:Enum=snowflake.offline;bigquery;redshift;spark;postgres;...
Type string `json:"type"`
SecretRef corev1.LocalObjectReference `json:"secretRef"`
```

### `spec.feastProjectDir.init.template: spark`

Bootstraps the feature repository from the built-in Spark template, which generates:

```yaml
# feature_store.yaml
offline_store:
  type: spark
  spark_conf:
    spark.master: "local[*]"
    spark.sql.execution.arrow.pyspark.enabled: "true"
    spark.sql.session.timeZone: "UTC"
```

```python
# feature_definitions.py (example)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource

user_features = SparkSource(
    path="s3a://smartshop-features/user_features/",
    file_format="parquet",
    timestamp_field="event_timestamp",
)
```

---

## Critical Finding: What SparkComputeEngine Actually Does

**SparkComputeEngine does NOT submit SparkApplication CRDs to the Spark Operator.** It runs Spark **in-process inside the feast server pod** using a local `SparkSession`.

```python
# sdk/python/feast/infra/compute_engines/spark/compute.py
def materialize_single(self, registry, feature_view, start_date, end_date, project):
    offline_job = self.offline_store.pull_latest_from_table_or_query(
        config=self.repo_config,
        data_source=feature_view.batch_source,   # reads SparkSource
        ...
    )
    spark_df = offline_job.to_spark_df()          # SparkSession in feast pod
    if self.repo_config.batch_engine.partitions != 0:
        spark_df = spark_df.repartition(...)

    spark_df.mapInPandas(                         # writes to online store
        lambda x: map_in_pandas(x, serialized_artifacts),
        "status int"
    ).count()
```

### Spark master mode determines execution model

| `spark.master` | What happens | Use case |
|----------------|-------------|----------|
| `local[*]` | All processing in-pod (no executor pods) | Demo / small data (<10 GiB) |
| `k8s://https://kubernetes.default.svc` | Feast pod = Spark driver; executor pods spun up in k8s | Production / large data |
| `spark://host:7077` | Standalone Spark cluster | Self-managed Spark clusters |

For the SmartShop demo (1.7 GiB feature Parquet for materialization), **`local[*]` is sufficient** and eliminates executor RBAC complexity.

### Architecture: current vs Spark-backed

```
CURRENT:
  SparkApplication (RHOAI Spark Operator)
    → Feature Parquet on MinIO (100+ GiB ETL data, 1.7 GiB feature views)
  Feast pod (dask in-process)
    → reads FileSource (s3:// via pyarrow + s3fs)
    → writes to Redis

SPARK-BACKED:
  SparkApplication (RHOAI Spark Operator)           ← same, unchanged
    → Feature Parquet on MinIO
  Feast pod (SparkSession local[*] or k8s//)        ← changed
    → reads SparkSource (s3a:// via hadoop-aws)
    → writes to Redis
```

The ETL `SparkApplication` manifests are unchanged. Only the **Parquet → Redis** materialization step switches from dask to Spark.

---

## What Changes Are Needed

### 1. Custom feast-spark image

The default image installs `feast[minimal]` only. `SparkOfflineStore` requires `pyspark>=4.0.0` (required by `feast[spark]` in v0.62.0).

```dockerfile
# build/Containerfile.feast-spark
FROM quay.io/feastdev/feature-server:0.62.0

USER 0

# pyspark 4.0 requires Python 3.9+ — base image is Python 3.12 ✅
RUN pip install --no-cache-dir \
    pyspark==4.0.0 \
    "feast[spark]==0.62.0"

# Hadoop-AWS JARs for s3a:// support in local[*] mode
# (Spark 4.0 bundles Hadoop 3.4; match the version)
ENV SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark
RUN curl -fsSL -o $SPARK_HOME/jars/hadoop-aws-3.4.0.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar && \
    curl -fsSL -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.367.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

USER 1001
```

### 2. BuildConfig for the feast-spark image

```yaml
# infrastructure/openshift/feast-spark-buildconfig.yaml
apiVersion: build.openshift.io/v1
kind: BuildConfig
metadata:
  name: feast-spark-server
  namespace: smartshop
spec:
  source:
    type: Git
    git:
      uri: https://github.com/abhijeet-dhumal/prod-ml-demo.git
      ref: feat/phase3-complete-rapids-docs-mlflow
    contextDir: "."
  strategy:
    type: Docker
    dockerStrategy:
      dockerfilePath: build/Containerfile.feast-spark
  output:
    to:
      kind: ImageStreamTag
      name: feast-spark-server:latest
```

### 3. `feast-spark-engine` ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: feast-spark-engine
  namespace: smartshop
data:
  config: |
    type: spark.engine
    spark_conf:
      spark.master: "local[*]"
      spark.ui.enabled: "false"
      spark.sql.session.timeZone: "UTC"
      spark.sql.execution.arrow.pyspark.enabled: "true"
      spark.sql.execution.arrow.fallback.enabled: "true"
      spark.hadoop.fs.s3a.endpoint: "http://minio.smartshop.svc.cluster.local:9000"
      spark.hadoop.fs.s3a.path.style.access: "true"
      spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
      spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
```

### 4. `feast-spark-config` Secret (SparkOfflineStore config)

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-spark-config
  namespace: smartshop
stringData:
  spark: |
    spark_conf:
      spark.master: "local[*]"
      spark.sql.session.timeZone: "UTC"
      spark.sql.execution.arrow.pyspark.enabled: "true"
      spark.hadoop.fs.s3a.endpoint: "http://minio.smartshop.svc.cluster.local:9000"
      spark.hadoop.fs.s3a.path.style.access: "true"
      spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
      spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
```

### 5. Updated FeatureStore CR

```yaml
# infrastructure/openshift/feast-operator.yaml (updated)
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: smartshop-feast
  namespace: smartshop
spec:
  feastProject: smartshop
  feastProjectDir:
    git:
      url: https://github.com/abhijeet-dhumal/prod-ml-demo.git
      ref: feat/phase3-complete-rapids-docs-mlflow
      featureRepoPath: feast/feature_repo
  batchEngine:
    configMapRef:
      name: feast-spark-engine
  services:
    offlineStore:
      persistence:
        store:
          type: spark
          secretRef:
            name: feast-spark-config
      server:
        image: image-registry.openshift-image-registry.svc:5000/smartshop/feast-spark-server:latest
        envFrom:
          - secretRef:
              name: feast-s3-credentials
    onlineStore:
      persistence:
        store:
          type: redis
          secretRef:
            name: feast-redis-secret
      server:
        envFrom:
          - secretRef:
              name: feast-s3-credentials
    registry:
      local:
        persistence:
          file:
            path: registry.db
            pvc:
              create:
                accessModes:
                  - ReadWriteOnce
                resources:
                  requests:
                    storage: 1Gi
                storageClassName: nfs-csi
              mountPath: /feast-registry
        server:
          envFrom:
            - secretRef:
                name: feast-s3-credentials
          restAPI: true
    ui: {}
```

### 6. `features.py` — swap `FileSource` → `SparkSource`

```python
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource

# s3a:// required for SparkSource (hadoop-aws); s3:// is pyarrow/fsspec only
user_features_source = SparkSource(
    name="user_features_source",
    path="s3a://smartshop-features/user_features/",
    file_format="parquet",
    timestamp_field="event_timestamp",
)

item_features_source = SparkSource(
    name="item_features_source",
    path="s3a://smartshop-features/item_features/",
    file_format="parquet",
    timestamp_field="event_timestamp",
)

review_embeddings_source = SparkSource(
    name="review_embeddings_source",
    path="s3a://smartshop-embeddings/review_embeddings/",
    file_format="parquet",
    timestamp_field="event_timestamp",
)
```

Note: `s3a://` protocol is required for SparkSource (uses hadoop-aws). The `s3_endpoint_override` parameter used by `FileSource` is replaced by `spark.hadoop.fs.s3a.endpoint` in the engine config.

### 7. `feature_store.yaml` (local dev)

```yaml
project: smartshop
provider: local

registry:
  registry_type: sql
  path: "postgresql+psycopg2://${PG_USER}:${PG_PASSWORD}@${PG_HOST:-localhost}:5432/${PG_DATABASE:-mlflow}"
  cache_mode: thread
  cache_ttl_seconds: 60

offline_store:
  type: spark
  spark_conf:
    spark.master: "local[*]"
    spark.ui.enabled: "false"
    spark.sql.session.timeZone: "UTC"
    spark.sql.execution.arrow.pyspark.enabled: "true"
    spark.hadoop.fs.s3a.endpoint: "${AWS_ENDPOINT_URL_S3}"
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
    spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"

online_store:
  type: redis
  connection_string: "${REDIS_HOST:-localhost}:${REDIS_PORT:-6379},password=${REDIS_PASSWORD}"

entity_key_serialization_version: 3
```

---

## Blockers and Known Issues

### pyspark version mismatch

`feast[spark]==0.62.0` requires `pyspark>=4.0.0`. PySpark 4.0 is a major version that:
- Requires Python ≥ 3.9 (base image is Python 3.12 ✅)
- Uses Spark 4.0.0 internals (different from our ETL Spark 3.5.3)
- The feast pod runs its own SparkSession — **independent** from the Spark Operator's `apache/spark:3.5.3` executors used for ETL. No version conflict.

### Hadoop-AWS JAR version alignment

PySpark 4.0 bundles Hadoop 3.4.x. Use `hadoop-aws-3.4.0.jar` + `aws-java-sdk-bundle-1.12.367.jar`. Wrong versions cause `ClassNotFound` or `NoSuchMethodError`.

### `s3a://` vs `s3://` protocol

| Source type | Protocol | S3 library |
|-------------|----------|------------|
| `FileSource` | `s3://` | pyarrow `s3fs` / fsspec |
| `SparkSource` | `s3a://` | Hadoop S3AFileSystem (hadoop-aws JAR) |

Paths in `features.py` must change from `s3://` → `s3a://` when switching to `SparkSource`.

### Feature server image pull secret

The internal registry image (`image-registry.openshift-image-registry.svc:5000/...`) is accessible without pull secrets from within the same namespace. Confirm with `oc get imagestream feast-spark-server -n smartshop`.

---

## Performance: Why SparkComputeEngine > dask for scale

From the [production deployment topologies doc](https://github.com/ntkathole/feast/blob/prod_deploy/docs/how-to-guides/production-deployment-topologies.md#materialization-performance):

| Data volume | Recommended engine |
|-------------|-------------------|
| <1M rows | In-process (dask/default) ✅ |
| 1M–100M rows | Spark, Ray, or Snowflake |
| **>100M rows** | **Spark on Kubernetes** ← our dataset |

SmartShop dataset: 140M raw reviews → 1.7 GiB materialized feature views (user + item). At current scale, dask works. At full 571M-row scale, Spark is required.

For OpenShift / on-prem, the recommended stack is explicitly:
> **Offline Store: Spark + MinIO · Compute Engine: Spark**

---

## Execution Order for Phase 4 Upgrade

```
1. Build feast-spark-server image (BuildConfig)    ~10 min
2. Update features.py (FileSource → SparkSource)   local
3. Update feature_store.yaml                        local
4. Create feast-spark-engine ConfigMap              oc apply
5. Create feast-spark-config Secret                 oc apply
6. Update FeatureStore CR (offlineStore + batchEngine + image)  oc apply
7. Wait for feast pod restart + ready               ~2 min
8. feast apply (re-register SparkSource views)      oc exec
9. feast materialize-incremental                    oc exec / cron
```

Steps 2–6 can be done while the image builds. Step 7+ require the build to complete.

---

## What Does NOT Change

- All ETL `SparkApplication` YAML manifests (`spark-application-rapids.yaml`, `spark-application-cpu-baseline.yaml`, `spark-application-text-preprocessing.yaml`, `spark-application-embedding.yaml`) remain **exactly as-is**. Spark ETL is independent of Feast's internal SparkSession.
- The Redis online store, PostgreSQL registry, MinIO bucket layout — all unchanged.
- Model training jobs (`trainjobs.yaml`) — unchanged. They read features via `feast.get_online_features()` from Redis, not from the offline store.
- `collect-run-metrics.sh`, `wait-and-materialize.sh` — largely unchanged; only the `feast materialize-incremental` step now benefits from SparkComputeEngine internally.
