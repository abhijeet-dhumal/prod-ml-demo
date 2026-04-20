# Infrastructure Gaps, Workflow Improvements & Productisation Roadmap

**Context:** This document captures findings from building a production-grade distributed ML
pipeline (Spark RAPIDS + Kubeflow + Feast + Slurm FSDP) on Red Hat OpenShift AI.
It is intended as a community reference for what works, what is rough, and what needs
to be provided natively by the platform for teams to adopt these patterns at scale.

---

## 1. Data Ingestion

### Current approach
Kubernetes Job streams HuggingFace Hub → MinIO via `s3fs`. Works but is hand-rolled.

### Gaps

| Gap | Impact | Improvement |
|---|---|---|
| No native dataset ingestion primitive | Every project reinvents a download Job | RHOAI should provide a **Data Connection + Dataset Import** wizard that generates a K8s Job from a URI (HF Hub, S3, GCS, HTTP) |
| HuggingFace `datasets` library API instability | `trust_remote_code` removed; streaming API changed across minor versions | Pin `datasets==2.x` in images; RHOAI workbench images should include a stable pinned version |
| No progress visibility for long-running download | 22 GB Electronics JSONL has no progress bar in `oc logs` | Add tqdm-style periodic logging + emit Prometheus counter `rows_downloaded_total` |
| Local disk exhaustion for large datasets | 49 GB total dataset cannot be downloaded on a laptop | Enforce cluster-side ingestion as the default path; document this clearly |
| No data versioning | Re-running the download job overwrites existing data silently | Integrate **DVC** or **Pachyderm** for dataset versioning, or use MinIO object versioning |

### Recommended platform addition
> **RHOAI Data Sources panel** (similar to Model Registry but for datasets) that tracks
> dataset URI, size, download date, schema, and links to the pipeline that consumed it.

---

## 2. Spark on Kubeflow (SparkApplication)

### Current approach
`SparkApplication` CR via `spark-operator`. Two image variants: CPU (`spark-jobs`) and
GPU/RAPIDS (`spark-jobs-rapids`). Two separate YAML manifests differ only by image and
a `sparkConf` block.

### Gaps

| Gap | Impact | Improvement |
|---|---|---|
| No official RHOAI-supported Spark runtime image | Must build and maintain `Containerfile.spark-jobs` manually; version drift risk | RHOAI should ship a **supported UBI9-based Spark 3.5 image** with pre-installed s3a connector, PyArrow, and Python 3.11 |
| RAPIDS image must be hand-built | `rapids-4-spark` JAR + CUDA compatibility is fragile (cuda12 JAR on CUDA 13.0 cluster) | NVIDIA and Red Hat should co-publish a **certified RAPIDS + Spark image** in the RHOAI catalog |
| `spark.plugins` config requires manual CUDA version matching | Easy to break across upgrades | RHOAI GPU operator should auto-inject the correct RAPIDS plugin version based on detected CUDA driver |
| No Spark History Server deployed by default | Post-run stage/SQL analysis requires port-forwarding to a running driver (ephemeral) | RHOAI should deploy a **Spark History Server** backed by MinIO/S3, auto-configured when SparkOperator is enabled |
| `envsubst` pipeline for manifest substitution is fragile | Shell quoting, zsh plugins, and `<placeholder>` values in `.env` all cause silent failures | Provide a **Helm chart or Kustomize overlay** for SparkApplication manifests with values files |
| S3A credentials provider mismatch | `EnvironmentVariableCredentialsProvider` expects `AWS_*` env vars, but MinIO secrets use `MINIO_*` naming | RHOAI Data Connection secrets should always inject both `AWS_*` and provider-specific aliases |
| Spark driver pod runs as root | Blocked by default OpenShift SCCs; requires custom SCC or `anyuid` | RHOAI should document the exact SCC needed for SparkOperator and optionally ship a `spark-scc` ClusterRole |

### Recommended platform addition
> **RHOAI Pipelines → Spark Job** UI: submit a SparkApplication from the dashboard,
> pick CPU or GPU runtime from a dropdown, configure S3 input/output, and get a
> live link to the Spark History Server — no YAML required.

---

## 3. RAPIDS GPU Acceleration

### Current approach
`rapids-4-spark` JAR pre-placed in the Docker image. `spark.plugins=com.nvidia.spark.SQLPlugin`
activates it. Verified working on A100 with CUDA 13.0 + cuda12 JAR (NVIDIA backward compat).

### Gaps

| Gap | Impact | Improvement |
|---|---|---|
| CUDA driver / JAR version mismatch is silent | Wrong JAR falls back to CPU silently with no warning | RAPIDS should emit a startup warning when JAR CUDA version != driver CUDA version |
| `spark.rapids.sql.explain=ALL` output is very verbose | Hard to parse GPU vs CPU coverage from raw logs | RHOAI Spark UI should highlight GPU-accelerated stages in a different color |
| UDFs and `input_file_name()` silently fall back to CPU | Developers don't know which operations are NOT accelerated | RAPIDS documentation should list all non-acceleratable operations prominently |
| No GPU-aware autoscaling for SparkApplication | Fixed executor count; can't scale based on queue depth | Integrate KEDA + GPU metrics for dynamic executor scaling |
| GPU executor scheduling requires manual `nodeSelector` or node labels | If GPU nodes are busy, Spark waits indefinitely with no feedback | RHOAI should expose a GPU quota / queue UI similar to SLURM's `squeue` |

### What works well (document for community)
- CUDA 13.0 cluster + `cuda12` JAR works via NVIDIA backward compatibility ✅
- Zero Python code changes needed — SQL plugin intercepts DataFrame ops automatically ✅
- `spark.rapids.sql.metrics.level=DEBUG` gives per-task `gpuTimeNs` in Spark UI ✅
- `spark.ui.prometheus.enabled=true` + DCGM gives full observability stack ✅

---

## 4. Feast Feature Store on RHOAI

### Current approach
`FeatureStore` CR via RHOAI Feast Operator. Offline store: `dask` (reads MinIO Parquet).
Registry: `file` on NFS PVC. Online store: Redis.

### Gaps

| Gap | Impact | Improvement |
|---|---|---|
| `odh-feature-server-rhel9` missing `pyspark` and `psycopg2` | Cannot use `SparkOfflineStore` or `SQLRegistry` — forces less scalable `dask` path | RHOAI should ship a **feature-server image variant** with optional extra packages, or support a `customImage` field in the FeatureStore CR |
| No `SparkOfflineStore` support out of the box | For 100M+ feature rows, `dask` is slow and memory-heavy | Native `SparkOfflineStore` support in RHOAI Feast image is a blocker for production scale |
| `feast materialize` is manual and not scheduled | Features go stale unless someone remembers to run the command | RHOAI should provide a **CronJob-based materialization schedule** configurable in the FeatureStore CR (e.g., `materializationSchedule: "0 * * * *"`) |
| NFS double-mount problem with `smartshop-shared-storage` | Feast registry PVC must be a separate PVC; sharing NFS PVC in the same pod fails | Document this as a known NFS CSI limitation; recommend dedicated PVC per Feast component |
| No built-in feature drift detection | Feature schema changes break downstream models silently | Integrate Great Expectations or Evidently AI for schema validation on `feast apply` |
| Feast Python SDK version pinning | `feast apply` errors when SDK version doesn't match server | RHOAI should lock SDK + server versions together and document the upgrade path |

### Recommended platform addition
> **Feast FeatureStore CR enhancements:**
> - `spec.materializationSchedule` — cron-based auto-materialization
> - `spec.offlineStore.customImage` — bring-your-own image with pyspark/psycopg2
> - `spec.monitoring.enabled` — auto-deploy feature freshness + drift metrics

---

## 5. Kubeflow Training Operator (DDP + FSDP)

### Current approach
`TrainJob` CR dispatches PyTorch DDP (rec model, 1 node × 4 GPU) and FSDP via Slurm
(`ClusterTrainingRuntime` with `slinky` plugin, 2 nodes × 4 GPU).

### Gaps

| Gap | Impact | Improvement |
|---|---|---|
| `ClusterTrainingRuntime` for Slurm is brand new (weeks old) | Zero community documentation; debugging dispatch failures is opaque | RHOAI should publish a **Slurm integration guide** with example `ClusterTrainingRuntime` + `slinky` configs |
| NCCL over OVN-K SDN needs `NCCL_IB_DISABLE=1` or SR-IOV | Full NVLink bandwidth (600 GB/s bidirectional) not achievable over default OVN network | Document SR-IOV / InfiniBand configuration for multi-node training; RHOAI should offer a `highPerformanceNetworking: true` flag |
| No native training job queue/priority | Multiple TrainJobs compete for GPUs with no fairness policy | Integrate **Kueue** (already in Kubernetes ecosystem) for workload queuing with GPU quotas |
| FSDP checkpoint format not standardized | `torch.save` vs `torch.distributed.checkpoint` incompatibility | RHOAI should recommend and document checkpoint format + provide a `CheckpointCallback` in the trainer template |
| No built-in hyperparameter optimization | Manual grid search in YAML; no Optuna/Ray Tune integration | RHOAI Pipelines should support HPO as a first-class job type |
| MLflow auto-logging not wired by default | Developers must manually call `mlflow.log_metric()` | RHOAI trainer base images should include `mlflow.pytorch.autolog()` in their entrypoint wrapper |

### What works well
- `TrainJob` CR is clean and declarative ✅
- Slurm dispatch via `slinky` works (though underdocumented) ✅
- RHOAI-hosted MLflow is accessible from within training pods via `MLFLOW_TRACKING_URI` ✅
- `/dev/shm` emptyDir volume for NCCL shared memory is well-documented ✅

---

## 6. KServe Model Serving

### Gaps (to be validated in Phase 6)

| Gap | Likely impact | Improvement |
|---|---|---|
| Model registry integration with KServe is manual | Must copy S3 URI from training output → InferenceService YAML by hand | RHOAI Model Registry → KServe deploy button should auto-generate InferenceService |
| No canary/shadow deployment for model updates | Full cutover only; risky for production | KServe `Canary` rollout + RHOAI dashboard support |
| Feast feature retrieval latency not tracked per request | Cannot measure end-to-end P99 for recommendation | Add OTEL span for Feast online store call inside serving code |
| GPU memory fragmentation across multiple InferenceServices | 3 models (rec + LLM + RAG) sharing GPU nodes may OOM | RHOAI should provide a **multi-model serving** runtime (Triton-based) that shares GPU memory efficiently |
| No rate limiting on KServe endpoints | Summit demo traffic spike could crash pods | Add `HorizontalPodAutoscaler` + KServe `minReplicas/maxReplicas` tuning |

---

## 7. Observability

### Current approach
DCGM → Prometheus, redis_exporter, Spark PrometheusServlet, MLflow, custom collection
script, Grafana with 3 dashboards, analysis notebook.

### Gaps

| Gap | Impact | Improvement |
|---|---|---|
| No OTEL distributed tracing | Cannot trace a single request from Gradio → KServe → Feast → Redis | Deploy **OpenTelemetry Collector + Tempo** in RHOAI; instrument Python services with `opentelemetry-sdk` |
| User-workload monitoring disabled by default | ServiceMonitors in user namespaces don't work without cluster-admin enabling it | RHOAI should enable user-workload monitoring automatically for Data Science projects |
| Grafana not included in RHOAI by default | Must self-deploy; no persistent storage | Bundle **Grafana Operator** in RHOAI and pre-wire it to the OCP Thanos instance |
| Spark History Server not deployed | Post-mortem stage analysis requires port-forwarding to a live driver | Ship Spark History Server as part of SparkOperator install in RHOAI |
| No alerting on training job failures | Silent failures; you only know if you watch `oc get trainjob` | RHOAI should ship `PrometheusRule` CRs for common ML job failure conditions |
| No cost attribution per job/namespace | Cannot measure GPU-hours per experiment | Integrate **OpenCost** or OCP metering for per-namespace GPU cost tracking |

### What the observability stack proves (for community)
- DCGM → Prometheus is the right GPU metrics path for OpenShift (no extra agent) ✅
- `redis_exporter` + ServiceMonitor gives Feast online store visibility ✅
- Spark PrometheusServlet exposes per-executor JVM + shuffle metrics ✅
- MLflow cross-run comparison gives reproducible GPU speedup numbers ✅

---

## 8. Runtime Images Required (that RHOAI should provide)

The following images were hand-built for this demo. All should be **supported, scanned,
and published in the RHOAI catalog** for production use:

| Image | Base | What it needs | Current status |
|---|---|---|---|
| `spark-jobs` | UBI9 Python 3.11 | PySpark 3.5, PyArrow, s3fs, datasets, sentence-transformers, boto3 | ❌ Hand-built (`Containerfile.spark-jobs`) |
| `spark-jobs-rapids` | apache/spark:3.5.3 | + `rapids-4-spark` JAR, `getGpusResources.sh` | ❌ Hand-built (`Containerfile.spark-rapids`) |
| `rec-trainer` | UBI9 Python 3.11 | PyTorch 2.x, mlflow, feast, boto3, torchmetrics | ❌ Hand-built |
| `llm-trainer` | UBI9 Python 3.11 | PyTorch 2.x + FSDP, peft, trl, mlflow, HuggingFace | ❌ Hand-built |
| Feast feature server + pyspark | `odh-feature-server-rhel9` | + pyspark 3.5, psycopg2-binary | ❌ Not available — forces dask fallback |
| Gradio app server | UBI9 Python 3.11 | gradio, requests, matplotlib, mlflow | ❌ Hand-built |

**Ask for RHOAI product team:** publish these as optional add-on images in the RHOAI
workbench / notebook catalog, versioned alongside the RHOAI release.

---

## 9. Developer Experience Gaps

| Gap | Pain point | Fix |
|---|---|---|
| `envsubst` + `.env` substitution is the only templating | Fragile with zsh plugins, `<placeholder>` values, and multi-line vars | Replace with **Kustomize overlays** or a `make render-manifests` target |
| No local development path for Spark jobs | `spark-submit` locally requires Java + Spark install; no dev container | Provide a **Dev Container** (`devcontainer.json`) with Spark, Python, and `oc` pre-installed |
| Manual secret management | `make setup-secrets` exists but requires many manual steps | Integrate with **External Secrets Operator** or **HashiCorp Vault** |
| No CI/CD for the ML pipeline | Every step is manual `oc apply` | Wire a **Tekton Pipeline** (native to OCP) that runs: build → ETL → materialize → train → serve |
| `Makefile` targets are partially documented | `make help` doesn't describe every target | Add a `make help` target that lists all commands with descriptions |
| No local MinIO for development | Developers must connect to the cluster MinIO; no offline dev | Provide a `docker-compose.yml` with MinIO + Redis + PostgreSQL for local dev |

---

## 10. Security & Compliance Gaps

| Gap | Risk | Fix |
|---|---|---|
| Spark driver runs with `anyuid` SCC effectively | Potential privilege escalation | Define a minimal custom SCC for Spark; upstream this to spark-operator |
| HuggingFace token in `hf-credentials` secret is long-lived | Token exposure = access to private models | Rotate quarterly; consider RHOAI integration with HF fine-grained tokens |
| MinIO root credentials in `smartshop-credentials` | Broad access; no bucket-level IAM | Migrate to MinIO IAM policies per service account |
| No network policy between components | Feast → Redis, Spark → MinIO unrestricted | Add `NetworkPolicy` CRs: Feast only talks to Redis; Spark only to MinIO |
| Model artifacts unsigned | No provenance for `best_model.pt` | Integrate **Sigstore/Cosign** for model artifact signing via RHOAI Model Registry |

---

## 11. What Should Be Prerequisites Before Starting

For a team adopting this stack from scratch, the following should be **pre-provisioned
by the platform team** before data scientists begin:

### Must have (blockers)
- [ ] GPU node pool with DCGM exporter DaemonSet deployed
- [ ] User-workload monitoring enabled (`cluster-monitoring-config` ConfigMap)
- [ ] Persistent storage: NFS CSI or ODF with `ReadWriteMany` StorageClass
- [ ] MinIO (or ODF ObjectBucketClaim) with pre-created buckets and a Data Connection in RHOAI
- [ ] Redis deployed (RHOAI Redis cache component or standalone)
- [ ] Kubeflow Spark Operator installed with GPU-capable `spark` ServiceAccount + SCC
- [ ] Kubeflow Training Operator installed
- [ ] RHOAI Feast Operator with a working `FeatureStore` CR template
- [ ] MLflow deployed and accessible (RHOAI provides this ✅)
- [ ] External container registry (quay.io) with push credentials as a K8s secret

### Should have (significant friction without)
- [ ] Spark History Server backed by S3
- [ ] Grafana with OCP Thanos datasource pre-configured
- [ ] Slurm `slinky` plugin configured + GPU partition available
- [ ] `anyuid` or custom Spark SCC pre-approved for the Data Science namespace
- [ ] HuggingFace token stored in a namespace secret (`hf-credentials`)
- [ ] Network policies drafted and reviewed

### Nice to have (quality of life)
- [ ] Kueue for job queuing with GPU quotas
- [ ] OpenTelemetry Collector + Tempo for distributed tracing
- [ ] External Secrets Operator for secret rotation
- [ ] Dev Container definition for local development
- [ ] Tekton Pipeline for full CI/CD of the ML workflow

---

## Summary — Top 5 Platform Investments for RHOAI

Ranked by impact on adoption of this stack:

| Priority | Investment | Why |
|---|---|---|
| 1 | **Feast feature server image with pyspark** | Unlocks SparkOfflineStore + SQLRegistry; current image forces dask fallback |
| 2 | **Supported Spark 3.5 + RAPIDS runtime images** | Every team rebuilds these from scratch; fragile CUDA compatibility |
| 3 | **User-workload monitoring enabled by default** | Blocks all custom ServiceMonitors; simple one-line fix with high impact |
| 4 | **Feast `materializationSchedule` in CR** | Features go stale without scheduled materialization; critical for production |
| 5 | **Slurm integration guide + ClusterTrainingRuntime template** | Zero community documentation; high barrier to adoption |
