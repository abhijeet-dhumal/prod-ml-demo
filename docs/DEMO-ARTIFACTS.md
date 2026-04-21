# SmartShop AI — Summit 2026 Demo Artifacts

Tracks every metric, chart, and screenshot needed for the Red Hat Summit 2026 demo.
Run `bash scripts/apply-all.sh notebook` after all Spark/training jobs complete to
auto-generate and upload most of these to `s3://smartshop-models/notebooks/`.

---

## Cluster Hardware

| Resource | Detail |
|----------|--------|
| GPU nodes | 2 × `oai-kft-ibm-jcsbk-gpu-*.msf4t` |
| GPU model | NVIDIA A100-SXM4-80GB (8 per node = 16 total) |
| GPU Operator | `nvcr.io/nvidia/gpu-operator@sha256:634471cf…` |
| DCGM Exporter | `nvcr.io/nvidia/k8s/dcgm-exporter@sha256:7c0ac443…` |
| Spark Operator | `registry.redhat.io/rhoai/odh-spark-operator-rhel9@sha256:82fb7c45…` |
| MLflow | RHOAI-managed, `kubernetes-auth` mode — workspace `smartshop` |

---

## Performance Metrics

### Phase 3 — Spark ETL / Feature Engineering

Both jobs processed the full Amazon review dataset (Electronics + Books + Home & Kitchen).

| Metric | CPU Baseline | RAPIDS GPU | Speedup |
|--------|-------------|------------|---------|
| Total rows processed | 140,772,341 ✅ | 140,772,341 ✅ | same |
| Read time (s) | 193.72 ✅ | **108.46** ✅ | **1.79×** |
| User feature aggregation (s) | 396.36 ✅ | **313.72** ✅ | **1.26×** |
| Item feature aggregation (s) | 41.18 ✅ | **22.06** ✅ | **1.87×** |
| Interaction join/write (s) | 55.15 ✅ | 83.56 ✅ | (shuffle-bound, expected) |
| **Total wall-clock (s)** | **719.23** ✅ | **536.82** ✅ | **1.34× overall** |
| Throughput (rows/s) | 195,727 ✅ | **262,231** ✅ | **+34%** |
| Unique users | 35,049,327 ✅ | 35,049,327 ✅ | |
| Unique items | 9,790,339 ✅ | 9,790,339 ✅ | |
| `gpu_accelerated` flag | `False` ✅ | `True` ✅ | |

**RAPIDS job completed:** `2026-04-21T09:10:08Z`  
**CPU job completed:** `2026-04-21T08:00:58Z`

```bash
# Reproduce metrics from driver logs:
oc logs smartshop-feature-engineering-rapids-driver -n smartshop | grep '\[METRIC\]'
oc logs smartshop-feature-engineering-cpu-baseline-driver -n smartshop | grep '\[METRIC\]'
```

#### RAPIDS image details

| Item | Value |
|------|-------|
| Image | `quay.io/abdhumal/smartshop-spark-jobs-rapids:latest` |
| Push digest | `sha256:4213f31c5ccef381422872ee409aebc77c9f16e75a398a8a514bb4e869d9ee19` |
| RAPIDS JAR | `rapids-4-spark_2.12-26.02.2-cuda13.jar` (Maven Central) |
| CUDA variant | `cuda13` — cluster runs CUDA 13.0 / driver 580.x |
| Base image | `apache/spark:3.5.3` |
| Built via | `BuildConfig/spark-jobs-rapids` in `smartshop` ns |

No code changes to `feature_engineering.py` — `spark.plugins=com.nvidia.spark.SQLPlugin`
intercepts all DataFrame ops at runtime.

---

### Phase 3 — Text Preprocessing (LLM instruction-tuning data)

> Job was RUNNING at snapshot time — final metrics pending completion.

| Metric | Value | Status |
|--------|-------|--------|
| Total input reviews | 140,772,341 | ✅ (from driver logs) |
| Clean reviews after dedup | 104,608,088 | ✅ |
| Read time (s) | 7.8 | ✅ |
| Train examples | _pending_ | job still running |
| Val examples | _pending_ | job still running |
| Total elapsed (s) | _pending_ | job still running |

```bash
oc logs smartshop-text-preprocessing-driver -n smartshop | grep '\[METRIC\]'
```

---

### Phase 5 — Recommendation Training (DDP, 4× A100)

| Metric | Value | Where |
|--------|-------|-------|
| Best val loss | _pending_ | MLflow `best_val_loss` |
| Val accuracy | _pending_ | MLflow `val_accuracy` |
| Epochs | 10 | `.env: REC_TRAIN_EPOCHS` |
| GPUs used | 4 × A100-80GB | `.env: REC_GPUS_PER_NODE=4` |
| Training time (s) | _pending_ | MLflow `total_training_time_s` |
| Throughput (samples/s) | _pending_ | MLflow `throughput_samples_per_s` |

---

### Phase 5 — LLM Fine-Tuning (FSDP, 2 nodes × 4 A100 = 8 GPUs)

| Metric | Value | Where |
|--------|-------|-------|
| Base model | `mistralai/Mistral-7B-Instruct-v0.3` | `.env` |
| Nodes × GPUs | 2 × 4 A100 = 8 total | `.env` |
| NCCL bus bandwidth | _pending_ | pod logs `busBw` |
| Train loss (final epoch) | _pending_ | MLflow `train_loss` |
| Adapter size (MB) | _pending_ | `s3://smartshop-models/llm-adapter/` |

---

## GitHub Manifest Links

All on branch [`refine-cluster-infra-setup`](https://github.com/abhijeet-dhumal/prod-ml-demo/tree/refine-cluster-infra-setup).

| File | Purpose | Link |
|------|---------|------|
| `spark-application-rapids.yaml` | RAPIDS SparkApplication | [link](https://github.com/abhijeet-dhumal/prod-ml-demo/blob/refine-cluster-infra-setup/infrastructure/openshift/spark-application-rapids.yaml) |
| `spark-application-cpu-baseline.yaml` | CPU baseline for A/B | [link](https://github.com/abhijeet-dhumal/prod-ml-demo/blob/refine-cluster-infra-setup/infrastructure/openshift/spark-application-cpu-baseline.yaml) |
| `spark-application-text-preprocessing.yaml` | LLM data prep | [link](https://github.com/abhijeet-dhumal/prod-ml-demo/blob/refine-cluster-infra-setup/infrastructure/openshift/spark-application-text-preprocessing.yaml) |
| `build/Containerfile.spark-rapids` | RAPIDS container image | [link](https://github.com/abhijeet-dhumal/prod-ml-demo/blob/refine-cluster-infra-setup/build/Containerfile.spark-rapids) |
| `spark/feature_engineering.py` | ETL script (CPU + GPU) | [link](https://github.com/abhijeet-dhumal/prod-ml-demo/blob/refine-cluster-infra-setup/spark/feature_engineering.py) |
| `spark/utils/mlflow_metrics.py` | MLflow logging helper | [link](https://github.com/abhijeet-dhumal/prod-ml-demo/blob/refine-cluster-infra-setup/spark/utils/mlflow_metrics.py) |
| `spark-metrics-configmap.yaml` | PrometheusServlet + PodMonitor | [link](https://github.com/abhijeet-dhumal/prod-ml-demo/blob/refine-cluster-infra-setup/infrastructure/openshift/spark-metrics-configmap.yaml) |
| `grafana.yaml` | Grafana + 3 dashboards | [link](https://github.com/abhijeet-dhumal/prod-ml-demo/blob/refine-cluster-infra-setup/infrastructure/openshift/grafana.yaml) |
| `trainjobs.yaml` | Kubeflow TrainJob (DDP + FSDP) | [link](https://github.com/abhijeet-dhumal/prod-ml-demo/blob/refine-cluster-infra-setup/infrastructure/openshift/trainjobs.yaml) |
| `scripts/apply-all.sh` | One-shot deploy script | [link](https://github.com/abhijeet-dhumal/prod-ml-demo/blob/refine-cluster-infra-setup/scripts/apply-all.sh) |

---

## Screenshots to Capture

### Grafana Dashboards

| Screenshot | URL | Status |
|-----------|-----|--------|
| GPU Utilization % spike (RAPIDS window ~14:00–14:55) | [grafana-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com](https://grafana-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com) → GPU panel | ✅ captured |
| GPU Framebuffer Memory Used (A100 80GB) | Same dashboard | ✅ captured |
| SM Active Ratio / DRAM Active Ratio | Same dashboard | ✅ captured |
| NVLink Bandwidth during training | GPU dashboard, during TrainJob | 🔲 pending |
| Redis ops/s during Feast materialize | Redis dashboard | 🔲 pending |

```bash
# Admin password:
oc get secret grafana-admin-credentials -n smartshop \
  -o jsonpath='{.data.GF_SECURITY_ADMIN_PASSWORD}' | base64 -d
```

### MLflow UI

| Screenshot | What to show | Status |
|-----------|-------------|--------|
| Experiment list — `smartshop-feature-engineering` | 2 runs: cpu-baseline + rapids | 🔲 pending |
| Side-by-side run comparison | `total_elapsed_s`, `throughput_rows_per_s`, speedup | 🔲 pending |
| Rec training loss curves | `train_loss` vs `val_loss` per epoch | 🔲 pending |

```bash
# MLflow browser URL (SSO via RHOAI dashboard):
open https://rh-ai.apps.oai-kft-ibm.ibm.rh-ods.com/mlflow/#/?workspace=smartshop

# Internal URI used by in-cluster pods:
# https://mlflow.redhat-ods-applications.svc.cluster.local:8443/mlflow
```

---

## Notebook-Generated PNGs (auto-uploaded to MinIO)

| File | Content | Status |
|------|---------|--------|
| `mlflow_gpu_vs_cpu.png` | 3-panel: elapsed time, throughput, 1.34× speedup bar chart | 🔲 |
| `dcgm_combined.png` | 6-panel DCGM grid (util, mem, power, SM active, DRAM active, NVLink) | 🔲 |
| `redis_ops.png` | Redis ops/s + hit ratio during Feast materialize + inference | 🔲 |
| `training_loss_curves.png` | Rec model loss per epoch (train + val) | 🔲 |

```bash
# Generate all PNGs:
bash scripts/apply-all.sh notebook

# Download from MinIO after job completes:
source .env
aws s3 cp s3://smartshop-models/notebooks/ ./notebooks/output/ \
  --recursive --endpoint-url $MINIO_ENDPOINT_EXTERNAL
```

---

## Key Numbers for Slides

```
GPU Feature Engineering Speedup:    1.34× overall  (CPU 719s → RAPIDS 537s)
Best stage speedup:                  1.87× (item feature aggregation)
Throughput at scale:                 140.8M rows processed (262,231 rows/s w/ RAPIDS)
Unique users materialized to Redis:  35M
Unique items in feature store:       9.8M
Clean LLM training examples:        104.6M (after dedup from 140.8M)
Rec model val accuracy:             ___ (pending TrainJob)
LLM fine-tune NCCL bandwidth:       ___ GB/s (pending LLM TrainJob)
```

---

## Artifact Collection Commands

```bash
# After Phase 3 (Spark ETL) — already completed:
source .env
oc create configmap smartshop-collect-script \
  --from-file=collect-run-metrics.sh=scripts/collect-run-metrics.sh \
  -n smartshop --dry-run=client -o yaml | oc apply -f -

RUN_TYPE=rapids APP_NAME=smartshop-feature-engineering-rapids \
  bash scripts/collect-run-metrics.sh
RUN_TYPE=cpu APP_NAME=smartshop-feature-engineering-cpu-baseline \
  bash scripts/collect-run-metrics.sh

# After Phase 4 (Feast materialization):
RUN_TYPE=feast bash scripts/collect-run-metrics.sh

# After Phase 5 (training):
RUN_TYPE=slurm TRAINJOB_NAME=smartshop-llm-finetune \
  bash scripts/collect-run-metrics.sh

# Full notebook run (generates all PNGs):
bash scripts/apply-all.sh notebook
oc logs -n smartshop job/smartshop-notebook-runner -f
aws s3 cp s3://smartshop-models/notebooks/ ./notebooks/output/ \
  --recursive --endpoint-url $MINIO_ENDPOINT_EXTERNAL
```

---

## Manifest Inventory

All working manifests in `infrastructure/openshift/*.yaml`.
Dot-prefixed files (`.*.yaml`) are temporary debugging artifacts — not tracked by git.
Deploy everything with `bash scripts/apply-all.sh all`.

| Manifest | Phase | Applied by | Status |
|----------|-------|-----------|--------|
| `user-workload-monitoring.yaml` | 1 | `apply-all.sh infra` | ✅ applied |
| `spark-metrics-configmap.yaml` | 1 | `apply-all.sh observability` | ✅ applied |
| `redis-exporter.yaml` | 1 | `apply-all.sh observability` | ✅ applied |
| `grafana.yaml` | 1 | `apply-all.sh observability` | ✅ applied |
| `metrics-collection-job.yaml` | 1 | `apply-all.sh observability` + per-run | ✅ applied |
| `imagestreams.yaml` | 2 | `apply-all.sh images` | ✅ applied |
| `buildconfigs.yaml` | 2 | `apply-all.sh images` | ✅ applied |
| `data-download-job.yaml` | 2 | `apply-all.sh data` | ✅ applied |
| `upload-spark-jars-job.yaml` | 2 | `apply-all.sh data` | ✅ applied |
| `spark-application-rapids.yaml` | 3 | `apply-all.sh spark` | ✅ COMPLETED |
| `spark-application-cpu-baseline.yaml` | 3 | `apply-all.sh spark` | ✅ COMPLETED |
| `spark-application-text-preprocessing.yaml` | 3 | `apply-all.sh spark` | 🔄 RUNNING |
| `spark-application-embedding.yaml` | 3 | `apply-all.sh spark` | 🔲 pending |
| `trainjobs.yaml` | 5 | `apply-all.sh training` | 🔲 pending |
| `inferenceservices.yaml` | 6 | `apply-all.sh serving` | 🔲 pending |
| `notebook-runner-job.yaml` | 7 | `apply-all.sh notebook` | 🔲 pending |
