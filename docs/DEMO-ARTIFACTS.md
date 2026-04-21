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

> **Run date:** 2026-04-21 — full dataset, both jobs submitted simultaneously from a clean cluster state.

| Metric | CPU Baseline | RAPIDS GPU | Speedup |
|--------|-------------|------------|---------|
| Total rows processed | 140,772,341 ✅ | 140,772,341 ✅ | same |
| Read time (s) | 1,546.6 | **1,091.4** | **1.42×** |
| User feature aggregation (s) | 3,020.8 | **1,912.7** | **1.58×** |
| Item feature aggregation (s) | 1,366.7 | **786.4** | **1.74×** |
| Interaction join/write (s) | 1,088.2 | **844.3** | **1.29×** |
| **Total elapsed (s)** | **7,029.1** | **4,667.7** | **1.51× overall** |
| **Wall-clock (s)** | **7,057** | **4,755** | **1.48×** |
| Throughput (rows/s) | 20,026 | **30,159** | **+51%** |
| Unique users | 35,049,327 ✅ | 35,049,327 ✅ | same |
| Unique items | 9,790,339 ✅ | 9,790,339 ✅ | same |
| `gpu_accelerated` flag | `False` ✅ | `True` ✅ | |

> **Bottleneck analysis:** Both jobs are I/O-bound (MinIO S3 over NFS).
> RAPIDS advantage is columnar in-memory processing via CUDF — no JVM serialization overhead.
> SM Active Ratio < 2.5%, GPU power 80–100W (vs 400W TDP) throughout — GPU waiting on S3 reads.
> On a compute-bound workload or with local NVMe storage the speedup would be significantly higher.
> Antonin (IBM cluster team) flagged storage as the bottleneck — SSD nodes would improve both jobs.

**RAPIDS job completed:** `2026-04-21T15:25:39Z`  
**CPU job completed:** `2026-04-21T16:03:05Z`

## Key Numbers for Slides (updated)

```
GPU Feature Engineering Speedup:    1.51× overall  (CPU 7,057s → RAPIDS 4,755s)
Best stage speedup:                  1.74× (item feature aggregation)
Throughput improvement:             +51%  (20,026 → 30,159 rows/s)
Rows processed:                     140.8M rows, 49 GB full dataset
Unique users materialized to Redis:  35M
Unique items in feature store:        9.8M
```

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

| Screenshot | File | What it shows | Status |
|-----------|------|--------------|--------|
| RAPIDS job mid-run — GPU memory 75 GB loaded | `Screenshot_2026-04-21_at_8.55.26_PM-4ebf164d.png` | Framebuffer at 75 GB, SM active, power at 80–100W during I/O-bound ETL | ✅ captured |
| RAPIDS job completed — GPU memory released to 0 | `Screenshot_2026-04-21_at_8.58.54_PM-dbf36a38.png` | Memory drops to 0 at 20:55 (job completion), DRAM spike on final flush | ✅ captured |
| NVLink Bandwidth during TrainJob | — | Inter-GPU comms during DDP/FSDP training | 🔲 pending |
| Redis ops/s during Feast materialize | — | Write throughput as features land in online store | 🔲 pending |

**Grafana dashboard:** `https://grafana-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com`  
**Dashboard name:** SmartShop GPU Performance (RAPIDS vs CPU)

**Key insight from DCGM data (captured in screenshots):**
- SM Active Ratio peak: ~2.5% — workload is **I/O-bound**, not compute-bound
- Framebuffer sustained at ~75 GB — full dataset held in GPU VRAM throughout
- Power: 80–100W (vs 400W TDP) — GPU waiting on MinIO S3 reads between stages
- NVLink near-zero — expected, single-node ETL (no inter-GPU comms needed)
- Large SM spike at ~20:20 IST = user feature aggregation stage (1,912s — heaviest compute stage)

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
