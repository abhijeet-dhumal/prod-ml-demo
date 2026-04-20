# Observability & Metrics — SmartShop AI on Red Hat OpenShift AI

> Community-facing reference for what we measured, how we measured it, and what it means.
> Intended for Red Hat Summit 2026 and subsequent blog / upstream sharing.

---

## Why this matters

Running **NVIDIA RAPIDS on Spark** inside **Kubeflow's Spark Operator**, combined with
**Slurm-dispatched FSDP training** and **Feast on RHOAI** is genuinely novel territory.
Very few people have done this end-to-end on OpenShift. This document captures every
metric layer so findings can be reproduced, refined, and shared with the wider community.

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Red Hat OpenShift AI (RHOAI) on IBM Cloud / on-prem            │
│                                                                 │
│  ┌───────────────┐   ┌──────────────┐   ┌──────────────────┐   │
│  │  Spark RAPIDS │   │ Feast (RHOAI │   │ Kubeflow Trainer  │   │
│  │  SparkApp     │──▶│  Operator)   │──▶│ DDP / FSDP+Slurm │   │
│  │  A100 × 4     │   │  dask + Redis│   │ A100 × 4–8       │   │
│  └───────┬───────┘   └──────────────┘   └────────┬─────────┘   │
│          │                                        │             │
│  ┌───────▼───────────────────────────────────────▼─────────┐   │
│  │              Observability Stack                         │   │
│  │  MLflow (RHOAI) · Prometheus/DCGM · Spark UI/REST API   │   │
│  │  MinIO metrics bundle · OCP monitoring dashboards        │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Metric layers

### Layer 1 — Spark driver logs (`[METRIC]` lines)

Every Spark job emits structured lines greppable from pod logs:

```
[METRIC] gpu_accelerated=True
[METRIC] total_reviews=999000
[METRIC] read_elapsed_s=12.4 s
[METRIC] user_features_elapsed_s=47.1 s
[METRIC] item_features_elapsed_s=38.6 s
[METRIC] interactions_elapsed_s=21.3 s
[METRIC] total_elapsed_s=128.7 s
[METRIC] throughput_rows_per_s=7763 rows/s
```

**Capture:**
```bash
DRIVER=$(oc get pod -n smartshop -l spark-role=driver,spark-app-name=<app> -o name | head -1)
oc logs -n smartshop $DRIVER | grep '\[METRIC\]'
```

---

### Layer 2 — MLflow (RHOAI-hosted)

Logged automatically by `spark/utils/mlflow_metrics.py` when `MLFLOW_TRACKING_URI` is set.

| MLflow artifact | What's logged |
|---|---|
| **Params** | Executor count, memory, RAPIDS conf, GPU amount |
| **Metrics** | All `[METRIC]` values + per-stage timings |
| **Tags** | `rapids_active`, `gpu_count`, `cluster_app_id`, `spark_version` |
| **Artifact** | `/metrics/run_metrics.json` — full bundle for download |

MLflow experiment: `smartshop-feature-engineering`

Access: `https://mlflow-redhat-ods-applications.apps.<cluster>/mlflow`

**Cross-run comparison** (CPU vs RAPIDS) is computed automatically:
```
gpu_vs_cpu_speedup = cpu_total_elapsed_s / rapids_total_elapsed_s
```

---

### Layer 3 — Spark REST API (stage + SQL + executor metrics)

Scraped by `scripts/collect-run-metrics.sh` while the driver pod is alive or via Spark History Server.

**Key endpoints:**
```
GET /api/v1/applications/<app-id>/stages
GET /api/v1/applications/<app-id>/sql      ← GPU operator count from physical plan
GET /api/v1/applications/<app-id>/executors
GET /api/v1/applications/<app-id>/environment
```

**What to look for in the SQL plan** (RAPIDS):
- `GpuHashAggregateExec` — `groupBy + agg` ran on GPU
- `GpuShuffleExchangeExec` — shuffle ran on GPU
- `GpuSortMergeJoinExec` — join ran on GPU
- `!Exec<...>` — these lines appear when an operation fell back to CPU

Enable with: `spark.rapids.sql.explain=ALL` and `spark.rapids.sql.metrics.level=DEBUG`
(both set in `spark-application-rapids.yaml`)

**Port-forward Spark UI:**
```bash
oc port-forward -n smartshop $DRIVER 4040:4040
# Open http://localhost:4040/SQL/  → click any query → see GPU operators in DAG
```

---

### Layer 4 — DCGM GPU metrics (Prometheus)

DCGM exporter runs as a DaemonSet on GPU nodes and is scraped by OCP monitoring.

| Metric | Description | Target range for RAPIDS |
|---|---|---|
| `DCGM_FI_DEV_GPU_UTIL` | SM utilization % | > 60% during agg/join stages |
| `DCGM_FI_DEV_FB_USED` | Framebuffer memory (MB) | < 60GB per A100-80GB |
| `DCGM_FI_PROF_DRAM_ACTIVE` | Memory bus active ratio | > 0.3 during shuffle |
| `DCGM_FI_PROF_SM_ACTIVE` | Streaming multiprocessor active | > 0.5 during compute |
| `DCGM_FI_DEV_NVLINK_BANDWIDTH_TOTAL` | NVLink bandwidth (cross-GPU) | Elevated during GPU shuffle |
| `DCGM_FI_DEV_POWER_USAGE` | Power draw (W) | 200–400W during RAPIDS |

**Query from OCP Observe → Metrics, or:**
```bash
# Average GPU utilization across all GPUs in the namespace window
oc exec -n openshift-monitoring prometheus-k8s-0 -- \
  curl -sg 'http://localhost:9090/api/v1/query?query=avg(DCGM_FI_DEV_GPU_UTIL)'
```

**For the Summit recording:** open OCP Observe → Metrics tab and paste:
```promql
DCGM_FI_DEV_GPU_UTIL{Hostname=~".*gpu.*"}
```
Screenshot the spike during the RAPIDS job — this is the visual proof.

---

### Layer 5 — Spark Prometheus endpoint (PrometheusServlet)

Enabled by `spark-metrics-config` ConfigMap (mounted into all SparkApplication pods).

- Endpoint: `http://<driver-pod-ip>:4040/metrics/prometheus`
- Captures: executor heap, GC pauses, shuffle read/write bytes, BlockManager cache size

OCP monitoring discovers these via pod annotations if a `ServiceMonitor` is created.
The existing `spark-metrics-configmap.yaml` includes the scrape config template.

---

### Layer 6 — Slurm + FSDP training metrics

#### NCCL debug bandwidth

`NCCL_DEBUG=INFO` is set in `trainjobs.yaml`. During AllReduce collective operations,
NCCL logs lines like:
```
NCCL INFO AllReduce: ... busBw 187.3 GB/s
```

Extract them:
```bash
for pod in $(oc get pod -n smartshop -l training.kubeflow.org/job-name=smartshop-llm-finetune -o name); do
  echo "=== $pod ==="; oc logs -n smartshop $pod | grep -i "busBw\|Avg bus"
done
```

Expected: **> 150 GB/s** for A100s on NVLink (300 GB/s peak bidirectional).
This proves the Slurm-dispatched pods use the GPU interconnect correctly.

#### Training throughput

The `training/llm/finetune.py` trainer should log tokens/sec. Look for:
```
[METRIC] tokens_per_second=<N>
[METRIC] samples_per_second=<N>
[METRIC] loss=<N>
```

Target: **> 2000 tokens/sec** for Mistral-7B with FSDP on 8× A100.

#### MLflow loss curves

The training scripts log to the same `MLFLOW_TRACKING_URI`. Compare:
- Experiment: `smartshop-llm-finetune`
- Key metrics: `train/loss`, `eval/loss` per step, `samples_per_second`

---

### Layer 7 — Feast materialization

After `feast materialize-incremental`, look for timing in the Feast pod logs:
```bash
oc logs -n smartshop -l app=feast-smartshop-feast | grep -i "materialize\|written\|rows"
```

**What to capture:**
- Time to materialize user/item features into Redis
- Number of rows pushed to the online store
- Any S3 read errors (dask offline store reads from MinIO)

---

## Post-run collection (automated)

Run after any Spark job:
```bash
# Apply the metrics ConfigMap with the actual script embedded
oc create configmap smartshop-collect-script \
  --from-file=collect-run-metrics.sh=scripts/collect-run-metrics.sh \
  -n smartshop --dry-run=client -o yaml | oc apply -f -

# Then submit the collection Job
source .env
RUN_TYPE=rapids APP_NAME=smartshop-feature-engineering-rapids \
  envsubst < infrastructure/openshift/metrics-collection-job.yaml | oc apply -f -

# Tail collection output
oc logs -n smartshop -f job/smartshop-collect-metrics-rapids
```

Output lands at: `s3://smartshop-models/metrics/<timestamp>-rapids_metrics_bundle.json`

---

## GPU vs CPU A/B comparison — runbook

This is the primary proof for the Summit demo.

```bash
# 1. Run CPU baseline (write to cpu-baseline/ prefix, no RAPIDS)
source .env
envsubst < infrastructure/openshift/spark-application-cpu-baseline.yaml | oc apply -f -
oc wait sparkapplication smartshop-feature-engineering-cpu-baseline \
  -n smartshop --for=jsonpath='{.status.applicationState.state}'=COMPLETED --timeout=30m

# 2. Capture CPU metrics
RUN_TYPE=cpu APP_NAME=smartshop-feature-engineering-cpu-baseline \
  bash scripts/collect-run-metrics.sh

# 3. Run RAPIDS job (write to default prefix)
envsubst < infrastructure/openshift/spark-application-rapids.yaml | oc apply -f -
oc wait sparkapplication smartshop-feature-engineering-rapids \
  -n smartshop --for=jsonpath='{.status.applicationState.state}'=COMPLETED --timeout=20m

# 4. Capture GPU metrics (includes MLflow speedup calculation)
RUN_TYPE=rapids APP_NAME=smartshop-feature-engineering-rapids \
  bash scripts/collect-run-metrics.sh

# 5. Print speedup
python3 -c "
import json, glob
bundles = sorted(glob.glob('/tmp/metrics_bundle_*.json'))
if len(bundles) >= 2:
    rapids = json.load(open(bundles[-1]))['mlflow'].get('gpu_vs_cpu_speedup')
    print(f'GPU speedup: {rapids}×')
"
```

---

## What's novel about this setup

| Topic | Why it's hard to find documented |
|---|---|
| RAPIDS on OpenShift | OCP SCCs restrict privileged GPU init; SparkApplication GPU resource block syntax differs from plain K8s docs |
| RAPIDS + Feast in one pipeline | No public example of RAPIDS doing feature ETL → Feast dask offline store → Redis online store |
| Kubeflow Trainer → Slurm dispatch | `ClusterTrainingRuntime` with `slinky` plugin is weeks old; zero community write-ups |
| FSDP on RHOAI multi-node | NCCL over OVN-K SDN requires `NCCL_IB_DISABLE=1` + host network or SR-IOV for full bandwidth |
| Feast on RHOAI Operator | `FeatureStore` CRD is RHOAI-specific; upstream Feast docs don't cover it |

---

---

## Layer 8 — Redis & Data Access (redis_exporter)

RedisInsight is deployed and accessible at `redisinsight-smartshop.apps.<cluster>` for GUI
inspection. For time-series Prometheus metrics, deploy `redis_exporter`:

```bash
# 1. Enable user-workload monitoring (once, cluster-admin)
oc apply -f infrastructure/openshift/user-workload-monitoring.yaml

# 2. Deploy redis_exporter
source .env
envsubst < infrastructure/openshift/redis-exporter.yaml | oc apply -f -

# 3. Verify scraping
oc port-forward -n smartshop svc/redis-exporter 9121:9121
curl http://localhost:9121/metrics | grep redis_commands
```

**Key Feast-specific metrics to watch:**

| Prometheus metric | Feast meaning |
|---|---|
| `redis_commands_processed_total` | Feature retrieval throughput (HGET/GET per request) |
| `redis_keyspace_hits_total` | Cache hits — materialized features being served |
| `redis_keyspace_misses_total` | Cache misses — features not yet materialized |
| `redis_db_keys` | Total materialized feature keys in online store |
| `redis_memory_used_bytes` | Memory footprint of online feature store |

**PromQL for hit ratio:**
```promql
rate(redis_keyspace_hits_total{namespace="smartshop"}[5m]) /
  (rate(redis_keyspace_hits_total{namespace="smartshop"}[5m]) +
   rate(redis_keyspace_misses_total{namespace="smartshop"}[5m]))
```
Target: **> 95%** during inference (warm feature store).

---

## Layer 9 — Grafana Dashboards

No Grafana Operator on this cluster, so a standalone Grafana deployment is provided.
Three pre-wired dashboards load on startup:

```bash
source .env
envsubst < infrastructure/openshift/grafana.yaml | oc apply -f -
oc get route grafana -n smartshop
# → https://grafana-smartshop.apps.<cluster>  (admin / smartshop2026)
```

| Dashboard | UID | What it shows |
|---|---|---|
| GPU Performance (RAPIDS vs CPU) | `smartshop-gpu` | DCGM util, FB mem, SM active, NVLink BW, power |
| Redis Feature Store | `smartshop-redis` | ops/sec, hit ratio, memory, connected clients |
| Spark Executor Metrics | `smartshop-spark` | JVM heap, GC, shuffle bytes, BlockManager cache |

Grafana reads from **OCP Thanos** (port 9091, cluster-wide metrics) and
**user-workload Prometheus** (port 9092, redis_exporter + Spark PrometheusServlet).
Auth via the `grafana-sa` service account token (auto-mounted).

---

## Layer 10 — Analysis Notebook (publication-quality charts)

`notebooks/metrics_analysis.ipynb` — pulls everything and generates charts ready for
blog posts, summit slides, and upstream community sharing:

```bash
# Run from repo root (load .env automatically)
cd /Users/abdhumal/Dev/RedHatDev/prod-ml-demo
jupyter notebook notebooks/metrics_analysis.ipynb

# Or in an RHOAI JupyterLab instance — all env vars will be set
```

**Charts generated:**
1. `mlflow_gpu_vs_cpu.png` — wall-clock speedup + throughput comparison bar chart
2. `dcgm_combined.png` — 6-panel DCGM GPU metrics over job window
3. `redis_metrics.png` — ops/sec, hit ratio, memory during Feast materialization + inference
4. `rapids_coverage.png` — GPU operator coverage pie + CPU vs RAPIDS stage comparison
5. `mlflow_stage_breakdown.png` — per-stage timing (user/item features, interactions)

All charts are bundled into `summit_charts.zip` and optionally uploaded to MinIO.

---

## OpenTelemetry — Honest Assessment

| Scenario | Recommended approach |
|---|---|
| Redis access latency p99 | `redis_exporter` covers this via `LATENCY HISTORY`; no OTEL needed |
| Data access timing (MinIO reads) | Boto3 timing wrappers in `download.py` + log to MLflow |
| Spark stage latency | Spark REST API already captures this |
| Full distributed trace (Gradio→KServe→Feast→Redis) | Requires OTEL Collector + Tempo — **next step post-Summit** |

OTEL full tracing is the right long-term investment. For the Summit scope,
`redis_exporter` + Prometheus covers all the Redis proof needed, and the notebook
stitches it into publication-quality visualizations.

---

## Files

| File | Purpose |
|---|---|
| `spark/utils/mlflow_metrics.py` | MLflow logging helper (params, metrics, artifacts) |
| `spark/feature_engineering.py` | Main ETL — emits `[METRIC]` + logs to MLflow |
| `infrastructure/openshift/spark-metrics-configmap.yaml` | PrometheusServlet config |
| `infrastructure/openshift/spark-application-rapids.yaml` | RAPIDS SparkApp + observability conf |
| `infrastructure/openshift/spark-application-cpu-baseline.yaml` | CPU baseline for A/B |
| `infrastructure/openshift/metrics-collection-job.yaml` | On-cluster scraper Job |
| `infrastructure/openshift/user-workload-monitoring.yaml` | Enable OCP user-workload Prometheus |
| `infrastructure/openshift/redis-exporter.yaml` | redis_exporter + ServiceMonitor |
| `infrastructure/openshift/grafana.yaml` | Grafana + 3 pre-wired dashboards |
| `scripts/collect-run-metrics.sh` | Post-run bundle (Spark REST + DCGM + MLflow + NCCL) |
| `notebooks/metrics_analysis.ipynb` | Analysis notebook — pull everything, generate charts |
