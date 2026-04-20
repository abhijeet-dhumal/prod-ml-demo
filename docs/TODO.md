# SmartShop AI — Demo Execution TODO

**Cluster:** `apps.oai-kft-ibm.ibm.rh-ods.com`
**Namespace:** `smartshop`
**Registry:** `quay.io/abdhumal`
**Branch:** `refine-cluster-infra-setup`

> Legend: ✅ Done · 🔄 In progress · ⏳ Blocked (dependency) · 🔲 Not started · ❌ Failed/needs fix

---

## Already completed

| Status | What |
|---|---|
| ✅ | Namespace, RBAC, secrets, MinIO buckets, Redis, PostgreSQL, Milvus created |
| ✅ | BuildConfigs updated to push to `quay.io/abdhumal` |
| ✅ | All images built and pushed: `spark-jobs`, `spark-jobs-rapids`, `rec-trainer`, `llm-trainer` |
| ✅ | Feast operator deployed — `smartshop-feast` pod Ready |
| ✅ | `spark-application.yaml`, `spark-application-rapids.yaml`, `spark-application-cpu-baseline.yaml` |
| ✅ | `data-download-job.yaml` — on-cluster HF → MinIO streaming Job |
| ✅ | Full observability stack: MLflow helper, PrometheusServlet, DCGM, redis_exporter, Grafana, collect script |
| ✅ | `notebooks/metrics_analysis.ipynb` — publication charts |
| ✅ | Gradio UI redesign — Platform Metrics tab + Architecture tab |
| ✅ | `docs/OBSERVABILITY.md`, `SETUP.md` up to date |
| ✅ | Git history cleaned (single-line commits, `Signed-off-by`) |

---

## Phase 1 — Observability

> **Goal:** metrics capture everything from the first Spark job onwards.
> **Blocker for:** Phase 3 metrics collection (DCGM data won't exist if deployed late).

| Status | # | Step | Command | Done when |
|---|---|---|---|---|
| 🔲 | 1.1 | Enable user-workload monitoring (cluster-admin, once) | `oc apply -f infrastructure/openshift/user-workload-monitoring.yaml` | `oc get prometheus -n openshift-user-workload-monitoring` shows a pod |
| 🔲 | 1.2 | Apply Spark metrics ConfigMap | `source .env && envsubst < infrastructure/openshift/spark-metrics-configmap.yaml \| oc apply -f -` | `oc get cm spark-metrics-config -n smartshop` |
| 🔲 | 1.3 | Deploy `redis_exporter` + ServiceMonitor | `source .env && envsubst < infrastructure/openshift/redis-exporter.yaml \| oc apply -f -` | `oc get pod -n smartshop -l app=redis-exporter` Running |
| 🔲 | 1.4 | Deploy Grafana | `source .env && envsubst < infrastructure/openshift/grafana.yaml \| oc apply -f -` | `oc get route grafana -n smartshop` has a host |
| 🔲 | 1.5 | Get Prometheus token → `.env` | `oc sa get-token grafana-sa -n smartshop` → add to `PROMETHEUS_TOKEN=` in `.env` | Grafana dashboards load data |
| 🔲 | 1.6 | Verify Grafana loads GPU metrics | Open `https://grafana-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com` → GPU dashboard | DCGM panels show data |

**Verification:**
```bash
# redis_exporter scraping Redis
oc port-forward -n smartshop svc/redis-exporter 9121:9121
curl http://localhost:9121/metrics | grep redis_commands

# Grafana route
oc get route grafana -n smartshop -o jsonpath='{.spec.host}'
```

---

## Phase 2 — Data Ingestion

> **Goal:** `s3://smartshop-raw/raw/reviews/{Category}.parquet` and `raw/metadata/{Category}_meta.parquet` exist in MinIO.
> **Blocker for:** All Spark ETL jobs, Feast, training.

| Status | # | Step | Command | Done when |
|---|---|---|---|---|
| 🔲 | 2.1 | Upload download script to ConfigMap | `oc create configmap smartshop-download-script --from-file=download_to_minio.py=<(python3 -c "import json; cm=open('infrastructure/openshift/data-download-job.yaml').read(); ...") -n smartshop` | See note below |
| ✅ | 2.2 | Submit data download Job | `source .env && envsubst < infrastructure/openshift/data-download-job.yaml \| oc apply -f -` | Job created — pod `smartshop-data-download-flv9g` Running on `oai-kft-ibm-jcsbk-gpu-2-8gmgw` |
| 🔄 | 2.3 | Tail download logs | `oc logs -n smartshop -f job/smartshop-data-download` | Running — image pulling on GPU node (first run), will stream HF → MinIO |
| 🔲 | 2.4 | Verify reviews in MinIO | `AWS_ACCESS_KEY_ID=minio AWS_SECRET_ACCESS_KEY=minio123 aws s3 ls s3://smartshop-raw/raw/reviews/ --endpoint-url https://minio-s3-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com --no-verify-ssl` | 3 `.parquet` files (Electronics, Books, Home_and_Kitchen) |
| 🔲 | 2.5 | Verify metadata in MinIO | Same but `raw/metadata/` | 3 `_meta.parquet` files |

> **Note on 2.1:** The download script is embedded in the ConfigMap inside `data-download-job.yaml`.
> The easiest apply path is:
> ```bash
> # Extract the script from the YAML and create the ConfigMap separately
> source .env
> python3 - <<'EOF'
> import yaml, subprocess
> docs = list(yaml.safe_load_all(open('infrastructure/openshift/data-download-job.yaml').read()))
> cm = next(d for d in docs if d['metadata']['name'] == 'smartshop-download-script')
> script = cm['data']['download_to_minio.py']
> with open('/tmp/download_to_minio.py', 'w') as f:
>     f.write(script)
> print("Extracted to /tmp/download_to_minio.py")
> EOF
> oc create configmap smartshop-download-script \
>   --from-file=download_to_minio.py=/tmp/download_to_minio.py \
>   -n smartshop --dry-run=client -o yaml | oc apply -f -
> ```

**Expected download times (sample mode, cluster bandwidth):**
- Electronics: ~5 min (22 GB JSONL streamed, 333K rows kept)
- Books: ~8 min (largest category)
- Home_and_Kitchen: ~3 min
- Total: ~15–20 min

---

## Phase 3 — Spark ETL + RAPIDS A/B Proof

> **Goal:** Feature Parquet files in `s3://smartshop-features/`, plus a measured GPU speedup number.
> **Requires:** Phase 2 complete (data in MinIO).
> **Blocker for:** Feast materialization, model training.

### 3a — CPU baseline (run first to establish the benchmark)

| Status | # | Step | Command |
|---|---|---|---|
| 🔲 | 3.1 | Apply CPU baseline SparkApp | `source .env && envsubst < infrastructure/openshift/spark-application-cpu-baseline.yaml \| oc apply -f -` |
| 🔲 | 3.2 | Watch until COMPLETED | `oc get sparkapplication smartshop-feature-engineering-cpu-baseline -n smartshop -w` |
| 🔲 | 3.3 | Capture `[METRIC]` lines | `oc logs -n smartshop $(oc get pod -n smartshop -l spark-app-name=smartshop-feature-engineering-cpu-baseline,spark-role=driver -o name) \| grep METRIC` |
| 🔲 | 3.4 | Collect full metrics bundle | `RUN_TYPE=cpu APP_NAME=smartshop-feature-engineering-cpu-baseline bash scripts/collect-run-metrics.sh` |

### 3b — RAPIDS GPU (the headline demo run)

| Status | # | Step | Command |
|---|---|---|---|
| 🔲 | 3.5 | Apply RAPIDS SparkApp | `source .env && envsubst < infrastructure/openshift/spark-application-rapids.yaml \| oc apply -f -` |
| 🔲 | 3.6 | Watch Spark UI during run (GPU operators visible) | `oc port-forward -n smartshop <driver-pod> 4040:4040` → http://localhost:4040/SQL |
| 🔲 | 3.7 | Capture `[METRIC]` lines | grep for `[METRIC] gpu_accelerated=True` and `total_elapsed_s` |
| 🔲 | 3.8 | Collect full metrics bundle (speedup auto-computed vs CPU run) | `RUN_TYPE=rapids APP_NAME=smartshop-feature-engineering-rapids bash scripts/collect-run-metrics.sh` |

### 3c — Text preprocessing + embeddings (CPU path, needed for RAG)

| Status | # | Step | Command |
|---|---|---|---|
| 🔲 | 3.9 | Apply full pipeline SparkApp (text + embeddings) | `source .env && envsubst < infrastructure/openshift/spark-application.yaml \| oc apply -f -` |
| 🔲 | 3.10 | Wait for all 3 SparkApps COMPLETED | `oc get sparkapplication -n smartshop` |
| 🔲 | 3.11 | Verify feature files in MinIO | `aws s3 ls s3://smartshop-features/ --recursive --endpoint-url ...` |

**Expected Spark runtimes (sample dataset ~1M reviews):**
- CPU feature engineering: ~10–20 min
- RAPIDS feature engineering: estimated ~2–5 min (depends on GPU speedup)
- Text preprocessing: ~5–10 min
- Embedding generation: ~15–30 min (sentence-transformers on CPU/GPU)

---

## Phase 4 — Feast Materialization

> **Goal:** Redis has materialized user and item features; `redis_db_keys > 0`.
> **Requires:** Phase 3 feature Parquet files in MinIO.
> **Blocker for:** Model training (rec model reads features from Feast).

| Status | # | Step | Command | Done when |
|---|---|---|---|---|
| 🔲 | 4.1 | Apply Feast RBAC | `oc apply -f infrastructure/feast/feast-spark-rbac.yaml` | `oc get role,rolebinding -n smartshop \| grep feast` |
| 🔲 | 4.2 | Verify Feast pod is healthy | `oc get pod -n smartshop -l app=feast-smartshop-feast` | All containers Running |
| 🔲 | 4.3 | Get Feast pod name | `FEAST_POD=$(oc get pod -n smartshop -l app=feast-smartshop-feast -o jsonpath='{.items[0].metadata.name}')` | — |
| 🔲 | 4.4 | Run `feast apply` (register feature views) | `oc exec -n smartshop $FEAST_POD -c offline -- bash -c "cd /feast/feature_repo && feast apply"` | No errors |
| 🔲 | 4.5 | Run `feast materialize-incremental` | `oc exec -n smartshop $FEAST_POD -c offline -- bash -c "cd /feast/feature_repo && feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)"` | Logs show rows written to Redis |
| 🔲 | 4.6 | Verify Redis has feature keys | `oc exec -n smartshop deploy/redis -- redis-cli -a smartshop-redis-2026 DBSIZE` | Count > 0 |
| 🔲 | 4.7 | Check Redis ops in Grafana | Open Redis Feature Store dashboard | Keys visible, ops/sec non-zero |

---

## Phase 5 — Model Training

> **Goal:** Trained model artifacts in `s3://smartshop-models/`.
> **Requires:** Feast materialized (Phase 4) for rec model; embedding features (Phase 3c) for LLM.

### 5a — Recommendation Model (PyTorch DDP, 4× A100, 1 node)

| Status | # | Step | Command | Done when |
|---|---|---|---|---|
| 🔲 | 5.1 | Apply TrainingRuntime + TrainJob (rec) | `source .env && envsubst < infrastructure/openshift/trainjobs.yaml \| oc apply -f - --field-manager=rec` | `oc get trainjob smartshop-rec-train -n smartshop` |
| 🔲 | 5.2 | Monitor training progress | `oc get trainjob smartshop-rec-train -n smartshop -w` | Status = Complete |
| 🔲 | 5.3 | Check MLflow for loss curves | Open MLflow → `smartshop-rec-train` experiment | Loss converging |
| 🔲 | 5.4 | Verify model in MinIO | `aws s3 ls s3://smartshop-models/recommendation/` | `best_model.pt` exists |

### 5b — LLM Fine-Tuning (Mistral-7B, FSDP + QLoRA, Slurm, 2 nodes × 4 A100)

| Status | # | Step | Command | Done when |
|---|---|---|---|---|
| 🔲 | 5.5 | Verify Slurm partition is available | `oc exec -n smartshop <llm-trainer-pod> -- sinfo -p slinky` | Nodes idle or allocated |
| 🔲 | 5.6 | Apply LLM TrainJob | `source .env && envsubst < infrastructure/openshift/trainjobs.yaml \| oc apply -f -` | `oc get trainjob smartshop-llm-finetune -n smartshop` |
| 🔲 | 5.7 | Verify NCCL bandwidth (cross-node) | `oc logs -n smartshop <worker-pod> \| grep "busBw\|Avg bus"` | `> 100 GB/s` |
| 🔲 | 5.8 | Collect Slurm metrics bundle | `RUN_TYPE=slurm TRAINJOB_NAME=smartshop-llm-finetune bash scripts/collect-run-metrics.sh` | Bundle in MinIO |
| 🔲 | 5.9 | Verify adapter in MinIO | `aws s3 ls s3://smartshop-models/llm-adapter/` | Adapter weights exist |

---

## Phase 6 — Serving (KServe InferenceServices)

> **Goal:** All 3 InferenceServices Ready; endpoints returning valid responses.
> **Requires:** Phase 5 model artifacts in MinIO.

| Status | # | Step | Command | Done when |
|---|---|---|---|---|
| 🔲 | 6.1 | Apply all 3 InferenceServices | `source .env && envsubst < infrastructure/openshift/inferenceservices.yaml \| oc apply -f -` | Resources created |
| 🔲 | 6.2 | Wait for rec model Ready | `oc get isvc smartshop-rec -n smartshop -w` | `READY=True` |
| 🔲 | 6.3 | Wait for LLM model Ready (may take 5–10 min to load) | `oc get isvc smartshop-llm -n smartshop -w` | `READY=True` |
| 🔲 | 6.4 | Wait for RAG Ready | `oc get isvc smartshop-rag -n smartshop -w` | `READY=True` |
| 🔲 | 6.5 | Smoke test rec endpoint | `curl -X POST $RECOMMEND_URL -H 'Content-Type: application/json' -d '{"user_id":"test123","top_k":5}'` | JSON with `recommendations` array |
| 🔲 | 6.6 | Smoke test LLM endpoint | `curl -X POST $SUMMARIZE_URL -d '{"product_name":"Headphones","review_text":"Great sound quality"}'` | JSON with `summary` |
| 🔲 | 6.7 | Smoke test RAG endpoint | `curl -X POST $RAG_URL -d '{"question":"Is this good for gaming?","product_id":""}'` | JSON with `answer` |

---

## Phase 7 — Demo + Summit Proof Artifacts

> **Goal:** Live Gradio UI working end-to-end; all charts generated for Summit slides/blog.

### 7a — Gradio UI

| Status | # | Step | Command | Done when |
|---|---|---|---|---|
| 🔲 | 7.1 | Set `PROMETHEUS_TOKEN` in `.env` | `oc sa get-token grafana-sa -n smartshop` | Gradio Platform Metrics tab shows live data |
| 🔲 | 7.2 | Run Gradio locally or deploy as pod | `source .env && python demo/app.py` | http://localhost:7860 opens |
| 🔲 | 7.3 | Test recommendation flow end-to-end | Enter user ID → Get Recommendations → watch GPU spike | Response in <100ms |
| 🔲 | 7.4 | Screenshot Platform Metrics tab | GPU utilization spike during inference | Summit slide ready |

### 7b — Summit Charts (via analysis notebook)

| Status | # | Step | Output |
|---|---|---|---|
| 🔲 | 7.5 | Run `notebooks/metrics_analysis.ipynb` | `mlflow_gpu_vs_cpu.png` — wall-clock speedup bar chart |
| 🔲 | 7.6 | — | `dcgm_combined.png` — 6-panel GPU metrics |
| 🔲 | 7.7 | — | `redis_metrics.png` — ops/sec, hit ratio, memory |
| 🔲 | 7.8 | — | `rapids_coverage.png` — GPU operator coverage pie |
| 🔲 | 7.9 | — | `mlflow_stage_breakdown.png` — per-stage timing comparison |
| 🔲 | 7.10 | Bundle all charts as zip | `summit_charts.zip` uploaded to MinIO |

---

## Blocked / Deferred

| Item | Reason | When to revisit |
|---|---|---|
| Feast `SparkOfflineStore` (pyspark) | RHOAI `odh-feature-server-rhel9` image missing `pyspark` | Custom Feast image build post-Summit |
| Feast `SQLRegistry` (psycopg2) | Same image limitation | Same as above |
| OTEL distributed tracing (Gradio→KServe→Feast→Redis) | Needs OTEL Collector + Tempo backend | Post-Summit infra upgrade |
| RAPIDS full dataset (571M reviews) | 49 GB download, ~4h ETL | Summit recording run (not demo) |
| Embedding generation on GPU | `embedding_generation.py` uses sentence-transformers, runs on `spark-jobs` not `spark-jobs-rapids` | Separate GPU-enabled embedding job |

---

## Quick reference — one-liner to apply each phase

```bash
# Phase 1 — Observability
source .env
oc apply -f infrastructure/openshift/user-workload-monitoring.yaml
envsubst < infrastructure/openshift/spark-metrics-configmap.yaml | oc apply -f -
envsubst < infrastructure/openshift/redis-exporter.yaml | oc apply -f -
envsubst < infrastructure/openshift/grafana.yaml | oc apply -f -
PROMETHEUS_TOKEN=$(oc sa get-token grafana-sa -n smartshop)

# Phase 2 — Data
envsubst < infrastructure/openshift/data-download-job.yaml | oc apply -f -
oc logs -n smartshop -f job/smartshop-data-download

# Phase 3 — Spark ETL
envsubst < infrastructure/openshift/spark-application-cpu-baseline.yaml | oc apply -f -
# (wait for COMPLETED)
envsubst < infrastructure/openshift/spark-application-rapids.yaml | oc apply -f -
envsubst < infrastructure/openshift/spark-application.yaml | oc apply -f -

# Phase 4 — Feast
oc apply -f infrastructure/feast/feast-spark-rbac.yaml
FEAST_POD=$(oc get pod -n smartshop -l app=feast-smartshop-feast -o jsonpath='{.items[0].metadata.name}')
oc exec -n smartshop $FEAST_POD -c offline -- bash -c "cd /feast/feature_repo && feast apply"
oc exec -n smartshop $FEAST_POD -c offline -- bash -c "cd /feast/feature_repo && feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)"

# Phase 5 — Training
envsubst < infrastructure/openshift/trainjobs.yaml | oc apply -f -

# Phase 6 — Serving
envsubst < infrastructure/openshift/inferenceservices.yaml | oc apply -f -
oc get isvc -n smartshop -w

# Phase 7 — Demo
python demo/app.py
# Open notebooks/metrics_analysis.ipynb in JupyterLab
```

---

## Status update log

| Date | Update |
|---|---|
| 2026-04-08 | Phases 1–7 documented; cluster state: Feast Ready, Redis 0 keys, no data, no Spark jobs, no training, no serving |
| 2026-04-08 | Phase 2 started — `smartshop-data-download` Job submitted, pod pulling image on `oai-kft-ibm-jcsbk-gpu-2-8gmgw`. Expected runtime: ~20 min for Electronics+Books+Home_and_Kitchen sample. Monitor: `oc logs -n smartshop -f job/smartshop-data-download` |
