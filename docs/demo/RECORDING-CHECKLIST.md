# Demo Recording Checklist

> **Red Hat Summit 2026** — 9 clips, ~10 minutes total, no voice (live voiceover during session).
> Core message: *"The developer writes Python and YAML. OpenShift AI handles everything else."*

## Pre-Recording Setup

- [ ] `oc login https://api.oai-kft-ibm.ibm.rh-ods.com:6443`
- [ ] Clean browser — no bookmarks bar, no extra tabs
- [ ] Browser zoom 110-125% for readability on projector
- [ ] Terminal font size 16-18pt, dark theme
- [ ] Screen recorder ready (QuickTime / OBS), 1080p+
- [ ] Bookmark all URLs below in a folder for quick switching

## URLs

| Label | URL |
|-------|-----|
| RHOAI Dashboard | `https://rh-ai.apps.oai-kft-ibm.ibm.rh-ods.com` |
| MinIO Console | `https://minio-console-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com` |
| Spark History | `https://spark-history-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com` |
| Grafana | `https://grafana-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com` |
| Feast UI | `https://feast-smartshop-feast-ui-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com` |
| RedisInsight | `https://redisinsight-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com` |
| MLflow | `https://mlflow-redhat-ods-applications.apps.oai-kft-ibm.ibm.rh-ods.com` |
| Gradio Demo UI | `https://smartshop-demo-ui-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com` |
| Grafana Inference | `https://grafana-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com/d/smartshop-inference` |

## Pre-Flight Verification

Run these before recording to confirm everything is healthy:

```bash
# All 3 InferenceServices ready?
oc get inferenceservice -n smartshop

# Spark jobs completed?
oc get sparkapplication -n smartshop

# TrainJobs completed?
oc get trainjob -n smartshop

# Demo UI pod running?
oc get pods -n smartshop -l component=demo-ui

# Grafana password (if prompted):
oc get secret grafana-admin-credentials -n smartshop \
  -o jsonpath='{.data.GF_SECURITY_ADMIN_PASSWORD}' | base64 -d
```

---

## Clip-by-Clip Recording Script

### Clip 1 — "The Platform" (45s)

**Screen: RHOAI Dashboard**

| Step | Action |
|------|--------|
| 1 | Open `https://rh-ai.apps.oai-kft-ibm.ibm.rh-ods.com` |
| 2 | Show landing page — enabled applications, installed operators |
| 3 | Click **Data Science Projects** → show `smartshop` project |
| 4 | Scroll sidebar: Distributed Workloads, Model Serving, Model Registry |

> Voiceover hook: "This is the control plane. Everything you're about to see is managed through this single platform."

---

### Clip 2 — "Data at Scale" (30s)

**Screen: MinIO Console**

| Step | Action |
|------|--------|
| 1 | Open MinIO → `smartshop-raw/raw/` → scroll to show ~49 GB of files |
| 2 | Switch to `smartshop-features/` → show Parquet output |
| 3 | Switch to `smartshop-models/` → show trained model artifacts |

> Voiceover hook: "Single storage layer. No copy steps between pipeline stages."

---

### Clip 3 — "Spark ETL + RAPIDS" (60s)

**Screen: Terminal → Spark History Server**

| Step | Action |
|------|--------|
| 1 | Terminal: `oc get sparkapplication -n smartshop` |
| 2 | Open Spark History → click RAPIDS job → Stages tab |
| 3 | Back → click CPU baseline → same Stages tab |
| 4 | Linger on Duration: **537s (RAPIDS) vs 719s (CPU)** |

> Voiceover hook: "Same code, same data. The Spark Operator swaps CPU for RAPIDS GPU executors — no code changes."

---

### Clip 4 — "GPU Observability" (45s)

**Screen: Grafana**

| Step | Action |
|------|--------|
| 1 | Open GPU dashboard → show DCGM utilization spikes |
| 2 | Show SM Active %, Framebuffer memory on A100s |
| 3 | Switch to Redis dashboard → show key count from Feast materialization |

> Voiceover hook: "GPU Operator + DCGM Exporter + User Workload Monitoring. Zero manual Prometheus config."

---

### Clip 5 — "Feature Store" (60s)

**Screen: Feast UI → RedisInsight**

| Step | Action |
|------|--------|
| 1 | Open Feast UI → Feature Views → show `user_features`, `item_features`, `item_metadata`, `review_embeddings` |
| 2 | Click `user_features` → show schema |
| 3 | Switch to RedisInsight → show 35M+ keys |
| 4 | Click one key → show hash fields (pre-computed feature vector) |

> Voiceover hook: "Feast Operator deployed the entire feature store from one Custom Resource. Same features for training and serving — zero skew."

---

### Clip 6 — "Distributed Training" (90s) — MOST TIME IN RHOAI DASHBOARD

**Screen: Terminal → RHOAI Dashboard → MLflow**

| Step | Action |
|------|--------|
| 1 | Terminal: `oc get trainjob -n smartshop` |
| 2 | RHOAI Dashboard → **Distributed Workloads** → show `smartshop-rec-train` TrainJob |
| 3 | Show worker pods, GPU allocation, status timeline |
| 4 | MLflow → `smartshop` workspace → `smartshop-rec-train` experiment |
| 5 | Show loss curve dropping over epochs |
| 6 | Click run → show hyperparams + artifact path `s3://smartshop-models/...` |
| 7 | Open `smartshop-llm-train` → show QLoRA params (rank=16, 4-bit, FSDP) |

> Voiceover hook: "Kubeflow Trainer orchestrates multi-GPU training. MLflow tracks everything. The artifact path goes straight to KServe — zero-copy deployment."

---

### Clip 7 — "Model Serving" (45s)

**Screen: RHOAI Dashboard → Terminal**

| Step | Action |
|------|--------|
| 1 | RHOAI Dashboard → **Model Serving** → show 3 deployed models |
| 2 | Click `smartshop-llm` → show vLLM runtime, replica count, endpoint URL |
| 3 | Terminal: `oc get inferenceservice -n smartshop` → all 3 READY=True |

> Voiceover hook: "KServe + Model Registry. The model the registry promotes is the exact artifact MLflow wrote."

---

### Clip 8 — "Live End-to-End" (150s) — THE HERO CLIP

**Screen: Gradio Demo UI**

**8a. Product Recommendations (~50s)**

| Step | Action |
|------|--------|
| 1 | Open Gradio UI |
| 2 | Select persona "Alice — Tech Enthusiast" |
| 3 | Click "Get Recommendations" |
| 4 | Show pipeline trace: Feast Lookup → Two-Tower Model → Top-K |
| 5 | Expand "Behind the scenes" — linger for 5s |

**8b. Review Intelligence (~50s)**

| Step | Action |
|------|--------|
| 1 | Switch to "Review Intelligence" tab |
| 2 | Click example: Sony WH-1000XM5 |
| 3 | Click "Summarize Review" |
| 4 | Show pipeline trace: Build Prompt → Mistral-7B + LoRA → Sentiment |
| 5 | Expand "Pipeline trace" — linger for 5s |

**8c. Product Q&A / RAG (~50s)**

| Step | Action |
|------|--------|
| 1 | Switch to "Product Q&A" tab |
| 2 | Click example question |
| 3 | Click "Ask" |
| 4 | Show pipeline trace: Embed Query → Milvus via Feast → LLM Generation |
| 5 | Expand "Sources" — show real reviews from Milvus |

> Voiceover hook: "Every request touches Feast, KServe, vLLM, Milvus — all RHOAI-managed. The pipeline traces make the platform visible."

---

### Clip 9 — "Observability + Architecture" (45s)

**Screen: Gradio UI (Observability + Architecture tabs)**

| Step | Action |
|------|--------|
| 1 | "Observability" tab → show green service health indicators |
| 2 | Show embedded Grafana dashboards auto-refreshing |
| 3 | "Architecture" tab → slowly scroll through all 7 layers |

> Voiceover hook: "ServiceMonitors + Prometheus + Grafana. Zero-config scraping for every inference endpoint."

---

## Timeline

```
Clip 1: RHOAI Dashboard — The Platform          0:00 – 0:45    (45s)
Clip 2: MinIO — Data at Scale                    0:45 – 1:15    (30s)
Clip 3: Spark History — ETL + RAPIDS             1:15 – 2:15    (60s)
Clip 4: Grafana — GPU Observability              2:15 – 3:00    (45s)
Clip 5: Feast + Redis — Feature Store            3:00 – 4:00    (60s)
Clip 6: RHOAI + MLflow — Distributed Training    4:00 – 5:30    (90s)
Clip 7: RHOAI Model Serving — KServe Endpoints   5:30 – 6:15    (45s)
Clip 8: Gradio UI — Live End-to-End Demo         6:15 – 8:45    (150s)
Clip 9: Observability + Architecture             8:45 – 9:30    (45s)
                                                 ─────────────
                                                 TOTAL: ~9:30
```

## Recording Tips

- **Fast-forward:** Clip 3 (Spark stage scrolling), Clip 5 (RedisInsight browsing), Clip 6 (MLflow loading)
- **Linger on:** RHOAI Dashboard in Clips 1, 6, 7. Pipeline traces in Clip 8. Loss curves in Clip 6. GPU spikes in Clip 4.
- **Record each clip as a separate file** — advance manually during live session for full pacing control
- **Mouse:** Move slowly and deliberately. Audience follows your cursor.
