# SmartShop AI — End-to-End Demo Recording Guide

> **Red Hat Summit 2026**
> Session: *Production ML at Scale: Distributed Training with PyTorch, Kubeflow, Spark & Feast on Red Hat OpenShift AI*

This document provides **detailed, step-by-step instructions** for recording each phase of the demo. For each phase it specifies: what screens to show, exact navigation paths, what to highlight for the audience, and what OpenShift AI capability is being demonstrated.

**Recording format:** Silent screen recordings. You narrate live during the session.
**Total target:** ~10 minutes across 9 clips.

---

## Phase 0 — Prerequisites (show, don't deploy)

> **Goal:** Establish that the cluster is already set up with all the RHOAI operators and infrastructure services. These are prerequisites the audience should know exist before the pipeline walkthrough begins.

### What to show

**0a. Installed Operators (RHOAI Dashboard or OperatorHub)**

1. Open the **OpenShift Console** (not RHOAI Dashboard — the OCP Console)
2. Navigate: **Operators > Installed Operators**
3. Filter by namespace `smartshop` or show cluster-wide
4. Scroll through and **pause briefly** on each of these:
   - **Red Hat OpenShift AI** (the platform itself)
   - **Apache Spark Operator** (manages SparkApplications)
   - **Kubeflow Trainer** (manages TrainJobs for distributed training)
   - **Feast Operator** (manages FeatureStore CRs)
   - **NVIDIA GPU Operator** (manages GPU drivers, DCGM, device plugin)
   - **KServe / OpenShift Serverless** (manages InferenceServices)

**Highlight:** "These six operators are the backbone. Each one adds a Custom Resource to Kubernetes. The entire ML pipeline — from ETL to serving — is defined as Kubernetes-native CRDs."

**0b. Infrastructure Services Running**

1. Switch to **Workloads > Pods** in namespace `smartshop`
2. Filter or scroll to show these running:
   - `minio-*` — S3-compatible object store (raw data, features, models)
   - `redis-*` — Feast online store (35M pre-materialized feature vectors)
   - `milvus-*` — Vector database (104M review embeddings for RAG)
   - `postgres-*` — MLflow backend database
   - `mlflow-*` — Experiment tracking server
   - `feast-smartshop-*` — Feast registry, server, UI
3. Optionally show **Networking > Routes** to show all the UIs that are exposed

**Highlight:** "All infrastructure runs inside the same OpenShift namespace. No external managed services. Everything is self-contained and reproducible from YAML manifests."

**Why this matters for recording:** This gives context before you dive into the pipeline. The audience understands the platform foundation before seeing what it can do. Record this as a quick 30-45s clip or fold it into Clip 1.

---

## Phase 1 — The Platform (RHOAI Dashboard)

> **OpenShift AI capability:** The unified control plane for all ML workloads.

### Exact navigation

1. Open: `https://rh-ai.apps.oai-kft-ibm.ibm.rh-ods.com`
2. You land on the **RHOAI Dashboard home page**
3. **Left sidebar** — slowly hover over each section:
   - **Data Science Projects** — click this
   - **Distributed Workloads** — just show it exists (you'll return here in Phase 6)
   - **Model Serving** — just show it exists (you'll return here in Phase 7)
   - **Model Registry** — just show it exists
4. Inside **Data Science Projects**, click the `smartshop` project
5. Show what's inside:
   - **Workbenches** — if any exist, show them (this is where notebooks run)
   - **Data connections** — show the MinIO S3 connection configured
   - **Model servers** — show the 3 InferenceServices listed

### What to highlight in recording

| Element | Why it matters |
|---------|---------------|
| RHOAI Dashboard sidebar | Shows breadth of platform — ETL, training, serving, registry in one UI |
| `smartshop` project view | Everything for this use case lives in one Data Science Project |
| Data connections | S3/MinIO is configured as a first-class RHOAI data connection — not a custom hack |
| Workbenches section | This is where data scientists work — JupyterLab with GPU access, managed by the platform |

### Recording tips

- Move slowly through the sidebar — let each label be readable for 2-3 seconds
- When you click into `smartshop`, pause for 3-4 seconds so the audience can read the tabs
- This is the "establishing shot" — set the stage before diving into components

---

## Phase 2 — Data at Scale (MinIO Console)

> **OpenShift AI capability:** S3-compatible object storage as a managed data connection.

### Exact navigation

1. Open: `https://minio-console-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com`
2. Login with MinIO credentials
3. Click **Object Browser** in the left sidebar
4. Navigate into **`smartshop-raw`** bucket:
   - Click into `raw/` folder
   - Scroll through the Parquet/JSON files — show file sizes to convey 49 GB scale
   - Hover over a few files so the tooltip shows size
5. Go back to bucket list, click **`smartshop-features`**:
   - Show `user_features/` and `item_features/` folders — these are Spark ETL output
6. Go back, click **`smartshop-models`**:
   - Show `recommendation/` (Two-Tower model) and `llm-checkpoints/` (LoRA adapter)

### What to highlight in recording

| Element | Why it matters |
|---------|---------------|
| `smartshop-raw` file sizes | Conveys real scale — 49 GB, 140M+ reviews, not a toy dataset |
| `smartshop-features` Parquet files | Spark wrote these. Feast reads them. Same S3 path, no copies. |
| `smartshop-models` artifacts | Training wrote these. KServe serves from the same path. Zero re-upload. |
| Single MinIO instance | **Key point:** raw data, features, and models all live in one storage layer |

### Recording tips

- Scroll slowly through `smartshop-raw` so file count/sizes are visible
- Keep this clip short (30s) — MinIO is a supporting character, not the star

---

## Phase 3 — Spark ETL + RAPIDS (Terminal + Spark History Server)

> **OpenShift AI capability:** Spark Operator manages SparkApplications as Kubernetes CRDs. GPU Operator enables RAPIDS acceleration.

### Exact navigation

1. **Terminal** (record terminal with dark background):
   ```bash
   oc get sparkapplication -n smartshop
   ```
   - Show output: 3-4 completed SparkApplications (CPU baseline, RAPIDS, text preprocessing, embedding)
   - Pause 3-4 seconds so the audience can read STATUS=COMPLETED

2. **Spark History Server**: `https://spark-history-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com`
   - You see a list of completed Spark applications
   - Click **`smartshop-feature-engineering-rapids`** (the GPU job)
   - Click the **Stages** tab
   - Slowly scroll through stages — show the timing for each stage
   - Note the total duration in the header
   - Click **Back** to return to the app list
   - Click **`smartshop-feature-engineering-cpu-baseline`** (the CPU job)
   - Click the **Stages** tab again
   - Show the same stages, but with longer durations
   - Scroll to the **Jobs** tab — show total Duration

3. **Side-by-side comparison** the audience should see:

   | Metric | CPU Baseline | RAPIDS GPU |
   |--------|-------------|------------|
   | Total wall-clock | ~719s | ~537s |
   | Item aggregation stage | 41s | 22s |
   | Speedup | — | **1.34x overall** |

### What to highlight in recording

| Element | Why it matters |
|---------|---------------|
| `oc get sparkapplication` output | SparkApplications are Kubernetes CRDs — not a Spark cluster you manage |
| Same stage names in both jobs | Identical Python code — only the executor image changed |
| Duration difference | RAPIDS GPU acceleration with zero code changes — just a different container image |
| Spark History Server | Automatically populated from S3 event logs — the Spark Operator configured this |

### Recording tips

- **Linger on the Duration column** — this is the punchline of the clip
- Consider a brief pause at the top of each job showing the app name so it's clear which is RAPIDS vs CPU
- Fast-forward scrolling through individual stages if needed

---

## Phase 4 — GPU Observability (Grafana)

> **OpenShift AI capability:** GPU Operator + DCGM Exporter + User Workload Monitoring.

### Exact navigation

1. Open: `https://grafana-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com`
2. Login: admin / (get password: `oc get secret grafana-admin-credentials -n smartshop -o jsonpath='{.data.GF_SECURITY_ADMIN_PASSWORD}' | base64 -d`)
3. Click **Dashboards** in the left sidebar
4. Open the **GPU Utilization** dashboard (or DCGM dashboard)
5. Set the time range to cover when the RAPIDS job ran
   - You should see GPU utilization spikes during that window
   - Show **SM Active %** — streaming multiprocessor utilization
   - Show **Framebuffer Memory** — A100 80GB filling up during joins
6. Go back to Dashboards, open the **Redis** dashboard
   - Show key count climbing during Feast materialization
   - Show ops/sec during serving requests

### What to highlight in recording

| Element | Why it matters |
|---------|---------------|
| DCGM GPU utilization spikes | Real GPU metrics from hardware — not synthetic. GPU Operator manages DCGM Exporter. |
| SM Active % | Shows GPUs are actually computing, not just allocated |
| Framebuffer memory | Shows A100 memory pressure during RAPIDS joins — real workload evidence |
| Redis key count | Shows Feast materialization populating the online store in real time |
| Prometheus scraping | All of this is auto-scraped by User Workload Monitoring — zero manual config |

### Recording tips

- Set a time range that clearly shows the spike (not too zoomed out)
- If GPU data is flat/old, set the time picker to the window when RAPIDS ran
- This clip is about visual impact — GPU spikes are visually compelling on a dashboard

---

## Phase 5 — Feature Store (Feast UI + RedisInsight)

> **OpenShift AI capability:** Feast Operator deploys the entire feature store from a single FeatureStore CR.

### Exact navigation

1. **Feast UI**: `https://feast-smartshop-feast-ui-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com`
   - Click **Feature Views** in the left sidebar
   - You should see: `user_features`, `item_features`, `review_embeddings`
   - Click **`user_features`**:
     - Show the feature schema: `avg_rating`, `review_count`, `avg_price`, `total_reviews`, etc.
     - Show the entity: `user_id`
     - Show the data source: points to `s3://smartshop-features/user_features/`
   - Go back, click **`review_embeddings`**:
     - Show this is a vector feature view — embeddings stored in Milvus
   - Click **Data Sources** — show the S3/MinIO Parquet sources
   - Click **Entities** — show `user_id`, `item_id`

2. **RedisInsight**: `https://redisinsight-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com`
   - Connect to the Redis instance if not already connected
   - Click **Browser** tab
   - Show the key count: **35M+ keys** (visible in the header or by scrolling)
   - Type a key pattern filter if helpful (e.g., `smartshop:user_features:*`)
   - Click any key — show the **Hash fields**:
     - Each field is a pre-computed feature: `avg_rating`, `review_count`, etc.
     - These are the exact values the rec model reads at serving time

### What to highlight in recording

| Element | Why it matters |
|---------|---------------|
| Feature Views list | Defined once in Python, used for both training and serving |
| `user_features` schema | Every feature the model was trained on is available at serving time — same schema |
| Data source → S3 path | Feast reads from the same MinIO that Spark wrote to |
| `review_embeddings` | Vector features for RAG — stored in Milvus, managed by Feast |
| 35M Redis keys | Pre-materialized feature vectors — sub-millisecond lookup at serving time |
| Hash field values | These are the actual feature values the KServe endpoint reads via `get_online_features` |

### Recording tips

- In Feast UI, slowly click through Feature Views → schema → data source — let the audience trace the lineage
- In RedisInsight, clicking into one key and showing the hash fields is the "aha" moment
- **Key message:** "Features defined once, materialized to Redis, served in < 1ms. No training-serving skew."

---

## Phase 6 — Distributed Training (RHOAI Workbenches + MLflow)

> **OpenShift AI capability:** Kubeflow Trainer v2 for TrainJob orchestration. RHOAI Workbenches for interactive notebook development. MLflow for experiment tracking.

This is the richest phase — spend the most time here.

### Option A: Show completed TrainJobs (if already trained)

1. **Terminal:**
   ```bash
   oc get trainjob -n smartshop
   ```
   Show both TrainJobs: `smartshop-rec-train` and `smartshop-llm-train` with status Succeeded

2. **RHOAI Dashboard → Distributed Workloads:**
   - Open: `https://rh-ai.apps.oai-kft-ibm.ibm.rh-ods.com`
   - Click **Distributed Workloads** in the sidebar
   - Select the `smartshop` project
   - Show the `smartshop-rec-train` TrainJob:
     - **Status timeline** — shows when it started, ran, completed
     - **Worker pods** — show 4 pods (torchrun DDP, 1 node x 4 GPUs)
     - **Resource usage** — GPU allocation per pod
   - If `smartshop-llm-train` is visible, show it too (2 nodes x 4 GPUs for FSDP)

### Option B: Record creating a Workbench and running the notebook (more compelling)

1. **RHOAI Dashboard → Data Science Projects → smartshop**
2. Click **Workbenches** tab
3. Click **Create workbench**:
   - Name: `smartshop-training`
   - Image: select a PyTorch image (e.g., `PyTorch` or `CUDA`)
   - Container size: Large (8 CPU, 32GB RAM)
   - GPU: request 1 (for submitting TrainJobs — the TrainJob workers get their own GPUs)
   - Data connections: select the MinIO S3 connection
   - Click **Create**
4. Once the workbench starts, click **Open** to launch JupyterLab
5. Upload `notebooks/01_training_rec.ipynb` and `notebooks/02_training_llm.ipynb`
6. Open `01_training_rec.ipynb`:
   - Show the notebook cells — Kubeflow SDK (`TrainerClient`) creates and submits a TrainJob
   - Run the key cells that show:
     - `from kubeflow.training import TrainerClient`
     - The TrainJob spec definition (model, dataset, GPU count, training runtime)
     - `client.create_train_job(...)` — this is the one-liner that submits to K8s
   - Show the notebook waiting for the TrainJob to complete
7. Show MLflow integration cells — `mlflow.log_metric()`, `mlflow.log_artifact()`

### MLflow Experiment Tracking

1. **MLflow UI**: `https://mlflow-redhat-ods-applications.apps.oai-kft-ibm.ibm.rh-ods.com`
2. Select the **`smartshop`** workspace (top-left dropdown)
3. Open the **`smartshop-rec-train`** experiment:
   - Show the run list — each training run is logged
   - Click the latest run
   - **Metrics tab:** show the loss curve dropping over epochs — this is the visual proof training worked
   - **Parameters tab:** show logged hyperparameters:
     - `epochs`, `batch_size`, `learning_rate`, `embed_dim`, `hidden_dim`
     - `num_nodes`, `gpus_per_node` — shows distributed config
   - **Artifacts tab:** show the artifact path: `s3://smartshop-models/recommendation/`
     - **Key point:** This is the exact same S3 path KServe reads. No re-upload.
4. Go back, open the **`smartshop-llm-train`** experiment:
   - Show QLoRA-specific parameters: `lora_rank=16`, `quantization=4bit`, `fsdp=True`
   - Show the adapter artifact: `s3://smartshop-models/llm-checkpoints/checkpoint-200/`
   - **Key point:** The adapter is ~270 MB vs ~28 GB for a full fine-tune

### What to highlight in recording

| Element | Why it matters |
|---------|---------------|
| RHOAI Distributed Workloads page | Platform-native view of distributed training jobs — not a CLI-only workflow |
| TrainJob worker pods + GPU allocation | Kubeflow Trainer orchestrates multi-GPU training as a single CRD |
| Workbench creation (Option B) | Data scientists use RHOAI Workbenches — JupyterLab with GPU, managed by the platform |
| KF SDK `create_train_job()` | One Python call submits a distributed training job to Kubernetes |
| MLflow loss curves | Visual proof the model learned — training loss decreasing over epochs |
| MLflow artifact path = KServe storageUri | Zero-copy model deployment — same S3 path from training to serving |
| QLoRA parameters | 7B parameter LLM fine-tuned with < 1% trainable params, fitting on a single A100 |

### Recording tips

- If using Option B (workbench), record the workbench creation flow — it shows the RHOAI UX
- In MLflow, **linger on the loss curve** for 4-5 seconds — it's visually satisfying
- When showing the artifact path, make sure the S3 URI is fully visible — the audience needs to see it matches what KServe reads
- This is the longest clip — 90s+. Don't rush.

---

## Phase 7 — Model Serving (RHOAI Dashboard + Terminal)

> **OpenShift AI capability:** KServe InferenceServices with autoscaling. RHOAI Model Registry for artifact lineage.

### Exact navigation

1. **RHOAI Dashboard → Model Serving:**
   - Open: `https://rh-ai.apps.oai-kft-ibm.ibm.rh-ods.com`
   - Click **Model Serving** in the sidebar
   - Select the `smartshop` project
   - You should see **3 deployed models**:
     - `smartshop-rec` — Two-Tower recommendation model (FastAPI)
     - `smartshop-llm` — Mistral-7B with LoRA adapter (vLLM)
     - `smartshop-rag` — RAG Q&A (FastAPI + Feast + Milvus)
   - Click into **`smartshop-llm`**:
     - Show the **ServingRuntime**: vLLM
     - Show the **endpoint URL**
     - Show **replica count** and **resource allocation** (GPU)
     - Show the **model path** pointing to S3

2. **Terminal** (confirms the same info programmatically):
   ```bash
   oc get inferenceservice -n smartshop
   ```
   Show output with all 3 endpoints, READY=True, and their URLs.

3. **(Optional) RHOAI Model Registry:**
   - If accessible, click **Model Registry** in the sidebar
   - Show the registered models — each points to the MLflow artifact S3 path
   - This closes the loop: MLflow artifact → Model Registry → KServe endpoint

### What to highlight in recording

| Element | Why it matters |
|---------|---------------|
| 3 models in Model Serving page | Three different model types (custom PyTorch, vLLM LLM, RAG composite) all managed by one platform |
| vLLM ServingRuntime | RHOAI provides pre-built serving runtimes — vLLM with continuous batching + PagedAttention |
| Endpoint URL | Production HTTPS endpoints, TLS terminated by OpenShift Router |
| READY=True | KServe health checks confirm models are loaded and serving |
| Model path = MLflow artifact | The serving model is the exact artifact from training — no copy, no re-upload |

### Recording tips

- Start in the RHOAI Dashboard (Model Serving page) — this is the RHOAI UX
- Then show the terminal command as confirmation — it's the same data, just CLI vs UI
- Keep this clip tight (45s) — the audience saw the models, they'll see them in action in the next clip

---

## Phase 8 — Live End-to-End Demo (Gradio UI)

> **OpenShift AI capability:** All RHOAI-managed services working together in a user-facing application.

### Exact navigation

**Open:** `https://smartshop-demo-ui-smartshop.apps.oai-kft-ibm.ibm.rh-ods.com`

#### 8a. Product Recommendations (~50s)

1. You land on the **Product Recommendations** tab
2. Read the description text briefly — it mentions Feast and Two-Tower model
3. Under **Select a customer profile**, click a persona (e.g., first one)
4. Click the green **"Get Recommendations"** button
5. **Wait for results** — you'll see:
   - **Pipeline trace** at the top: three green boxes showing `Feast Lookup → Two-Tower Model → Top-K Ranking` with timing
   - **Ranked results** below: product cards with score bars, medals for top 3
6. Click **"Behind the scenes"** (expandable section):
   - Shows: Feast Redis lookup < 1ms, Two-Tower scoring of full catalog, KServe serving

**What to highlight:** Pipeline trace shows the audience exactly which RHOAI components handle each step. Feast provides features, KServe runs the model. Sub-100ms end-to-end.

#### 8b. Review Intelligence (~50s)

1. Click the **"Review Intelligence"** tab
2. Under **Try an example**, click the **Sony WH-1000XM5** example (fills in product name + review text)
3. Click the green **"Summarize Review"** button
4. **Wait for results** — you'll see:
   - **Pipeline trace**: `Build Prompt → Mistral-7B + LoRA → Sentiment + Summary` with timing and token throughput
   - **Generated summary** with a colored **sentiment badge** (POSITIVE/NEGATIVE/MIXED)
5. Click **"Pipeline trace"** (expandable):
   - Shows: Mistral-7B fine-tuned with QLoRA + FSDP, LoRA adapter hot-loaded, vLLM on KServe with continuous batching

**What to highlight:** The LLM was fine-tuned via Kubeflow Trainer (Phase 6). The LoRA adapter is loaded at serving time by vLLM — no base model retraining. KServe manages the endpoint.

#### 8c. Product Q&A / RAG (~50s)

1. Click the **"Product Q&A"** tab
2. Under **Try an example**, click any question (e.g., "What do customers say about battery life?")
3. Click the green **"Ask"** button
4. **Wait for results** — you'll see:
   - **Pipeline trace**: `Embed Query → Milvus Search via Feast → LLM Generation` with timing
   - **Generated answer** grounded in real reviews
   - **Source reviews** — actual Amazon reviews that the system retrieved
5. Click **"Sources"** to expand and show the retrieved reviews with similarity scores

**What to highlight:** The query is embedded, Feast's Milvus vector store retrieves relevant reviews, and the LLM generates an answer grounded in real data — no hallucination. Three RHOAI components in one request: Feast (vector search), Milvus (storage), KServe (LLM inference).

### Recording tips for this phase

- **Linger on pipeline traces** — they are the most RHOAI-relevant visual. Each green box is an RHOAI component.
- Wait for results to fully render before clicking the next tab
- Expand "Behind the scenes" / "Pipeline trace" / "Sources" — these show the technical depth
- Move between tabs slowly — let the tab labels be readable
- This is the hero clip — **take your time** (150s budget)

---

## Phase 9 — Observability + Architecture (Gradio UI)

> **OpenShift AI capability:** User Workload Monitoring, ServiceMonitors, Prometheus integration.

### Exact navigation

1. Still in the Gradio UI, click the **"Observability"** tab
2. **Service Health section:**
   - Show the health indicators — green for all 3 endpoints (rec, llm, rag)
   - If any is red, it means that endpoint is down (which shouldn't happen)
3. **Demo Session Stats:**
   - Shows request counts and latencies from the demo session
   - If you've been clicking through tabs 1-3, these will have real data
4. **Grafana Dashboards:**
   - Scroll down to see embedded Grafana panels (iframes)
   - Show: Request Rate, vLLM Token Throughput, Latency panels
   - These auto-refresh every 10s — wait for a refresh to show they're live
5. Click the **"Architecture"** tab
6. **Slowly scroll** through the full architecture diagram:
   - Layer 1: Data Ingestion (Amazon Reviews → MinIO)
   - Layer 2: Feature Engineering (Spark + RAPIDS)
   - Layer 3: Feature Store (Feast + Redis + Milvus)
   - Layer 4: Distributed Training (Kubeflow Trainer + MLflow)
   - Layer 5: Model Registry (RHOAI)
   - Layer 6: Model Serving (KServe + vLLM)
   - Layer 7: Demo Application (Gradio)
   - Platform stack summary at the bottom

### What to highlight in recording

| Element | Why it matters |
|---------|---------------|
| Green health indicators | ServiceMonitors + Prometheus provide live endpoint health — zero config |
| Embedded Grafana panels | Inference metrics (request rate, token throughput, latency) scraped automatically |
| Auto-refresh | Proves metrics are live, not static screenshots |
| Architecture diagram | Shows the full RHOAI operator stack — every layer maps to an RHOAI-managed component |
| Platform stack summary | Lists all operators: Spark, Kubeflow Trainer, Feast, KServe, GPU Operator, MLflow |

### Recording tips

- In Observability tab, wait for at least one Grafana panel refresh before moving on
- In Architecture tab, scroll **very slowly** — each layer should be visible for 3-4 seconds
- End on the platform stack summary — it's the closing statement: "all managed, all declarative"

---

## Summary: What to Highlight Per Phase

| Phase | Primary RHOAI Capability | Key Visual | "Aha" Moment |
|-------|--------------------------|------------|--------------|
| 0. Prerequisites | Installed Operators | OperatorHub list | Six operators = entire ML platform |
| 1. Platform | RHOAI Dashboard | Data Science Projects view | Single control plane for all ML |
| 2. Data | Data Connections | MinIO bucket structure | One storage layer, zero copies |
| 3. Spark ETL | Spark Operator + GPU Operator | Duration comparison | 1.34x faster, zero code changes |
| 4. GPU Metrics | DCGM + User Workload Monitoring | GPU utilization spikes | Real hardware metrics, auto-scraped |
| 5. Features | Feast Operator | Feature schema + 35M Redis keys | Same features for train and serve |
| 6. Training | Kubeflow Trainer + MLflow | Loss curve + artifact path | One-liner submits distributed training |
| 7. Serving | KServe + Model Registry | 3 endpoints READY=True | MLflow artifact → KServe, zero re-upload |
| 8. Live Demo | All components together | Pipeline traces | Every request shows RHOAI components |
| 9. Observability | ServiceMonitors + Prometheus | Live Grafana dashboards | Zero-config scraping |

---

## Through-Line for Voiceover

Every phase should connect back to this core message:

> **"The developer writes Python and YAML. Red Hat OpenShift AI handles everything else."**

- Phase 0-1: "Here's the platform"
- Phase 2-3: "Here's how it processes data at scale"
- Phase 4: "Here's how it monitors GPU hardware"
- Phase 5: "Here's how it prevents training-serving skew"
- Phase 6: "Here's how it orchestrates distributed training"
- Phase 7: "Here's how it serves models in production"
- Phase 8: "Here's what the end user experiences"
- Phase 9: "Here's how it monitors everything in production"

Close with: "Six operators. One namespace. Standard Kubernetes manifests. No vendor lock-in."
