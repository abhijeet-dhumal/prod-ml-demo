#!/usr/bin/env bash
# apply-all.sh — Full cluster state reproduction for SmartShop AI demo.
#
# Applies every manifest in dependency order from a clean namespace.
# Idempotent: safe to re-run after partial failures.
#
# USAGE:
#   set -a; source .env; set +a
#   bash scripts/apply-all.sh [PHASE]
#
# PHASES (run individually or all at once):
#   infra         Namespace, RBAC, secrets, storage, monitoring
#   images        BuildConfigs + ImageStreams (triggers builds)
#   data          Data download job (HuggingFace → MinIO)
#   observability Grafana, redis_exporter, Spark metrics ConfigMap, collect script
#   spark         Submit all 3 Spark ETL jobs (feature engineering, text preprocessing)
#   feast         feast apply inside pod + redis secret patch
#   training      TrainingRuntime + submit rec TrainJob (after spark completes)
#   serving       Apply all 3 InferenceServices
#   notebook      Create notebook ConfigMap + submit papermill runner job
#   all           Everything above in order (default)
#
# PREREQUISITES:
#   oc login <cluster>
#   All vars in .env filled (especially QUAY_TOKEN, HF_TOKEN, PROMETHEUS_TOKEN)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PHASE="${1:-all}"

source "$REPO_ROOT/.env" 2>/dev/null || true

log() { echo ""; echo "══ $* ══════════════════════════════════════════════════"; }
ok()  { echo "  ✅ $*"; }
run() { echo "  → $*"; eval "$*"; }

render() {
  python3 "$REPO_ROOT/scripts/render_yaml.py" "$1" "${@:2}"
}

apply() {
  render "$@" | oc apply -f -
}

# ── Phase: infra ──────────────────────────────────────────────────────────────
phase_infra() {
  log "Phase: infra"

  # Namespace (idempotent)
  oc get namespace "$NAMESPACE" &>/dev/null || oc create namespace "$NAMESPACE"
  ok "namespace $NAMESPACE"

  # Secrets — smartshop-credentials
  oc create secret generic smartshop-credentials \
    --from-literal=AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
    --from-literal=AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION" \
    --from-literal=REDIS_HOST="$REDIS_HOST" \
    --from-literal=REDIS_PORT="$REDIS_PORT" \
    --from-literal=REDIS_PASSWORD="$REDIS_PASSWORD" \
    --from-literal=MLFLOW_TRACKING_URI="$MLFLOW_TRACKING_URI" \
    --from-literal=MINIO_ENDPOINT="$MINIO_ENDPOINT" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "secret smartshop-credentials"

  # HuggingFace secret
  oc create secret generic hf-credentials \
    --from-literal=token="$HF_TOKEN" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "secret hf-credentials"

  # Quay push secret
  oc create secret docker-registry "$QUAY_PUSH_SECRET" \
    --docker-server=quay.io \
    --docker-username="$QUAY_USER" \
    --docker-password="$QUAY_TOKEN" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "secret $QUAY_PUSH_SECRET"

  # Feast Redis secret (used by Feast operator)
  oc create secret generic feast-redis-secret \
    --from-literal=redis="type: redis
connection_string: \"${REDIS_HOST}:${REDIS_PORT},password=${REDIS_PASSWORD}\"" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "secret feast-redis-secret"

  # Feast S3 credentials
  oc create secret generic feast-s3-credentials \
    --from-literal=AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
    --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
    --from-literal=AWS_ENDPOINT_URL_S3="$MINIO_ENDPOINT" \
    --from-literal=AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "secret feast-s3-credentials"

  # Spark RBAC (ServiceAccount + Role + RoleBinding)
  oc create serviceaccount spark -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  oc create role spark-role \
    --verb=create,get,list,watch,delete,patch,update,deletecollection \
    --resource=pods,services,configmaps,persistentvolumeclaims,endpoints \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  oc create role spark-role-logs \
    --verb=get,list,watch \
    --resource=pods/log,pods/exec \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  oc create rolebinding spark-role-binding \
    --role=spark-role --serviceaccount="$NAMESPACE:spark" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  oc create rolebinding spark-role-logs-binding \
    --role=spark-role-logs --serviceaccount="$NAMESPACE:spark" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "spark RBAC"

  # User-workload monitoring (cluster-admin required, once)
  apply "$REPO_ROOT/infrastructure/openshift/user-workload-monitoring.yaml" || \
    echo "  ⚠ user-workload-monitoring: needs cluster-admin — skip if already enabled"
}

# ── Phase: images ─────────────────────────────────────────────────────────────
phase_images() {
  log "Phase: images"
  apply "$REPO_ROOT/infrastructure/openshift/imagestreams.yaml"
  ok "ImageStreams"

  apply "$REPO_ROOT/infrastructure/openshift/buildconfigs.yaml"
  ok "BuildConfigs applied — builds auto-start from git"
  echo "  Monitor: oc get builds -n $NAMESPACE -w"
  echo "  All 4 must Complete before proceeding: spark-jobs, spark-jobs-rapids, rec-trainer, llm-trainer"
}

# ── Phase: data ───────────────────────────────────────────────────────────────
phase_data() {
  log "Phase: data"
  # Upload S3A JARs to MinIO (required by Spark before ETL)
  apply "$REPO_ROOT/infrastructure/openshift/upload-spark-jars-job.yaml"
  ok "upload-spark-jars job submitted"
  echo "  Wait: oc wait job/upload-spark-jars -n $NAMESPACE --for=condition=Complete --timeout=600s"

  # Full dataset download (HuggingFace → MinIO sharded parquet)
  apply "$REPO_ROOT/infrastructure/openshift/data-download-job.yaml"
  ok "data-download job submitted"
  echo "  Wait: oc logs -n $NAMESPACE job/smartshop-data-download-full -f"
}

# ── Phase: observability ──────────────────────────────────────────────────────
phase_observability() {
  log "Phase: observability"

  apply "$REPO_ROOT/infrastructure/openshift/spark-metrics-configmap.yaml"
  ok "spark-metrics-config ConfigMap"

  apply "$REPO_ROOT/infrastructure/openshift/redis-exporter.yaml"
  ok "redis-exporter Deployment + ServiceMonitor"

  apply "$REPO_ROOT/infrastructure/openshift/grafana.yaml"
  ok "Grafana Deployment + route"
  echo "  URL: https://grafana-$NAMESPACE.apps.$OC_CLUSTER_DOMAIN"

  # Embed collect script into ConfigMap
  oc create configmap smartshop-collect-script \
    --from-file=collect-run-metrics.sh="$REPO_ROOT/scripts/collect-run-metrics.sh" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "smartshop-collect-script ConfigMap (collect-run-metrics.sh)"
}

# ── Phase: spark ──────────────────────────────────────────────────────────────
phase_spark() {
  log "Phase: spark"

  # Refresh script ConfigMaps
  oc create configmap smartshop-feature-engineering-script \
    --from-file=feature_engineering.py="$REPO_ROOT/spark/feature_engineering.py" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "feature_engineering script ConfigMap"

  oc create configmap smartshop-text-preprocessing-script \
    --from-file=text_preprocessing.py="$REPO_ROOT/spark/text_preprocessing.py" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "text_preprocessing script ConfigMap"

  oc create configmap smartshop-embedding-script \
    --from-file=embedding_generation.py="$REPO_ROOT/spark/embedding_generation.py" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "embedding_generation script ConfigMap"

  # GPU discovery script
  oc create configmap smartshop-gpu-discovery-script \
    --from-literal=getGpusResources.sh='#!/usr/bin/env bash
echo "[{\"name\": \"gpu\", \"addresses\": [\"$(nvidia-smi --query-gpu=uuid --format=csv,noheader | head -1)\"]}]"' \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "GPU discovery script ConfigMap"

  # RAPIDS GPU job (8 executors, ~140M rows — sharded dirs only)
  render "$REPO_ROOT/infrastructure/openshift/spark-application-rapids.yaml" \
    SPARK_EXECUTOR_INSTANCES=8 SPARK_EXECUTOR_CORES=4 \
    SPARK_EXECUTOR_MEMORY=16g SPARK_DRIVER_MEMORY=8g | oc apply -f -
  ok "RAPIDS SparkApp submitted (8 executors, full 140M row dataset)"

  # CPU baseline (8 executors — apples-to-apples comparison)
  render "$REPO_ROOT/infrastructure/openshift/spark-application-cpu-baseline.yaml" \
    SPARK_EXECUTOR_INSTANCES=8 SPARK_EXECUTOR_CORES=4 \
    SPARK_EXECUTOR_MEMORY=16g SPARK_DRIVER_MEMORY=8g | oc apply -f -
  ok "CPU baseline SparkApp submitted (8 executors)"

  # Text preprocessing (4 executors)
  render "$REPO_ROOT/infrastructure/openshift/spark-application-text-preprocessing.yaml" \
    SPARK_EXECUTOR_INSTANCES=4 SPARK_EXECUTOR_CORES=4 \
    SPARK_EXECUTOR_MEMORY=8g | oc apply -f -
  ok "Text preprocessing SparkApp submitted"

  echo ""
  echo "  Monitor all jobs: oc get sparkapplication -n $NAMESPACE -w"
  echo "  Get metrics after completion:"
  echo "    oc logs -n $NAMESPACE smartshop-feature-engineering-rapids-driver | grep METRIC"
  echo "    oc logs -n $NAMESPACE smartshop-feature-engineering-cpu-baseline-driver | grep METRIC"
}

# ── Phase: feast ──────────────────────────────────────────────────────────────
phase_feast() {
  log "Phase: feast"

  # Patch Redis secret with correct password (Feast operator sets it empty by default)
  REDIS_CONN_B64=$(python3 -c "
import base64
s = 'type: redis\nconnection_string: \"${REDIS_HOST}:${REDIS_PORT},password=${REDIS_PASSWORD}\"'
print(base64.b64encode(s.encode()).decode())
")
  oc patch secret feast-redis-secret -n "$NAMESPACE" \
    --type='json' \
    -p="[{\"op\":\"replace\",\"path\":\"/data/redis\",\"value\":\"$REDIS_CONN_B64\"}]"
  ok "feast-redis-secret patched with Redis password"

  # Copy updated features.py and run feast apply
  FEAST_POD=$(oc get pod -n "$NAMESPACE" -l "feast.dev/name=smartshop-feast" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  if [[ -z "$FEAST_POD" ]]; then
    echo "  ERROR: feast pod not found — is the FeatureStore CR applied?"
    exit 1
  fi

  oc cp "$REPO_ROOT/feast/feature_repo/features.py" \
    "$NAMESPACE/$FEAST_POD:/feast-data/smartshop/feast/feature_repo/features.py" \
    -c online
  ok "features.py synced to feast pod"

  oc exec -n "$NAMESPACE" "$FEAST_POD" -c online -- \
    feast -c /feast-data/smartshop/feast/feature_repo apply
  ok "feast apply — feature views registered"

  echo ""
  echo "  ⚠  Run feast materialize AFTER Spark ETL jobs complete:"
  echo "  bash scripts/wait-and-materialize.sh"
  echo "  OR manually: oc exec -n $NAMESPACE $FEAST_POD -c online -- \\"
  echo "    feast -c /feast-data/smartshop/feast/feature_repo materialize-incremental \$(date -u +%Y-%m-%dT%H:%M:%S)"
}

# ── Phase: training ───────────────────────────────────────────────────────────
phase_training() {
  log "Phase: training"

  # Apply TrainingRuntime + ClusterTrainingRuntime (without TrainJobs)
  python3 -c "
content = open('$REPO_ROOT/infrastructure/openshift/trainjobs.yaml').read()
import re, os
# Load env
env = {}
for line in open('$REPO_ROOT/.env'):
    line = line.strip()
    if line and not line.startswith('#') and '=' in line:
        k, _, v = line.partition('=')
        env[k.strip()] = v.strip().strip('\"').strip(\"'\")

def render(tmpl):
    result = re.sub(r'\\\$\{(\w+)(?::-(.*?))?\}',
                    lambda m: env.get(m.group(1), m.group(2) or m.group(0)), tmpl)
    return re.sub(r'\\\$(\w+)', lambda m: env.get(m.group(1), m.group(0)), result)

for doc in content.split('---'):
    doc = doc.strip()
    if 'TrainingRuntime' in doc and 'kind: TrainJob' not in doc:
        print('---'); print(render(doc))
" | oc apply -f -
  ok "TrainingRuntime + ClusterTrainingRuntime applied"

  # Render and save rec TrainJob for manual submission after Spark completes
  render "$REPO_ROOT/infrastructure/openshift/trainjobs.yaml" \
    > /tmp/trainjobs-rendered.yaml
  python3 -c "
content = open('/tmp/trainjobs-rendered.yaml').read()
for doc in content.split('---'):
    if 'kind: TrainJob' in doc and 'smartshop-rec-train' in doc:
        print('---'); print(doc.strip())
" > /tmp/rec-trainjob.yaml
  ok "rec-trainjob.yaml saved to /tmp/rec-trainjob.yaml"

  echo ""
  echo "  ⚠  Submit TrainJob AFTER feast materialize completes:"
  echo "    oc apply -f /tmp/rec-trainjob.yaml"
  echo "    oc get trainjob smartshop-rec-train -n $NAMESPACE -w"
}

# ── Phase: serving ────────────────────────────────────────────────────────────
phase_serving() {
  log "Phase: serving"
  apply "$REPO_ROOT/infrastructure/openshift/inferenceservices.yaml"
  ok "InferenceServices applied"
  echo "  Monitor: oc get inferenceservice -n $NAMESPACE -w"
  echo "  Ready when READY=True for all 3: smartshop-rec, smartshop-llm, smartshop-rag"
}

# ── Phase: notebook ───────────────────────────────────────────────────────────
phase_notebook() {
  log "Phase: notebook"

  # Create/update notebook ConfigMap (< 1MB — fits fine)
  oc create configmap smartshop-metrics-notebook \
    --from-file=metrics_analysis.ipynb="$REPO_ROOT/notebooks/metrics_analysis.ipynb" \
    -n "$NAMESPACE" --dry-run=client -o yaml | oc apply -f -
  ok "smartshop-metrics-notebook ConfigMap"

  # Delete previous run if exists (Job names must be unique)
  oc delete job smartshop-notebook-runner -n "$NAMESPACE" --ignore-not-found=true

  # Submit papermill job
  render "$REPO_ROOT/infrastructure/openshift/notebook-runner-job.yaml" | oc apply -f -
  ok "notebook-runner Job submitted"

  echo ""
  echo "  Watch: oc logs -n $NAMESPACE job/smartshop-notebook-runner -f"
  echo "  Outputs uploaded to: s3://smartshop-models/notebooks/"
  echo "  Download:"
  echo "    aws s3 cp s3://smartshop-models/notebooks/ ./notebooks/output/ \\"
  echo "      --recursive --endpoint-url $MINIO_ENDPOINT_EXTERNAL"
}

# ── Main ──────────────────────────────────────────────────────────────────────
case "$PHASE" in
  infra)         phase_infra ;;
  images)        phase_images ;;
  data)          phase_data ;;
  observability) phase_observability ;;
  spark)         phase_spark ;;
  feast)         phase_feast ;;
  training)      phase_training ;;
  serving)       phase_serving ;;
  notebook)      phase_notebook ;;
  all)
    phase_infra
    phase_images
    phase_data
    phase_observability
    phase_spark
    phase_feast
    phase_training
    phase_serving
    phase_notebook
    ;;
  *)
    echo "Unknown phase: $PHASE"
    echo "Valid: infra images data observability spark feast training serving notebook all"
    exit 1
    ;;
esac

echo ""
echo "✅  Phase '$PHASE' complete."
