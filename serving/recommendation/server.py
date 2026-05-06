"""KServe-compatible recommendation server.

Loads the Two-Tower model with ID mappings from the training checkpoint
and serves real-time product recommendations. Enriches results with
product metadata (title, brand, category, rating, price) from Feast.

Endpoints:
    POST /v1/models/smartshop-rec:predict
    Body: {"user_id": "...", "candidate_items": ["ASIN1", "ASIN2", ...], "top_k": 10}
    Response: {"recommendations": [{"item_id": "...", "score": 0.95, "title": "...", ...}, ...]}
"""

import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import torch
import torch.nn as nn
from fastapi import FastAPI
from prometheus_client import Counter, Histogram, make_asgi_app
from pydantic import BaseModel

REQUEST_DURATION = Histogram(
    "smartshop_rec_request_duration_seconds", "Predict latency",
    buckets=[.005, .01, .025, .05, .1, .25, .5, 1, 2.5],
)
REQUESTS_TOTAL = Counter(
    "smartshop_rec_requests_total", "Total prediction requests", ["status"],
)
CANDIDATES_SCORED = Histogram(
    "smartshop_rec_candidates_scored", "Items scored per request",
    buckets=[10, 25, 50, 100, 200, 500],
)


class TwoTower(nn.Module):
    """Matches the architecture produced by 01_training_rec.ipynb."""

    def __init__(self, n_users: int, n_items: int, embed_dim: int = 64, hidden_dim: int = 256):
        super().__init__()
        self.user_embed = nn.Embedding(n_users, embed_dim)
        self.item_embed = nn.Embedding(n_items, embed_dim)
        self.user_mlp = nn.Sequential(
            nn.Linear(embed_dim, hidden_dim), nn.ReLU(), nn.Dropout(0.2),
            nn.Linear(hidden_dim, embed_dim),
        )
        self.item_mlp = nn.Sequential(
            nn.Linear(embed_dim, hidden_dim), nn.ReLU(), nn.Dropout(0.2),
            nn.Linear(hidden_dim, embed_dim),
        )

    def forward(self, users: torch.Tensor, items: torch.Tensor) -> torch.Tensor:
        u = self.user_mlp(self.user_embed(users))
        i = self.item_mlp(self.item_embed(items))
        return (u * i).sum(dim=1)


_model: Optional[TwoTower] = None
_user_to_idx: dict = {}
_item_to_idx: dict = {}
_idx_to_item: dict = {}
_all_item_ids: list = []
_all_item_embeddings: Optional[torch.Tensor] = None
_all_item_indices: Optional[torch.Tensor] = None
_feast_store = None

ITEM_FEATURES = [
    "item_metadata:item_title",
    "item_metadata:item_brand",
    "item_metadata:item_category",
    "item_features:item_avg_rating",
    "item_metadata:item_price",
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _model, _user_to_idx, _item_to_idx, _idx_to_item, _all_item_ids
    global _all_item_embeddings, _all_item_indices, _feast_store

    feast_config = os.environ.get("FEAST_CONFIG_PATH")
    if feast_config:
        try:
            from feast import FeatureStore
            from feast.repo_config import load_repo_config
            import pathlib

            cfg_path = pathlib.Path(feast_config)
            repo_config = load_repo_config(
                repo_path=str(cfg_path.parent), fs_yaml_file=str(cfg_path),
            )
            _feast_store = FeatureStore(config=repo_config)
            print(f"Feast connected: {feast_config}")
        except Exception as e:
            print(f"Feast init failed ({e}), serving without enrichment")
            _feast_store = None
    else:
        print("No FEAST_CONFIG_PATH set, serving without product metadata enrichment")

    model_path = os.environ.get("MODEL_PATH", "models/recommendation/best_model.pt")

    if model_path.startswith("s3://"):
        import fsspec
        fs, _ = fsspec.core.url_to_fs(
            model_path,
            endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3"),
        )
        if fs.isdir(model_path):
            model_path = model_path.rstrip("/") + "/best_model.pt"
        local_path = "/tmp/best_model.pt"
        print(f"Downloading {model_path} → {local_path}")
        fs.get(model_path, local_path)
        model_path = local_path

    checkpoint = torch.load(model_path, map_location="cpu", weights_only=False)

    if isinstance(checkpoint, dict) and "model_state_dict" in checkpoint:
        state_dict = checkpoint["model_state_dict"]
        n_users = checkpoint["n_users"]
        n_items = checkpoint["n_items"]
        embed_dim = checkpoint.get("embed_dim", 64)
        hidden_dim = checkpoint.get("hidden_dim", 256)
        _user_to_idx = checkpoint.get("user_to_idx", {})
        _item_to_idx = checkpoint.get("item_to_idx", {})
    else:
        state_dict = checkpoint
        n_users = state_dict["user_embed.weight"].shape[0]
        n_items = state_dict["item_embed.weight"].shape[0]
        embed_dim = state_dict["user_embed.weight"].shape[1]
        hidden_dim = state_dict["user_mlp.0.weight"].shape[0]

    _idx_to_item = {v: k for k, v in _item_to_idx.items()}
    _all_item_ids = list(_item_to_idx.keys())

    _model = TwoTower(n_users, n_items, embed_dim, hidden_dim)
    _model.load_state_dict(state_dict)
    _model.eval()

    with torch.no_grad():
        _all_item_indices = torch.arange(n_items, dtype=torch.long)
        _all_item_embeddings = _model.item_mlp(_model.item_embed(_all_item_indices))

    print(f"Model loaded: {n_users} users, {n_items} items, "
          f"embed_dim={embed_dim}, hidden_dim={hidden_dim}, "
          f"mappings={'yes' if _user_to_idx else 'no'}, "
          f"item_embeddings_precomputed={_all_item_embeddings.shape}")
    yield


app = FastAPI(title="SmartShop Recommendation Service", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


class RecommendRequest(BaseModel):
    user_id: str
    candidate_items: list[str] = []
    top_k: int = 10


class RecommendResponse(BaseModel):
    recommendations: list[dict]


def _enrich_with_feast(results: list[dict]) -> list[dict]:
    """Look up product metadata from Feast for recommended item_ids."""
    if not _feast_store or not results:
        return results
    try:
        entity_rows = [{"item_id": r["item_id"]} for r in results]
        features = _feast_store.get_online_features(
            features=ITEM_FEATURES, entity_rows=entity_rows,
        ).to_dict()
        for i, rec in enumerate(results):
            rec["title"] = features.get("item_title", [None] * len(results))[i] or ""
            rec["brand"] = features.get("item_brand", [None] * len(results))[i] or ""
            rec["category"] = features.get("item_category", [None] * len(results))[i] or ""
            rec["avg_rating"] = features.get("item_avg_rating", [None] * len(results))[i]
            price = features.get("item_price", [None] * len(results))[i]
            rec["price"] = round(price, 2) if price else None
    except Exception as e:
        print(f"Feast enrichment failed: {e}")
    return results


@app.post("/v1/models/smartshop-rec:predict", response_model=RecommendResponse)
def predict(request: RecommendRequest):
    t0 = time.perf_counter()
    try:
        if _user_to_idx:
            user_idx = _user_to_idx.get(request.user_id, 0)
        else:
            user_idx = hash(request.user_id) % _model.user_embed.num_embeddings

        with torch.no_grad():
            user_t = torch.tensor([user_idx], dtype=torch.long)
            user_emb = _model.user_mlp(_model.user_embed(user_t))

            if request.candidate_items:
                idxs = [_item_to_idx.get(iid, 0) for iid in request.candidate_items]
                item_t = torch.tensor(idxs, dtype=torch.long)
                item_embs = _model.item_mlp(_model.item_embed(item_t))
                scores_t = torch.sigmoid((user_emb * item_embs).sum(dim=1))
                item_ids = request.candidate_items
            else:
                scores_t = torch.sigmoid((user_emb * _all_item_embeddings).sum(dim=1))
                item_ids = None

            top_k = min(request.top_k, scores_t.shape[0])
            top_scores, top_indices = torch.topk(scores_t, top_k)

        raw_results = []
        for score, idx in zip(top_scores.tolist(), top_indices.tolist()):
            iid = item_ids[idx] if item_ids else _idx_to_item.get(idx, str(idx))
            raw_results.append({"item_id": iid, "score": round(score, 4)})

        results = _enrich_with_feast(raw_results)

        CANDIDATES_SCORED.observe(scores_t.shape[0])
        REQUESTS_TOTAL.labels(status="ok").inc()
        return RecommendResponse(recommendations=results)
    except Exception as e:
        REQUESTS_TOTAL.labels(status="error").inc()
        raise e
    finally:
        REQUEST_DURATION.observe(time.perf_counter() - t0)


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "model_loaded": _model is not None,
        "has_mappings": bool(_user_to_idx),
    }
