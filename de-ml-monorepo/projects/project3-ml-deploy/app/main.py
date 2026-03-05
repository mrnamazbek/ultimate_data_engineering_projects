"""FastAPI inference service: load model.pkl and serve /predict endpoint."""

import os
import pickle
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Load model once at startup
_model: Any = None


def load_model() -> None:
    """Load the trained model from disk into the module-level _model."""
    global _model
    path = Path(os.getenv("MODEL_PATH", "/models/model.pkl"))
    if path.exists():
        with open(path, "rb") as f:
            _model = pickle.load(f)
    else:
        # Allow startup even without model (returns 503 on predict)
        _model = None


@asynccontextmanager
async def lifespan(application: FastAPI):  # noqa: ARG001
    """Application lifespan: load model on startup."""
    load_model()
    yield


app = FastAPI(title="ML Model API", version="1.0.0", lifespan=lifespan)


class PredictRequest(BaseModel):
    user_id: int
    item_id: int
    hour_of_day: int = 12
    day_of_week: int = 0


class PredictResponse(BaseModel):
    score: float
    label: int


@app.get("/health")
def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok", "model_loaded": _model is not None}


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest) -> PredictResponse:
    """Return a prediction score for the given user/item pair."""
    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    features = [[req.user_id, req.item_id, req.hour_of_day, req.day_of_week]]
    proba = _model.predict_proba(features)[0][1]
    label = int(_model.predict(features)[0])
    return PredictResponse(score=float(proba), label=label)
