"""Unit tests for the FastAPI inference service."""

import sys
import os

import pytest
from fastapi.testclient import TestClient

# Insert app directory into path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "trainer"))


@pytest.fixture(scope="session")
def trained_model_path(tmp_path_factory):
    """Train a small model and save it to a temp file."""
    from train import generate_data, train, save

    X, y = generate_data(500)
    model = train(X, y)
    path = tmp_path_factory.mktemp("models") / "model.pkl"
    save(model, path)
    return path


@pytest.fixture()
def client(trained_model_path, monkeypatch):
    """Return a TestClient with the model loaded via lifespan."""
    monkeypatch.setenv("MODEL_PATH", str(trained_model_path))
    import importlib
    import main as app_module

    importlib.reload(app_module)
    with TestClient(app_module.app) as tc:
        yield tc


def test_health_ok(client):
    """GET /health should return 200 and model_loaded=true."""
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["model_loaded"] is True


def test_predict_returns_score(client):
    """POST /predict should return a float score between 0 and 1."""
    payload = {"user_id": 1, "item_id": 50, "hour_of_day": 10, "day_of_week": 2}
    resp = client.post("/predict", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert 0.0 <= data["score"] <= 1.0
    assert data["label"] in (0, 1)


def test_predict_missing_model(monkeypatch, tmp_path):
    """POST /predict should return 503 when model is not loaded."""
    monkeypatch.setenv("MODEL_PATH", str(tmp_path / "nonexistent.pkl"))
    import importlib
    import main as app_module

    importlib.reload(app_module)
    with TestClient(app_module.app, raise_server_exceptions=False) as tc:
        resp = tc.post("/predict", json={"user_id": 1, "item_id": 1})
    assert resp.status_code == 503
