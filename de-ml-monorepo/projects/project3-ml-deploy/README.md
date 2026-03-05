# Project 3 – ML Deploy (FastAPI + Docker + K8s)

## Goal
Train a simple recommendation model and serve predictions via FastAPI.
Includes Docker packaging and Kubernetes manifests for deployment.

## Stack
- **Python 3.10** + scikit-learn (LogisticRegression)
- **FastAPI 0.111** + uvicorn – REST API
- **Docker** – containerisation
- **Kubernetes** manifests (minikube / kind compatible)

## How to run

### Train the model
```bash
docker-compose run --rm trainer
# model saved to Docker volume: model_data
```

### Start the API
```bash
docker-compose up --build model-api
# API available at http://localhost:8000/docs
```

### Health check
```bash
curl http://localhost:8000/health
# {"status":"ok","model_loaded":true}
```

### Predict
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "item_id": 50, "hour_of_day": 14, "day_of_week": 3}'
# {"score": 0.62, "label": 1}
```

## Deploy to Kubernetes (minikube / kind)

```bash
# Build and load image
docker build -t model-api:latest app/
minikube image load model-api:latest   # or: kind load docker-image model-api:latest

# Apply manifests
kubectl apply -f k8s/
kubectl get pods -l app=model-api
```

## Run tests
```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Trade-offs
- **LogisticRegression** is a placeholder; swap `train.py` for any
  sklearn-compatible estimator without changing the API.
- **model.pkl**: simple pickle is fine for dev; use MLflow model registry or
  ONNX for production versioning.
- **K8s HPA**: optional `hpa.yaml` scales on CPU; for ML workloads, consider
  custom metrics (requests/s, queue depth).
- **No auth**: add OAuth2 / API key middleware before exposing publicly.
