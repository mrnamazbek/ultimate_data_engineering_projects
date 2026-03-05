"""Trainer: generate synthetic data and train a simple model → model.pkl."""

import os
import pickle
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression

MODEL_PATH = Path(os.getenv("MODEL_PATH", "/models/model.pkl"))


def generate_data(n_samples: int = 5000) -> tuple[np.ndarray, np.ndarray]:
    """Create synthetic user-item interaction data."""
    rng = np.random.default_rng(42)
    # Features: [user_id, item_id, hour_of_day, day_of_week]
    X = rng.integers(low=0, high=[1000, 500, 24, 7], size=(n_samples, 4)).astype(float)
    # Label: 1 = liked, 0 = not liked (simple rule with noise)
    y = ((X[:, 0] % 3 + X[:, 1] % 2 + rng.integers(0, 2, n_samples)) > 2).astype(int)
    return X, y


def train(X: np.ndarray, y: np.ndarray) -> LogisticRegression:
    """Train a logistic regression classifier."""
    model = LogisticRegression(max_iter=200, random_state=42)
    model.fit(X, y)
    return model


def save(model: LogisticRegression, path: Path) -> None:
    """Persist model to disk as a pickle file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(model, f)
    print(f"Model saved to {path}")


if __name__ == "__main__":
    X, y = generate_data()
    model = train(X, y)
    save(model, MODEL_PATH)
