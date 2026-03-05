"""Unit tests for the trainer module."""

import sys
import os

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "trainer"))


def test_generate_data_shape():
    """generate_data should return arrays of expected shape."""
    from train import generate_data

    X, y = generate_data(100)
    assert X.shape == (100, 4)
    assert y.shape == (100,)


def test_generate_data_label_values():
    """Labels must be binary (0 or 1)."""
    from train import generate_data

    _, y = generate_data(200)
    assert set(np.unique(y)).issubset({0, 1})


def test_train_model_predicts():
    """Trained model should predict labels for new data."""
    from train import generate_data, train

    X, y = generate_data(200)
    model = train(X, y)
    preds = model.predict(X[:5])
    assert len(preds) == 5


def test_save_and_load(tmp_path):
    """Saved model pickle should be loadable and produce predictions."""
    import pickle
    from train import generate_data, train, save

    X, y = generate_data(200)
    model = train(X, y)
    path = tmp_path / "model.pkl"
    save(model, path)

    with open(path, "rb") as f:
        loaded = pickle.load(f)
    preds = loaded.predict(X[:3])
    assert len(preds) == 3
