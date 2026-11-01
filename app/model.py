"""The ML part: a scikit-learn classifier trained on the Iris dataset.
Swap this file for any other model; nothing else in the system needs to change."""
import os
from datetime import datetime, timezone

import joblib
import numpy as np
from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split


def train_and_save(path: str) -> float:
    data = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(
        data.data, data.target, test_size=0.2, random_state=42, stratify=data.target)
    clf = LogisticRegression(max_iter=500).fit(X_train, y_train)
    accuracy = float(clf.score(X_test, y_test))
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    joblib.dump({
        "model": clf,
        "labels": list(data.target_names),
        "version": datetime.now(timezone.utc).strftime("iris-logreg-%Y%m%d"),
        "test_accuracy": accuracy,
    }, path)
    return accuracy


class IrisModel:
    def __init__(self, path: str):
        if not os.path.exists(path):
            train_and_save(path)  # convenience for local runs; Docker trains at build time
        bundle = joblib.load(path)
        self.clf = bundle["model"]
        self.labels = bundle["labels"]
        self.version = bundle["version"]

    def predict(self, features: list[float]) -> dict:
        X = np.asarray([features], dtype=float)
        if not np.isfinite(X).all():
            raise ValueError("features must be finite numbers")
        probs = self.clf.predict_proba(X)[0]
        best = int(probs.argmax())
        return {
            "label": self.labels[best],
            "confidence": round(float(probs[best]), 4),
            "probabilities": {label: round(float(p), 4) for label, p in zip(self.labels, probs)},
            "model_version": self.version,
        }
