"""
Train a supervised classifier on the feature matrix.

Default: LightGBM (fast, small artifact). Falls back to
sklearn GradientBoostingClassifier if LightGBM isn't installed.

Refuses to train on fewer than `MIN_SAMPLES` labeled rows; the cold-start
is expected for the first few days and should not fail the workflow.
"""
from __future__ import annotations

import json
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from ml.features import build_matrix

MIN_SAMPLES = 50
MODEL_PATH = Path("ml/model.pkl")
METRICS_PATH = Path("ml/metrics.json")


def train(db_path: Path | str) -> Dict[str, Any]:
    import numpy as np

    df, y, _ids, feature_names = build_matrix(db_path, labeled_only=True)
    labeled = (y != -1)
    df = df[labeled]
    y = y[labeled]

    if len(df) < MIN_SAMPLES:
        metrics = {
            "status": "insufficient_data",
            "labeled_rows": int(len(df)),
            "required": MIN_SAMPLES,
            "trained_ts": datetime.now(timezone.utc).isoformat(),
        }
        METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
        METRICS_PATH.write_text(json.dumps(metrics, indent=2))
        return metrics

    from sklearn.model_selection import train_test_split
    from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score

    pos_rate = float((y == 1).mean())
    stratify = y if 0 < pos_rate < 1 else None
    X_train, X_val, y_train, y_val = train_test_split(
        df.values, y, test_size=0.2, random_state=42, stratify=stratify
    )

    model, model_kind = _build_model(y_train)
    model.fit(X_train, y_train)

    y_prob = model.predict_proba(X_val)[:, 1]
    y_pred = (y_prob >= 0.5).astype(int)

    metrics = {
        "status": "ok",
        "model": model_kind,
        "trained_ts": datetime.now(timezone.utc).isoformat(),
        "labeled_rows": int(len(df)),
        "positive_rate": pos_rate,
        "roc_auc": _safe_auc(y_val, y_prob),
        "precision": float(precision_score(y_val, y_pred, zero_division=0)),
        "recall": float(recall_score(y_val, y_pred, zero_division=0)),
        "f1": float(f1_score(y_val, y_pred, zero_division=0)),
        "feature_count": len(feature_names),
        "top_features": _top_features(model, feature_names, k=20),
    }

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MODEL_PATH.open("wb") as f:
        pickle.dump({"model": model, "feature_names": feature_names,
                     "model_kind": model_kind, "trained_ts": metrics["trained_ts"]}, f)
    METRICS_PATH.write_text(json.dumps(metrics, indent=2))
    return metrics


def _build_model(y_train):
    try:
        import lightgbm as lgb
        return lgb.LGBMClassifier(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=-1,
            num_leaves=31,
            min_child_samples=5,
            reg_lambda=1.0,
            class_weight="balanced",
            verbosity=-1,
        ), "lightgbm"
    except ImportError:
        from sklearn.ensemble import GradientBoostingClassifier
        return GradientBoostingClassifier(n_estimators=200, learning_rate=0.05, max_depth=3), "sklearn_gbt"


def _safe_auc(y_true, y_prob) -> float:
    from sklearn.metrics import roc_auc_score
    try:
        if len(set(y_true)) < 2:
            return 0.0
        return float(roc_auc_score(y_true, y_prob))
    except Exception:
        return 0.0


def _top_features(model, names, k: int):
    try:
        if hasattr(model, "feature_importances_"):
            imps = model.feature_importances_
        else:
            return []
        pairs = sorted(zip(names, imps), key=lambda p: -float(p[1]))[:k]
        return [{"name": n, "importance": float(v)} for n, v in pairs]
    except Exception:
        return []


if __name__ == "__main__":
    import sys
    m = train(sys.argv[1] if len(sys.argv) > 1 else "data/birdeye_quant.db")
    print(json.dumps(m, indent=2))
