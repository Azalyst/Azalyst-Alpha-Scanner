"""
Emit `reports/latest_ml_scores.json` summarizing:

- model metadata (trained_ts, AUC, precision/recall, top features)
- top-N most confident ml_scores from the last 24h
- top-K mined patterns by lift

The dashboard reads this file to display the ML health chip and the
pattern library. Keeping it a single JSON keeps dashboard.html static.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

REPORT_PATH = Path("reports/latest_ml_scores.json")
TOP_SIGNAL_LIMIT = 25
TOP_PATTERN_LIMIT = 15
WINDOW_HOURS = 24


def export(db_path: Path | str) -> Dict[str, Any]:
    metrics_path = Path("ml/metrics.json")
    metrics = json.loads(metrics_path.read_text()) if metrics_path.exists() else {"status": "no_metrics"}

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)).isoformat()
        top_signals = [
            dict(r) for r in conn.execute(
                """
                SELECT ms.snapshot_id, ms.ts, ms.chain, ms.address, ms.symbol,
                       ms.ml_prob, ms.ml_direction,
                       sig.pump_score, sig.dump_score, sig.anomaly_score, sig.label
                  FROM ml_scores ms
                  LEFT JOIN signals sig ON sig.snapshot_id = ms.snapshot_id
                 WHERE ms.ts >= ?
                 ORDER BY ms.ml_prob DESC
                 LIMIT ?
                """,
                (since, TOP_SIGNAL_LIMIT),
            ).fetchall()
        ]

        top_patterns = [
            dict(r) for r in conn.execute(
                """
                SELECT pattern_id, prefix_json, length, support,
                       lift, positive_rate, horizon_min
                  FROM pattern_library
                 ORDER BY lift DESC
                 LIMIT ?
                """,
                (TOP_PATTERN_LIMIT,),
            ).fetchall()
        ]
        for p in top_patterns:
            p["prefix"] = json.loads(p.pop("prefix_json", "[]"))

        cluster_counts = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT cluster, COUNT(*) FROM wallet_clusters GROUP BY cluster"
            ).fetchall()
        }

        totals = conn.execute(
            """
            SELECT (SELECT COUNT(*) FROM token_snapshots) AS snapshots,
                   (SELECT COUNT(*) FROM wallet_events) AS events,
                   (SELECT COUNT(*) FROM signal_outcomes WHERE is_true IS NOT NULL) AS labeled
            """
        ).fetchone()

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "model_metrics": metrics,
            "cluster_counts": cluster_counts,
            "totals": {
                "snapshots": totals["snapshots"] if totals else 0,
                "wallet_events": totals["events"] if totals else 0,
                "labeled_outcomes": totals["labeled"] if totals else 0,
            },
            "top_signals": top_signals,
            "top_patterns": top_patterns,
        }
    finally:
        conn.close()

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(payload, indent=2, default=str))
    return {"status": "ok", "signals": len(top_signals), "patterns": len(top_patterns)}


if __name__ == "__main__":
    import sys
    db = sys.argv[1] if len(sys.argv) > 1 else "data/birdeye_quant.db"
    print(json.dumps(export(db), indent=2))
