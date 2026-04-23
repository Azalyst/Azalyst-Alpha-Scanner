"""
Feature matrix builder.

One row per `token_snapshots.id`. Features:

- Token metrics (price changes, liquidity, holders, concentration)
- Existing heuristic scores from the signals table
- Trade-aggs whale/retail ratios
- Cluster-level counts inside a 30-min lookback window per snapshot
- Binary indicators for top mined patterns matching this snapshot

Label: `signal_outcomes.is_true` (only rows with a label are kept
for training).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

CLUSTERS = ("whale", "smart_money", "sniper", "mm", "anonymous")
ACTIONS = ("buy", "sell")
LOOKBACK_MIN = 30


def build_matrix(db_path: Path | str, labeled_only: bool = True):
    import numpy as np
    import pandas as pd

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = _fetch_base(conn, labeled_only)
        if not rows:
            return pd.DataFrame(), np.array([]), [], []

        pattern_ids = [r[0] for r in conn.execute(
            "SELECT pattern_id FROM pattern_library ORDER BY pattern_id"
        ).fetchall()]
        pattern_match_map: Dict[int, set] = {}
        for snap_id, pid in conn.execute(
            "SELECT snapshot_id, pattern_id FROM pattern_matches"
        ).fetchall():
            pattern_match_map.setdefault(snap_id, set()).add(pid)

        cluster_counts = _cluster_counts(conn)

        feature_rows = []
        labels = []
        snapshot_ids = []
        for r in rows:
            feats = _row_features(r, cluster_counts.get(r["id"], {}), pattern_match_map.get(r["id"], set()), pattern_ids)
            feature_rows.append(feats)
            labels.append(int(r["is_true"]) if r["is_true"] is not None else -1)
            snapshot_ids.append(r["id"])

        df = pd.DataFrame(feature_rows)
        y = np.array(labels)
        feature_names = list(df.columns)
        return df, y, snapshot_ids, feature_names
    finally:
        conn.close()


def _fetch_base(conn: sqlite3.Connection, labeled_only: bool):
    where = "WHERE o.is_true IS NOT NULL" if labeled_only else ""
    return conn.execute(
        f"""
        SELECT s.id, s.ts, s.chain, s.address, s.symbol,
               s.price, s.liquidity_usd, s.market_cap,
               s.volume_5m_usd, s.volume_1h_usd, s.volume_24h_usd,
               s.price_change_5m_pct, s.price_change_1h_pct, s.price_change_24h_pct,
               s.holder_count, s.holder_change_24h, s.top10_holder_pct,
               s.is_mintable, s.freeze_authority,
               ta.buy_count, ta.sell_count,
               ta.buy_volume_usd, ta.sell_volume_usd,
               ta.whale_buy_count, ta.whale_sell_count,
               ta.whale_buy_volume_usd, ta.whale_sell_volume_usd,
               ta.unique_wallets, ta.largest_trade_usd,
               sig.pump_score, sig.dump_score, sig.anomaly_score,
               sig.smart_money_score, sig.risk_score,
               o.is_true
          FROM token_snapshots s
          LEFT JOIN trade_aggs ta ON ta.snapshot_id = s.id
          LEFT JOIN signals sig ON sig.snapshot_id = s.id
          LEFT JOIN signal_outcomes o ON o.snapshot_id = s.id
          {where}
        """
    ).fetchall()


def _cluster_counts(conn: sqlite3.Connection) -> Dict[int, Dict[str, int]]:
    """For each snapshot, count cluster-action occurrences in the lookback window."""
    from datetime import datetime, timedelta
    out: Dict[int, Dict[str, int]] = {}
    snap_ts = conn.execute(
        "SELECT id, chain, address, ts FROM token_snapshots"
    ).fetchall()
    for snap_id, chain, address, ts in snap_ts:
        t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        lb = (t - timedelta(minutes=LOOKBACK_MIN)).isoformat()
        counts: Dict[str, int] = {f"{c}_{a}": 0 for c in CLUSTERS for a in ACTIONS}
        for cluster, action, n in conn.execute(
            """
            SELECT cluster, action, COUNT(*)
              FROM wallet_events
             WHERE chain = ? AND address = ?
               AND ts >= ? AND ts <= ?
             GROUP BY cluster, action
            """,
            (chain, address, lb, ts),
        ).fetchall():
            key = f"{cluster}_{action}"
            if key in counts:
                counts[key] = n
        out[snap_id] = counts
    return out


def _row_features(r, cluster_counts: Dict[str, int], matched: set, pattern_ids: List[int]) -> Dict[str, float]:
    d: Dict[str, float] = {}
    numeric_cols = [
        "price", "liquidity_usd", "market_cap",
        "volume_5m_usd", "volume_1h_usd", "volume_24h_usd",
        "price_change_5m_pct", "price_change_1h_pct", "price_change_24h_pct",
        "holder_count", "holder_change_24h", "top10_holder_pct",
        "is_mintable", "freeze_authority",
        "buy_count", "sell_count", "buy_volume_usd", "sell_volume_usd",
        "whale_buy_count", "whale_sell_count",
        "whale_buy_volume_usd", "whale_sell_volume_usd",
        "unique_wallets", "largest_trade_usd",
        "pump_score", "dump_score", "anomaly_score",
        "smart_money_score", "risk_score",
    ]
    for col in numeric_cols:
        try:
            v = r[col]
        except (KeyError, IndexError):
            v = None
        d[col] = float(v) if v is not None else 0.0

    buy_v = d["buy_volume_usd"]
    sell_v = d["sell_volume_usd"]
    d["buy_sell_ratio"] = buy_v / max(1.0, sell_v)
    d["whale_share"] = (d["whale_buy_volume_usd"] + d["whale_sell_volume_usd"]) / max(1.0, buy_v + sell_v)
    d["net_whale_flow"] = d["whale_buy_volume_usd"] - d["whale_sell_volume_usd"]

    for k, v in cluster_counts.items():
        d[f"win_{k}"] = float(v)

    for pid in pattern_ids:
        d[f"pat_{pid}"] = 1.0 if pid in matched else 0.0

    return d


if __name__ == "__main__":
    import sys
    df, y, ids, names = build_matrix(sys.argv[1] if len(sys.argv) > 1 else "data/birdeye_quant.db")
    print(f"rows={len(df)} cols={len(names)} labeled={sum(1 for v in y if v != -1)}")
