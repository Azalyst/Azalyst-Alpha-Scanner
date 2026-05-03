"""
Wallet clustering — assigns each wallet to one of:

    whale         — top 1% by cumulative volume, high repeat presence
    smart_money   — high pnl + win rate, at least N observations
    sniper        — first observed within 10 min of token creation
    mm            — buy/sell balance suggests market making
    anonymous     — everything else (the default)

Pure-Python, zero ML deps. Runs on `top_traders` aggregates that
`quant_signal_engine.py` already collects.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple

MIN_OBS_FOR_CLUSTER = 3
WHALE_VOLUME_PCT = 0.99
SMART_PNL_PCT = 0.95
SMART_MIN_WIN_RATE = 0.55
MM_BUY_SELL_RATIO_BAND = (0.85, 1.15)


def _pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(len(s) - 1, max(0, int(len(s) * q)))
    return s[idx]


def rebuild_clusters(db_path: Path | str) -> Dict[str, int]:
    """Recompute wallet_clusters from top_traders. Returns counts by cluster."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT t.wallet, s.chain, s.address, s.ts,
                   t.pnl_usd, t.volume_usd, t.buy_count, t.sell_count,
                   t.win_rate, t.trade_count, t.raw_json
              FROM top_traders t
              JOIN token_snapshots s ON s.id = t.snapshot_id
             WHERE t.wallet IS NOT NULL AND t.wallet <> ''
            """
        ).fetchall()

        if not rows:
            return {}

        per_wallet: Dict[str, Dict] = {}
        for r in rows:
            w = r["wallet"]
            pnl = (r["pnl_usd"] or 0)
            volume = (r["volume_usd"] or 0)
            buys = (r["buy_count"] or 0)
            sells = (r["sell_count"] or 0)
            # Back-fill from raw_json when the structured columns are empty
            # (pre-fix ingestion wrote zeros for tradeBuy/Sell/totalPnl/volumeUsd).
            if pnl == 0 and volume == 0 and buys == 0 and sells == 0 and r["raw_json"]:
                try:
                    raw = json.loads(r["raw_json"])
                    pnl = float(raw.get("totalPnl", 0) or 0)
                    volume = float(raw.get("volumeUsd", 0) or 0)
                    buys = int(raw.get("tradeBuy", 0) or 0)
                    sells = int(raw.get("tradeSell", 0) or 0)
                except (ValueError, TypeError):
                    pass
            d = per_wallet.setdefault(w, {
                "chain": r["chain"],
                "pnl": 0.0,
                "volume": 0.0,
                "obs": 0,
                "buys": 0,
                "sells": 0,
                "win_rate_sum": 0.0,
                "win_rate_n": 0,
                "first_ts": r["ts"],
            })
            d["pnl"] += pnl
            d["volume"] += volume
            d["obs"] += 1
            d["buys"] += buys
            d["sells"] += sells
            if r["win_rate"] is not None and r["win_rate"] > 0:
                d["win_rate_sum"] += r["win_rate"]
                d["win_rate_n"] += 1
            if r["ts"] < d["first_ts"]:
                d["first_ts"] = r["ts"]

        # Sniper detection requires true on-chain token creation timestamps.
        # The `tokens.first_seen_ts` column records when OUR scanner first
        # saw the token, which gives a false positive for nearly every
        # observed wallet. Until we ingest `/defi/token_creation_info`
        # into its own column, we skip the sniper rule entirely.
        sniper_windows: Dict[str, str] = {}

        volumes = [d["volume"] for d in per_wallet.values() if d["obs"] >= MIN_OBS_FOR_CLUSTER]
        pnls = [d["pnl"] for d in per_wallet.values() if d["obs"] >= MIN_OBS_FOR_CLUSTER]
        whale_cut = _pct(volumes, WHALE_VOLUME_PCT) if volumes else float("inf")
        smart_cut = _pct(pnls, SMART_PNL_PCT) if pnls else float("inf")

        now = datetime.now(timezone.utc).isoformat()
        counts: Dict[str, int] = {}

        for wallet, d in per_wallet.items():
            cluster = _assign(d, whale_cut, smart_cut, wallet in sniper_windows)
            counts[cluster] = counts.get(cluster, 0) + 1
            win_rate_avg = (d["win_rate_sum"] / d["win_rate_n"]) if d["win_rate_n"] else 0.0
            conn.execute(
                """
                INSERT INTO wallet_clusters(
                    wallet, chain, cluster, score,
                    pnl_total_usd, volume_total_usd, snapshots_seen,
                    win_rate, first_seen_ts, last_updated_ts
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    cluster = excluded.cluster,
                    score = excluded.score,
                    pnl_total_usd = excluded.pnl_total_usd,
                    volume_total_usd = excluded.volume_total_usd,
                    snapshots_seen = excluded.snapshots_seen,
                    win_rate = excluded.win_rate,
                    last_updated_ts = excluded.last_updated_ts
                """,
                (
                    wallet, d["chain"], cluster,
                    _cluster_score(d, cluster),
                    d["pnl"], d["volume"], d["obs"],
                    win_rate_avg, d["first_ts"], now,
                ),
            )

        conn.commit()
        return counts
    finally:
        conn.close()


def _assign(d: Dict, whale_cut: float, smart_cut: float, is_sniper: bool) -> str:
    if is_sniper:
        return "sniper"
    if d["obs"] >= MIN_OBS_FOR_CLUSTER and d["volume"] >= whale_cut and whale_cut > 0:
        return "whale"
    win_rate_avg = (d["win_rate_sum"] / d["win_rate_n"]) if d["win_rate_n"] else 0.0
    if (d["obs"] >= MIN_OBS_FOR_CLUSTER and d["pnl"] >= smart_cut
            and smart_cut > 0 and win_rate_avg >= SMART_MIN_WIN_RATE):
        return "smart_money"
    total = d["buys"] + d["sells"]
    if total >= 20:
        ratio = d["buys"] / max(1, d["sells"])
        if MM_BUY_SELL_RATIO_BAND[0] <= ratio <= MM_BUY_SELL_RATIO_BAND[1]:
            return "mm"
    return "anonymous"


def _cluster_score(d: Dict, cluster: str) -> float:
    if cluster == "whale":
        return float(d["volume"])
    if cluster == "smart_money":
        return float(d["pnl"])
    return float(d["obs"])


def _iso_delta_min(a: str, b: str) -> float:
    t1 = datetime.fromisoformat(a.replace("Z", "+00:00"))
    t2 = datetime.fromisoformat(b.replace("Z", "+00:00"))
    return (t2 - t1).total_seconds() / 60.0


if __name__ == "__main__":
    import sys
    counts = rebuild_clusters(sys.argv[1] if len(sys.argv) > 1 else "data/azalyst_quant.db")
    for c, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"{c:15s} {n}")
