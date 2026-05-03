"""
Synthesize wallet_events from existing `top_traders` + `trade_aggs` snapshots.

Birdeye's per-trade firehose is not yet ingested; for v1 we treat each
top_traders row as N virtual events at the snapshot ts. Each event gets
the wallet's cluster label (from wallet_clusters) and a size bucket.

Once real-time wallet tx streams are ingested this module is swapped
out with a pass-through insert — nothing downstream depends on the
synthesis being virtual.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

SIZE_BUCKETS = [
    ("xs", 500),
    ("s", 5_000),
    ("m", 25_000),
    ("l", 100_000),
    ("xl", float("inf")),
]


def bucket_for(size_usd: float) -> str:
    for name, cap in SIZE_BUCKETS:
        if size_usd <= cap:
            return name
    return "xl"


def _parse_raw(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return {}


def rebuild_events(db_path: Path | str, since_ts: str | None = None) -> int:
    """(Re)build wallet_events for snapshots newer than since_ts (or all).
    Returns number of events written."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        select_cols = """
            t.id, t.snapshot_id, t.wallet, t.volume_usd,
            t.buy_count, t.sell_count, t.trade_count, t.raw_json,
            s.ts, s.chain, s.address
        """
        if since_ts is None:
            conn.execute("DELETE FROM wallet_events")
            cur = conn.execute(
                f"""
                SELECT {select_cols}
                  FROM top_traders t
                  JOIN token_snapshots s ON s.id = t.snapshot_id
                 WHERE t.wallet IS NOT NULL AND t.wallet <> ''
                """
            )
        else:
            conn.execute(
                "DELETE FROM wallet_events WHERE ts >= ?", (since_ts,)
            )
            cur = conn.execute(
                f"""
                SELECT {select_cols}
                  FROM top_traders t
                  JOIN token_snapshots s ON s.id = t.snapshot_id
                 WHERE s.ts >= ?
                   AND t.wallet IS NOT NULL AND t.wallet <> ''
                """,
                (since_ts,),
            )

        clusters = {
            row[0]: row[1]
            for row in conn.execute("SELECT wallet, cluster FROM wallet_clusters").fetchall()
        }

        count = 0
        batch: list[tuple] = []
        for r in cur.fetchall():
            buys = int(r["buy_count"] or 0)
            sells = int(r["sell_count"] or 0)
            trade_total = int(r["trade_count"] or 0)
            volume_usd = float(r["volume_usd"] or 0)
            # Earlier versions of quant_signal_engine persisted the wrong
            # Birdeye fields (mapping `volume` to volume_usd, missing
            # tradeBuy/tradeSell). Back-fill from raw_json when structured
            # columns are zero.
            if (buys == 0 and sells == 0 and trade_total == 0) or volume_usd == 0:
                parsed = _parse_raw(r["raw_json"])
                buys = buys or int(parsed.get("tradeBuy", 0) or 0)
                sells = sells or int(parsed.get("tradeSell", 0) or 0)
                trade_total = trade_total or int(parsed.get("trade", 0) or 0)
                if volume_usd == 0:
                    volume_usd = float(parsed.get("volumeUsd", 0) or 0)
            if buys == 0 and sells == 0 and trade_total > 0:
                buys = (trade_total + 1) // 2
                sells = trade_total - buys
            if buys == 0 and sells == 0:
                continue
            total = max(1, buys + sells)
            avg_size = volume_usd / total
            bucket = bucket_for(avg_size)
            cluster = clusters.get(r["wallet"], "anonymous")
            for _ in range(min(buys, 10)):
                batch.append((r["snapshot_id"], r["ts"], r["chain"], r["address"],
                              r["wallet"], cluster, "buy", avg_size, bucket))
            for _ in range(min(sells, 10)):
                batch.append((r["snapshot_id"], r["ts"], r["chain"], r["address"],
                              r["wallet"], cluster, "sell", avg_size, bucket))
            if len(batch) >= 500:
                conn.executemany(
                    """INSERT INTO wallet_events(
                        snapshot_id, ts, chain, address,
                        wallet, cluster, action, size_usd, size_bucket
                    ) VALUES(?,?,?,?,?,?,?,?,?)""",
                    batch,
                )
                count += len(batch)
                batch.clear()
        if batch:
            conn.executemany(
                """INSERT INTO wallet_events(
                    snapshot_id, ts, chain, address,
                    wallet, cluster, action, size_usd, size_bucket
                ) VALUES(?,?,?,?,?,?,?,?,?)""",
                batch,
            )
            count += len(batch)
        conn.commit()
        return count
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    n = rebuild_events(sys.argv[1] if len(sys.argv) > 1 else "data/azalyst_quant.db")
    print(f"Wrote {n} wallet events.")
