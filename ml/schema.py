"""
ML-pipeline schema: additive, idempotent DDL.

Extends the quant DB with four tables plus an `ml_score` column on
`signals`. Safe to run repeatedly; only creates what's missing.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS wallet_clusters (
    wallet TEXT PRIMARY KEY,
    chain TEXT NOT NULL,
    cluster TEXT NOT NULL,
    score REAL DEFAULT 0,
    pnl_total_usd REAL DEFAULT 0,
    volume_total_usd REAL DEFAULT 0,
    snapshots_seen INTEGER DEFAULT 0,
    win_rate REAL DEFAULT 0,
    first_seen_ts TEXT,
    last_updated_ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wallet_clusters_cluster
    ON wallet_clusters(cluster);

CREATE TABLE IF NOT EXISTS wallet_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL,
    ts TEXT NOT NULL,
    chain TEXT NOT NULL,
    address TEXT NOT NULL,
    wallet TEXT NOT NULL,
    cluster TEXT NOT NULL,
    action TEXT NOT NULL,
    size_usd REAL DEFAULT 0,
    size_bucket TEXT,
    FOREIGN KEY(snapshot_id) REFERENCES token_snapshots(id)
);

CREATE INDEX IF NOT EXISTS idx_wallet_events_token_ts
    ON wallet_events(chain, address, ts);
CREATE INDEX IF NOT EXISTS idx_wallet_events_snapshot
    ON wallet_events(snapshot_id);

CREATE TABLE IF NOT EXISTS pattern_library (
    pattern_id INTEGER PRIMARY KEY AUTOINCREMENT,
    prefix_json TEXT NOT NULL UNIQUE,
    length INTEGER NOT NULL,
    support INTEGER NOT NULL,
    lift REAL DEFAULT 0,
    positive_rate REAL DEFAULT 0,
    horizon_min INTEGER NOT NULL,
    mined_ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pattern_library_lift
    ON pattern_library(lift DESC);

CREATE TABLE IF NOT EXISTS pattern_matches (
    snapshot_id INTEGER NOT NULL,
    pattern_id INTEGER NOT NULL,
    matched_ts TEXT NOT NULL,
    PRIMARY KEY(snapshot_id, pattern_id),
    FOREIGN KEY(snapshot_id) REFERENCES token_snapshots(id),
    FOREIGN KEY(pattern_id) REFERENCES pattern_library(pattern_id)
);

CREATE TABLE IF NOT EXISTS ml_scores (
    snapshot_id INTEGER PRIMARY KEY,
    ts TEXT NOT NULL,
    chain TEXT NOT NULL,
    address TEXT NOT NULL,
    symbol TEXT,
    ml_prob REAL DEFAULT 0,
    ml_direction TEXT,
    model_version TEXT,
    feature_snapshot_json TEXT,
    FOREIGN KEY(snapshot_id) REFERENCES token_snapshots(id)
);

CREATE INDEX IF NOT EXISTS idx_ml_scores_ts
    ON ml_scores(ts);
"""


def ensure_schema(db_path: Path | str) -> None:
    """Create ML tables if missing. Safe to call repeatedly."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(DDL)
        _add_column_if_missing(conn, "signals", "ml_prob", "REAL DEFAULT NULL")
        _add_column_if_missing(conn, "signals", "ml_direction", "TEXT DEFAULT NULL")
        conn.commit()
    finally:
        conn.close()


def _add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, decl: str) -> None:
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")


if __name__ == "__main__":
    import sys
    ensure_schema(sys.argv[1] if len(sys.argv) > 1 else "data/azalyst_quant.db")
    print("Schema ready.")
