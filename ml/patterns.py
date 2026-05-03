"""
Sequence-pattern mining.

For each snapshot we build the sequence of (cluster, action) tokens
that occurred inside a lookback window before the snapshot. Across all
sequences we mine frequent subsequences with PrefixSpan (pure-Python),
then rank by lift against the `signal_outcomes.is_true` label.

The top-K patterns are stored in `pattern_library`; per-snapshot
matches land in `pattern_matches`, which the feature builder consumes.

Graceful degradation: if the `prefixspan` package is missing we fall
back to frequent-bigram counting so the pipeline still runs.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Sequence, Tuple

LOOKBACK_MIN = 30
MIN_SUPPORT = 5
TOP_K = 40
MAX_PATTERN_LEN = 4


def _token(cluster: str, action: str) -> str:
    return f"{cluster}_{action}"


def _load_sequences(conn: sqlite3.Connection) -> List[Tuple[int, List[str], int | None]]:
    """For each snapshot, return (snapshot_id, [tokens...], is_true)."""
    snapshots = conn.execute(
        """
        SELECT s.id, s.ts, s.chain, s.address,
               COALESCE(o.is_true, NULL) AS is_true
          FROM token_snapshots s
          LEFT JOIN signal_outcomes o ON o.snapshot_id = s.id
        """
    ).fetchall()

    out: List[Tuple[int, List[str], int | None]] = []
    for snap_id, ts, chain, address, is_true in snapshots:
        lb_start = _subtract_minutes(ts, LOOKBACK_MIN)
        rows = conn.execute(
            """
            SELECT cluster, action
              FROM wallet_events
             WHERE chain = ? AND address = ?
               AND ts >= ? AND ts <= ?
             ORDER BY ts ASC, id ASC
            """,
            (chain, address, lb_start, ts),
        ).fetchall()
        if not rows:
            continue
        seq = [_token(c, a) for (c, a) in rows]
        out.append((snap_id, seq, is_true))
    return out


def _subtract_minutes(iso_ts: str, minutes: int) -> str:
    from datetime import timedelta
    t = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    return (t - timedelta(minutes=minutes)).isoformat()


def _mine_with_prefixspan(seqs: Sequence[List[str]], min_support: int) -> List[Tuple[int, List[str]]]:
    try:
        from prefixspan import PrefixSpan
    except ImportError:
        return _mine_bigrams(seqs, min_support)
    ps = PrefixSpan(list(seqs))
    ps.minlen = 2
    ps.maxlen = MAX_PATTERN_LEN
    results = ps.frequent(min_support)
    return [(support, pattern) for support, pattern in results if len(pattern) >= 2]


def _mine_bigrams(seqs: Sequence[List[str]], min_support: int) -> List[Tuple[int, List[str]]]:
    from collections import Counter
    counts: Counter[Tuple[str, str]] = Counter()
    for seq in seqs:
        seen: set[Tuple[str, str]] = set()
        for i in range(len(seq) - 1):
            pair = (seq[i], seq[i + 1])
            if pair not in seen:
                counts[pair] += 1
                seen.add(pair)
    return [(c, list(pair)) for pair, c in counts.items() if c >= min_support]


def _contains_subseq(seq: List[str], pattern: List[str]) -> bool:
    it = iter(seq)
    return all(tok in it for tok in pattern)


def mine_patterns(db_path: Path | str, horizon_min: int = 60) -> int:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        data = _load_sequences(conn)
        if not data:
            return 0

        seqs = [seq for _, seq, _ in data]
        labeled_seqs = [(seq, lbl) for _, seq, lbl in data if lbl is not None]
        total_pos = sum(1 for _, lbl in labeled_seqs if lbl == 1)
        total_labeled = len(labeled_seqs)
        base_rate = (total_pos / total_labeled) if total_labeled else 0.0

        raw = _mine_with_prefixspan(seqs, MIN_SUPPORT)
        if not raw:
            return 0

        scored: List[Tuple[float, float, int, int, List[str]]] = []
        for support, pattern in raw:
            match_pos = match_tot = 0
            for seq, lbl in labeled_seqs:
                if _contains_subseq(seq, pattern):
                    match_tot += 1
                    if lbl == 1:
                        match_pos += 1
            if match_tot == 0:
                continue
            positive_rate = match_pos / match_tot
            lift = (positive_rate / base_rate) if base_rate > 0 else 0.0
            scored.append((lift, positive_rate, support, match_tot, pattern))

        scored.sort(key=lambda x: (x[0], x[2]), reverse=True)
        top = scored[:TOP_K]

        now = datetime.now(timezone.utc).isoformat()
        conn.execute("DELETE FROM pattern_library")
        conn.execute("DELETE FROM pattern_matches")

        written = 0
        pid_map: dict[tuple[str, ...], int] = {}
        for lift, pos_rate, support, _match_tot, pattern in top:
            cur = conn.execute(
                """INSERT INTO pattern_library(
                    prefix_json, length, support, lift,
                    positive_rate, horizon_min, mined_ts
                ) VALUES(?,?,?,?,?,?,?)""",
                (json.dumps(pattern), len(pattern), support, lift,
                 pos_rate, horizon_min, now),
            )
            pid_map[tuple(pattern)] = cur.lastrowid
            written += 1

        for snap_id, seq, _ in data:
            for pat, pid in pid_map.items():
                if _contains_subseq(seq, list(pat)):
                    conn.execute(
                        """INSERT OR IGNORE INTO pattern_matches(
                            snapshot_id, pattern_id, matched_ts
                        ) VALUES(?,?,?)""",
                        (snap_id, pid, now),
                    )
        conn.commit()
        return written
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    n = mine_patterns(sys.argv[1] if len(sys.argv) > 1 else "data/azalyst_quant.db")
    print(f"Stored {n} patterns.")
