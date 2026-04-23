"""
CLI for the behavioral ML pipeline.

Usage:
    python -m ml schema   [db]
    python -m ml cluster  [db]
    python -m ml events   [db]
    python -m ml mine     [db]
    python -m ml train    [db]
    python -m ml score    [db] [mode=recent|full]
    python -m ml export   [db]
    python -m ml all      [db]        # schema → cluster → events → mine → train → score(full) → export
    python -m ml refresh  [db]        # cluster → events → mine → score(recent) → export   (cheap cron)

`db` defaults to `data/birdeye_quant.db`.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

DEFAULT_DB = "data/birdeye_quant.db"


def _db_from(argv: list[str], idx: int = 2) -> str:
    if len(argv) > idx and not argv[idx].startswith("-") and "=" not in argv[idx]:
        return argv[idx]
    return DEFAULT_DB


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2

    cmd = argv[1]
    db = _db_from(argv)

    if cmd == "schema":
        from ml.schema import ensure_schema
        ensure_schema(db)
        print("schema ready")
        return 0

    if cmd == "cluster":
        from ml.schema import ensure_schema
        from ml.clustering import rebuild_clusters
        ensure_schema(db)
        counts = rebuild_clusters(db)
        print(json.dumps({"clusters": counts}, indent=2))
        return 0

    if cmd == "events":
        from ml.schema import ensure_schema
        from ml.events import rebuild_events
        ensure_schema(db)
        n = rebuild_events(db)
        print(json.dumps({"events_written": n}, indent=2))
        return 0

    if cmd == "mine":
        from ml.schema import ensure_schema
        from ml.patterns import mine_patterns
        ensure_schema(db)
        n = mine_patterns(db)
        print(json.dumps({"patterns": n}, indent=2))
        return 0

    if cmd == "train":
        from ml.schema import ensure_schema
        from ml.train import train
        ensure_schema(db)
        metrics = train(db)
        print(json.dumps(metrics, indent=2))
        return 0

    if cmd == "score":
        from ml.schema import ensure_schema
        from ml.score import score
        ensure_schema(db)
        mode = "recent"
        for a in argv[2:]:
            if a.startswith("mode="):
                mode = a.split("=", 1)[1]
        result = score(db, mode=mode)
        print(json.dumps(result, indent=2))
        return 0

    if cmd == "export":
        from ml.schema import ensure_schema
        from ml.export import export
        ensure_schema(db)
        result = export(db)
        print(json.dumps(result, indent=2))
        return 0

    if cmd == "refresh":
        return _run_pipeline(db, include_train=False, score_mode="recent")

    if cmd == "all":
        return _run_pipeline(db, include_train=True, score_mode="full")

    print(f"unknown command: {cmd}")
    print(__doc__)
    return 2


def _run_pipeline(db: str, *, include_train: bool, score_mode: str) -> int:
    from ml.schema import ensure_schema
    from ml.clustering import rebuild_clusters
    from ml.events import rebuild_events
    from ml.patterns import mine_patterns
    from ml.score import score
    from ml.export import export

    print(f"[ml] db={db}")
    ensure_schema(db)
    print("[ml] schema ready")
    counts = rebuild_clusters(db)
    print(f"[ml] clusters: {counts}")
    n_events = rebuild_events(db)
    print(f"[ml] events: {n_events}")
    n_patterns = mine_patterns(db)
    print(f"[ml] patterns: {n_patterns}")
    if include_train:
        from ml.train import train
        metrics = train(db)
        print(f"[ml] train: {json.dumps(metrics)}")
    result = score(db, mode=score_mode)
    print(f"[ml] score: {json.dumps(result)}")
    ex = export(db)
    print(f"[ml] export: {json.dumps(ex)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
