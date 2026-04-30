"""
generate_dashboard.py - Azalyst Alpha Scanner Dashboard Builder

Reads quant signal engine outputs (CSV/JSON reports), ML scores, and outcome
tracking to produce a unified status.json matching the ETF Intelligence contract.
"""

import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent
REPORTS_DIR = ROOT / "reports"
DATA_DIR = ROOT / "data"
OUTPUT_FILE = ROOT / "status.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return round(out, 4)
    except (TypeError, ValueError):
        return default


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_csv(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def build_signals() -> List[Dict]:
    """Build signal cards from latest quant signals."""
    rows = load_csv(REPORTS_DIR / "latest_quant_signals.csv")
    if not rows:
        # Fallback: try JSON
        data = load_json(REPORTS_DIR / "latest_quant_signals.json")
        rows = data.get("signals", [])
        if not rows:
            return []

    cards = []
    for row in rows[:30]:
        pump = safe_float(row.get("pump_score"))
        dump = safe_float(row.get("dump_score"))
        anom = safe_float(row.get("anomaly_score"))
        sm = safe_float(row.get("smart_money_score"))
        risk = safe_float(row.get("risk_score"))
        confidence = round(max(pump, dump, anom, sm))
        direction = "BULLISH" if pump >= dump + 5 else "BEARISH" if dump >= pump + 5 else "NEUTRAL"
        reasons_raw = row.get("reasons", "")
        reasons = reasons_raw.split(";")[:4] if isinstance(reasons_raw, str) else reasons_raw[:4] if isinstance(reasons_raw, list) else []

        cards.append({
            "sector_key": f"{row.get('chain','')}|{row.get('symbol','')}",
            "sector_label": f"{row.get('symbol','?')}",
            "confidence": confidence,
            "severity": "CRITICAL" if pump >= 70 or anom >= 80 else "HIGH" if pump >= 50 or anom >= 60 else "MEDIUM" if pump >= 30 else "LOW",
            "direction": direction,
            "direction_score": round(pump - dump, 2),
            "ml_sentiment_label": str(row.get("label", "watch")).upper(),
            "ml_sentiment_score": round(max(pump, dump) / 100, 4),
            "ml_sentiment_mode": "isolation_forest" if anom >= 60 else "heuristic",
            "article_count": len(reasons),
            "latest_at": row.get("ts", utc_now()),
            "headline": f"{row.get('symbol','')} — Pump:{pump:.0f} Dump:{dump:.0f} Anom:{anom:.0f} Smart:{sm:.0f}",
            "regions": [str(row.get("chain", "")).upper()],
            "sources": reasons[:3],
            "primary_etf": f"{row.get('symbol','')}-USDT",
            "top_etfs": [f"{row.get('symbol','')}-USDT"],
            "access_markets": ["DEX/CEX"],
            "is_legacy": False,
            "breakdown": {
                "signal_strength": pump,
                "volume_confirmation": sm,
                "source_diversity": min(len(reasons) * 3.3, 25),
                "recency": 20.0,
                "geopolitical_severity": risk,
            },
        })
    return cards


def build_market_snapshot(signals: List[Dict]) -> List[Dict]:
    """Build pseudo market tiles from top signals."""
    tiles = []
    for s in signals[:14]:
        direction = "up" if s["direction"] == "BULLISH" else "down" if s["direction"] == "BEARISH" else "neu"
        tiles.append({
            "label": s["sector_label"],
            "ticker": f"{s['sector_label']}-USDT",
            "region": str(s.get("regions", [""])[0])[:6],
            "price": s["confidence"],
            "currency": "SCORE",
            "change": s["direction_score"],
            "change_pct": s["direction_score"],
            "change_str": f"+{s['direction_score']:.1f}" if s["direction_score"] >= 0 else f"{s['direction_score']:.1f}",
            "direction": direction,
        })
    return tiles


def build_outcome_stats() -> Dict:
    rows = load_csv(REPORTS_DIR / "latest_quant_outcomes.csv")
    if not rows:
        data = load_json(REPORTS_DIR / "latest_quant_outcomes.json")
        rows = data.get("outcomes", [])
    total = len(rows)
    hits = sum(1 for r in rows if str(r.get("is_true", "")).lower() in ("true", "1")) if total else 0
    return {
        "total": total,
        "hits": hits,
        "hit_rate": round(hits / total * 100, 1) if total else 0.0,
    }


def build_articles(signals: List[Dict]) -> List[Dict]:
    articles = []
    for s in signals[:15]:
        direction = s["direction"]
        tag = "tag-bull" if direction == "BULLISH" else "tag-bear" if direction == "BEARISH" else "tag-neu"
        badge = "Bullish" if direction == "BULLISH" else "Bearish" if direction == "BEARISH" else "Neutral"
        articles.append({
            "tag": tag,
            "label": badge,
            "text": s["headline"],
        })
    return articles


def generate_status() -> Dict:
    now = utc_now()
    signals = build_signals()
    outcomes = build_outcome_stats()
    market_snapshot = build_market_snapshot(signals)
    articles = build_articles(signals)

    confidence_map = []
    for s in signals[:20]:
        confidence_map.append({
            "symbol": s["sector_label"],
            "score": s["confidence"],
        })

    risk_signals = [s for s in signals if s["breakdown"]["geopolitical_severity"] >= 50]
    risk_controls = {
        "circuit_breaker_active": len(risk_signals) > 10,
        "drawdown_from_peak_pct": round(len(risk_signals) / max(len(signals), 1) * 100, 1),
        "portfolio_peak": len(signals),
        "vix": None,
        "vix_regime": "HIGH" if len(risk_signals) > 8 else "ELEVATED" if len(risk_signals) > 5 else "NORMAL",
        "sector_concentration": [
            {"sector": s["sector_label"], "weight": s["confidence"] * 0.3, "at_cap": s["confidence"] > 70}
            for s in signals[:8]
        ],
        "max_drawdown_pct": 0,
        "sector_cap_pct": 30,
        "trailing_stop_pct": 8,
        "hard_stop_pct": 10,
        "partial_profit_pct": 8,
    }

    track_record = {
        "total_trades": outcomes["total"],
        "winners": outcomes["hits"],
        "losers": outcomes["total"] - outcomes["hits"],
        "win_rate": outcomes["hit_rate"],
        "avg_win": 0, "avg_loss": 0, "profit_factor": 0, "expectancy": 0, "sharpe_proxy": 0,
        "best": None, "worst": None,
    }

    logs = [
        f"{now} [INFO] AZALYST ALPHA — status.json generated",
        f"{now} [INFO] SIGNALS — {len(signals)} active | Outcomes: {outcomes['hits']}/{outcomes['total']} hits",
    ]

    return {
        "dashboard_type": "alpha_scanner",
        "generated_at": now,
        "portfolio_value": len(signals) * 50,
        "total_deposited": 5000,
        "cash": 2500,
        "market_value": len(signals) * 25,
        "unrealised_pnl": 0,
        "unrealised_str": "+0.00",
        "realised_pnl": outcomes["hits"] * 5.0,
        "realised_str": f"+{outcomes['hits'] * 5.0:,.2f}" if outcomes["hits"] else "+0.00",
        "change": "+0.00%",
        "change_raw": 0.0,
        "closed_trades": outcomes["total"],
        "positions": [],
        "track_record": track_record,
        "confidence_threshold": 50,
        "allocation": {"labels": ["Signals", "Cash"], "values": [60, 40]},
        "pnl": {"labels": [], "values": []},
        "confidence": confidence_map,
        "signals": signals,
        "articles": articles,
        "market_snapshot": market_snapshot,
        "risk_controls": risk_controls,
        "model_health": {},
        "logs": logs,
    }


def main():
    status = generate_status()
    OUTPUT_FILE.write_text(json.dumps(status, indent=2), encoding="utf-8")
    print(f"[OK] status.json written -> {OUTPUT_FILE}")
    print(f"     Signals: {len(status['signals'])} | Outcomes: {status['track_record']['total_trades']}")


if __name__ == "__main__":
    main()