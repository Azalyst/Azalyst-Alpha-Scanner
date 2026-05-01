from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import requests

DEFAULT_WEBHOOK_URL = "https://discord.com/api/webhooks/1497641889032044757/RGAO5csfQhJePKkdh9ZPq1IZ5DO7i2_tQVtd2KVJVtVQsjy6Hm208CwmqEL9WHpgHv9q"
DEFAULT_DASHBOARD_URL = "https://azalyst.github.io/Azalyst-Alpha-Scanner/dashboard.html"
DEFAULT_REPO_URL = "https://github.com/Azalyst/Azalyst-Alpha-Scanner"
DEFAULT_QUANT_PATH = Path("reports/latest_quant_signals.json")
DEFAULT_OUTCOMES_PATH = Path("reports/latest_quant_outcomes.json")
DEFAULT_ML_PATH = Path("reports/latest_ml_scores.json")
DEFAULT_BRIEF_PATH = Path("reports/latest_quant_brief.md")
DEFAULT_PORTFOLIO_PATH = Path("portfolio.json")
MAX_EMBED_DESCRIPTION = 4000

CHAIN_LABELS = {
    "solana": "SOL",
    "ethereum": "ETH",
    "base": "BASE",
    "arbitrum": "ARB",
    "bnb": "BNB",
    "avalanche": "AVAX",
    "polygon": "POLY",
    "optimism": "OP",
    "zksync": "ZK",
}

LABEL_EXPLANATIONS = {
    "pump_candidate": "stronger upside watch with better confirmation than a normal anomaly alert",
    "whale_accumulation": "bigger wallets are leaning net-buy, so it is worth watching for follow-through",
    "dump_risk": "pressure is pointing down, so this is more of a caution signal than a buy setup",
    "anomaly_watch": "something unusual showed up, but it still needs confirmation before treating it like a trade",
    "avoid_high_risk": "token risk is too high to trust even if the tape looks active",
    "watch": "interesting enough to monitor, not strong enough to act on by itself",
}


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def load_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def clean_markdown(text: str) -> str:
    text = re.sub(r"^#{1,6}\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1", text)
    text = text.replace("**", "").replace("*", "")
    text = re.sub(r"\n{2,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def fmt_num(value: Any, digits: int = 1) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def chain_label(chain: str) -> str:
    key = str(chain or "unknown").lower()
    return CHAIN_LABELS.get(key, key.upper())


def signal_block(signal: Dict[str, Any]) -> str:
    reasons = ", ".join((signal.get("reasons") or [])[:3]) or "no clear reason tags"
    return (
        f"**{signal.get('symbol') or '?'}** [{chain_label(signal.get('chain') or '')}] - {signal.get('label') or 'watch'}\n"
        f"Tech: pump {fmt_num(signal.get('pump_score'), 1)} | dump {fmt_num(signal.get('dump_score'), 1)} | "
        f"anomaly {fmt_num(signal.get('anomaly_score'), 1)} | smart {fmt_num(signal.get('smart_money_score'), 1)} | "
        f"risk {fmt_num(signal.get('risk_score'), 1)}\n"
        f"Why it showed up: {reasons}"
    )


def build_plain_english(quant: Dict[str, Any], outcomes: Dict[str, Any], ml: Dict[str, Any]) -> str:
    signals = quant.get("signals") or []
    counts = Counter((s.get("label") or "watch") for s in signals)
    scan_chains = (quant.get("filters") or {}).get("scan_chains") or []
    chain_text = ", ".join(chain_label(c) for c in scan_chains) if scan_chains else "the active batch"
    snapshots = int(quant.get("snapshot_count") or 0)
    errors = len(quant.get("errors") or [])
    evaluated = int(outcomes.get("evaluated_count") or 0)
    top = signals[0] if signals else None

    if not signals:
        main = f"This run did not produce any live watchlist names from {chain_text}."
    elif counts.get("pump_candidate", 0) > 0 or counts.get("whale_accumulation", 0) > 0:
        main = (
            f"This run found {counts.get('pump_candidate', 0) + counts.get('whale_accumulation', 0)} stronger upside watches "
            f"inside {snapshots} scored snapshots from {chain_text}."
        )
    elif counts.get("anomaly_watch", 0) >= max(1, len(signals) // 2):
        main = (
            f"This run is mostly anomaly-watch territory: unusual behavior showed up across {snapshots} scored snapshots "
            f"from {chain_text}, but most names still need confirmation before they count as clean buy setups."
        )
    else:
        main = f"This run produced a mixed watchlist across {snapshots} scored snapshots from {chain_text}."

    if top:
        follow = (
            f"Lead name right now: {top.get('symbol') or '?'} on {chain_label(top.get('chain') or '')} "
            f"with label `{top.get('label') or 'watch'}`."
        )
    else:
        follow = "There is no lead name to highlight yet."

    ml_metrics = ml.get("model_metrics") or {}
    auc = float(ml_metrics.get("roc_auc") or 0.0) if ml_metrics.get("status") == "ok" else 0.0
    accuracy = ml_metrics.get("accuracy")
    baseline = ml_metrics.get("majority_baseline_accuracy")
    if ml_metrics.get("status") != "ok":
        ml_line = "ML status: not ready yet, so treat the scanner as rules-first only."
    elif accuracy is not None and baseline is not None and float(accuracy) <= float(baseline) + 1e-9:
        ml_line = "ML status: still research-grade. It is not beating the simple baseline yet."
    elif auc < 0.7:
        ml_line = "ML status: has some ranking signal, but still needs more data and calibration."
    else:
        ml_line = "ML status: moderate ranking signal, but still not something to trust blindly."

    outcome_line = (
        "No older calls matured this run."
        if evaluated == 0
        else f"Older calls checked this run: {evaluated}, hit rate {fmt_num((outcomes.get('hit_rate') or 0) * 100, 1)}%."
    )
    error_line = "No API/reporting errors were recorded." if errors == 0 else f"Errors recorded this run: {errors}."

    return "\n".join([main, follow, outcome_line, ml_line, error_line])


def build_technical_summary(quant: Dict[str, Any], outcomes: Dict[str, Any], ml: Dict[str, Any]) -> str:
    filters = quant.get("filters") or {}
    ml_metrics = ml.get("model_metrics") or {}
    lines = [
        f"Generated: {quant.get('generated_at') or 'n/a'}",
        f"Chains this run: {', '.join(filters.get('scan_chains') or []) or 'n/a'}",
        f"Snapshots: {quant.get('snapshot_count') or 0}",
        f"Trade limit: {filters.get('trade_limit', 'n/a')} | Top traders: {filters.get('top_trader_limit', 'n/a')} | Smart-money inputs: {'ON' if filters.get('smart_money_enabled') else 'OFF'}",
        f"Errors: {len(quant.get('errors') or [])} | Mature outcomes checked: {outcomes.get('evaluated_count') or 0}",
    ]
    if ml_metrics:
        lines.append(
            f"ML: status {ml_metrics.get('status', 'n/a')} | auc {fmt_num(ml_metrics.get('roc_auc'), 3)} | "
            f"f1 {fmt_num(ml_metrics.get('f1'), 3)} | acc {fmt_num(ml_metrics.get('accuracy'), 3)} | "
            f"baseline {fmt_num(ml_metrics.get('majority_baseline_accuracy'), 3)} | labeled {ml_metrics.get('labeled_rows', 'n/a')}"
        )
        lines.append(
            f"Validation: {ml_metrics.get('validation_mode', 'n/a')} | pattern features used: "
            f"{'yes' if ml_metrics.get('pattern_features_used') else 'no'}"
        )
    return "\n".join(lines)


def build_qwen_excerpt(brief_text: str) -> str:
    cleaned = clean_markdown(brief_text)
    if not cleaned:
        return "No Qwen brief file was available for this run."
    if cleaned.lower().startswith("quant signal brief"):
        cleaned = cleaned[len("quant signal brief"):].strip()
    return truncate(cleaned, 1000)


def build_paper_trading_section(portfolio_data: Dict[str, Any]) -> str:
    if not portfolio_data:
        return "Paper Trading: No portfolio data available yet."
    
    # If portfolio_data already has summary fields, use them directly
    if "portfolio_value" in portfolio_data:
        cash = float(portfolio_data.get("cash", 0))
        invested = float(portfolio_data.get("invested", 0))
        unrealised = float(portfolio_data.get("unrealised", 0))
        closed_pnl = float(portfolio_data.get("closed_pnl", 0))
        portfolio_value = float(portfolio_data.get("portfolio_value", 0))
        total_return = float(portfolio_data.get("total_return_pct", 0))
        open_count = int(portfolio_data.get("open_count", 0))
        closed_count = int(portfolio_data.get("closed_count", 0))
        win_rate = float(portfolio_data.get("win_rate", 0))
    else:
        # Raw portfolio.json format - calculate summary
        cash = float(portfolio_data.get("cash", 5000))
        open_positions = portfolio_data.get("open", [])
        closed_positions = portfolio_data.get("closed", [])
        
        invested = sum(p.get("invested", 0) for p in open_positions)
        cur_value = sum(p.get("current", p.get("entry", 0)) * p.get("units", 0) for p in open_positions)
        unrealised = cur_value - invested
        closed_pnl = sum(t.get("pnl", 0) for t in closed_positions)
        portfolio_value = cash + cur_value
        deposited = 5000.0
        total_return = ((portfolio_value - deposited) / deposited * 100) if deposited else 0
        open_count = len(open_positions)
        closed_count = len(closed_positions)
        wins = len([t for t in closed_positions if t.get("pnl", 0) > 0])
        win_rate = (wins / closed_count * 100) if closed_count else 0
    
    lines = [
        f"**Portfolio Value:** ${portfolio_value:,.2f}",
        f"**Cash:** ${cash:,.2f} | **Invested:** ${invested:,.2f}",
        f"**Unrealized P&L:** ${unrealised:+,.2f} | **Closed P&L:** ${closed_pnl:+,.2f}",
        f"**Total Return:** {total_return:+.2f}% | **Win Rate:** {win_rate:.1f}%",
        f"**Open Positions:** {open_count} | **Closed Trades:** {closed_count}",
    ]
    
    return "\n".join(lines)


def build_signal_embed(signals: List[Dict[str, Any]]) -> str:
    if not signals:
        return "No live signals were produced."
    unique: List[Dict[str, Any]] = []
    seen = set()
    for signal in signals:
        key = (signal.get("chain"), signal.get("address"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(signal)
        if len(unique) >= 5:
            break
    blocks = [signal_block(signal) for signal in unique]
    return truncate("\n\n".join(blocks), MAX_EMBED_DESCRIPTION)


def build_payload(
    quant: Dict[str, Any],
    outcomes: Dict[str, Any],
    ml: Dict[str, Any],
    brief_text: str,
    dashboard_url: str,
    repo_url: str,
    run_url: str,
    portfolio_data: Dict[str, Any],
) -> Dict[str, Any]:
    title = "Azalyst Alpha Scanner"
    plain_english = build_plain_english(quant, outcomes, ml)
    technical = build_technical_summary(quant, outcomes, ml)
    signal_lines = build_signal_embed(quant.get("signals") or [])
    qwen_excerpt = build_qwen_excerpt(brief_text)
    paper_trading_lines = build_paper_trading_section(portfolio_data)
    quant_generated = str(quant.get("generated_at") or "")
    if quant_generated and brief_text and quant_generated not in brief_text:
        qwen_excerpt = truncate(
            "Note: the saved Qwen brief looks older than the latest scan, so treat it as context rather than a perfect run match.\n\n"
            + qwen_excerpt,
            MAX_EMBED_DESCRIPTION,
        )
    link_bits = [f"[Dashboard]({dashboard_url})", f"[Repo]({repo_url})"]
    if run_url:
        link_bits.append(f"[Workflow Run]({run_url})")
    links = " | ".join(link_bits)

    embeds = [
        {
            "title": "Executive Brief",
            "color": 0xF97316,
            "description": truncate(plain_english, MAX_EMBED_DESCRIPTION),
        },
        {
            "title": "Signal Board",
            "color": 0x2563EB,
            "description": signal_lines,
        },
        {
            "title": "Paper Trading Performance",
            "color": 0x10B981,
            "description": truncate(paper_trading_lines, MAX_EMBED_DESCRIPTION),
        },
        {
            "title": "Model + Run Details",
            "color": 0x64748B,
            "description": truncate(technical, MAX_EMBED_DESCRIPTION),
        },
        {
            "title": "Research Note",
            "color": 0x7C3AED,
            "description": truncate(qwen_excerpt, MAX_EMBED_DESCRIPTION),
        },
    ]

    return {
        "username": "Azalyst Alpha Scanner",
        "allowed_mentions": {"parse": []},
        "content": f"{title} | Quant Update | {links}",
        "embeds": embeds,
    }


def send_payload(webhook_url: str, payload: Dict[str, Any]) -> None:
    resp = requests.post(webhook_url, json=payload, timeout=30)
    resp.raise_for_status()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Post Azalyst reports to Discord.")
    parser.add_argument("--webhook-url", default=DEFAULT_WEBHOOK_URL, help="Discord webhook URL")
    parser.add_argument("--dashboard-url", default=DEFAULT_DASHBOARD_URL, help="Public dashboard URL")
    parser.add_argument("--repo-url", default=DEFAULT_REPO_URL, help="Repository URL")
    parser.add_argument("--run-url", default="", help="Workflow run URL")
    parser.add_argument("--quant-report", default=str(DEFAULT_QUANT_PATH), help="Path to latest quant signals JSON")
    parser.add_argument("--outcomes-report", default=str(DEFAULT_OUTCOMES_PATH), help="Path to latest outcomes JSON")
    parser.add_argument("--ml-report", default=str(DEFAULT_ML_PATH), help="Path to latest ML JSON")
    parser.add_argument("--brief-report", default=str(DEFAULT_BRIEF_PATH), help="Path to latest Qwen brief markdown")
    parser.add_argument("--portfolio-report", default=str(DEFAULT_PORTFOLIO_PATH), help="Path to portfolio JSON")
    parser.add_argument("--payload-out", default="", help="Optional path to write the Discord JSON payload")
    parser.add_argument("--dry-run", action="store_true", help="Print payload instead of posting")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    quant = load_json(Path(args.quant_report))
    outcomes = load_json(Path(args.outcomes_report))
    ml = load_json(Path(args.ml_report))
    brief_text = load_text(Path(args.brief_report))
    portfolio_data = load_json(Path(args.portfolio_report))

    payload = build_payload(
        quant=quant,
        outcomes=outcomes,
        ml=ml,
        brief_text=brief_text,
        dashboard_url=args.dashboard_url,
        repo_url=args.repo_url,
        run_url=args.run_url,
        portfolio_data=portfolio_data,
    )

    if args.payload_out:
        Path(args.payload_out).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return 0

    send_payload(args.webhook_url, payload)
    print("Discord update sent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
