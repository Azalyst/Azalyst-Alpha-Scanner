"""
Birdeye Whale Tracking Module — Multi-Chain
Supports: Solana, Ethereum, Base, Arbitrum, BNB, Avalanche, Polygon, Optimism, zkSync
"""

import requests
import json
import time
import os
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict


SUPPORTED_CHAINS = {
    "solana":    "solana",
    "ethereum":  "ethereum",
    "base":      "base",
    "arbitrum":  "arbitrum",
    "bnb":       "bsc",
    "avalanche": "avalanche",
    "polygon":   "polygon",
    "optimism":  "optimism",
    "zksync":    "zksync",
}

DEFAULT_CHAIN = "solana"


@dataclass
class WhaleWallet:
    address: str
    chain: str
    total_holdings: float
    recent_activity: List[Dict]
    most_held_tokens: List[Dict]
    last_updated: str

    def to_dict(self):
        return asdict(self)


@dataclass
class TokenSignal:
    token_address: str
    token_name: str
    chain: str
    signal_type: str
    confidence: float
    indicators: List[str]
    timestamp: str

    def to_dict(self):
        return asdict(self)


class BirdeyeAPI:
    BASE_URL = "https://public-api.birdeye.so"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.headers = {
            "X-API-KEY": api_key if api_key else "",
            "Accept": "application/json",
        }

    def _h(self, chain: str) -> Dict:
        resolved = SUPPORTED_CHAINS.get(chain.lower(), chain.lower())
        return {**self.headers, "x-chain": resolved}

    def get_trending_tokens(self, chain: str = DEFAULT_CHAIN, time_frame: str = "24h") -> List[Dict]:
        endpoint = f"{self.BASE_URL}/defi/trending_tokens/{SUPPORTED_CHAINS.get(chain.lower(), chain)}"
        try:
            r = requests.get(endpoint, headers=self._h(chain), params={"time_frame": time_frame}, timeout=10)
            return r.json().get("data", []) if r.status_code == 200 else []
        except Exception as e:
            return [{"error": str(e)}]

    def get_wallet_portfolio(self, wallet_address: str, chain: str = DEFAULT_CHAIN) -> Dict:
        try:
            r = requests.get(f"{self.BASE_URL}/v1/wallet/token_list", headers=self._h(chain),
                             params={"wallet": wallet_address}, timeout=10)
            return r.json().get("data", {}) if r.status_code == 200 else {}
        except Exception as e:
            return {"error": str(e)}

    def get_token_overview(self, token_address: str, chain: str = DEFAULT_CHAIN) -> Dict:
        try:
            r = requests.get(f"{self.BASE_URL}/defi/token_overview", headers=self._h(chain),
                             params={"address": token_address}, timeout=10)
            return r.json().get("data", {}) if r.status_code == 200 else {}
        except Exception as e:
            return {"error": str(e)}

    def get_token_trades(self, token_address: str, chain: str = DEFAULT_CHAIN, limit: int = 100) -> List[Dict]:
        try:
            r = requests.get(f"{self.BASE_URL}/defi/txs/token", headers=self._h(chain),
                             params={"address": token_address, "limit": limit, "offset": 0}, timeout=10)
            return r.json().get("data", {}).get("items", []) if r.status_code == 200 else []
        except Exception as e:
            return [{"error": str(e)}]

    def get_token_security(self, token_address: str, chain: str = DEFAULT_CHAIN) -> Dict:
        try:
            r = requests.get(f"{self.BASE_URL}/defi/token_security", headers=self._h(chain),
                             params={"address": token_address}, timeout=10)
            return r.json().get("data", {}) if r.status_code == 200 else {}
        except Exception as e:
            return {"error": str(e)}

    def get_profitable_traders(self, chain: str = DEFAULT_CHAIN, time_frame: str = "7D", limit: int = 20) -> List[Dict]:
        try:
            r = requests.get(f"{self.BASE_URL}/trader/gainers-losers", headers=self._h(chain),
                             params={"type": "gainers", "sort_by": "PnL", "time_frame": time_frame, "limit": limit},
                             timeout=15)
            data = r.json().get("data", []) if r.status_code == 200 else []
            for t in data:
                t["chain"] = chain
            return data
        except Exception as e:
            return [{"error": str(e)}]

    def get_wallet_pnl(self, wallet_address: str, chain: str = DEFAULT_CHAIN) -> Dict:
        try:
            h = {**self._h(chain), "Content-Type": "application/json"}
            r = requests.post(f"{self.BASE_URL}/wallet/v2/pnl/summary", headers=h,
                              json={"wallet": wallet_address}, timeout=15)
            return r.json().get("data", {}) if r.status_code == 200 else {}
        except Exception as e:
            return {"error": str(e)}

    def get_top_traders(self, token_address: str, chain: str = DEFAULT_CHAIN,
                        time_frame: str = "24h", limit: int = 10) -> List[Dict]:
        try:
            r = requests.get(f"{self.BASE_URL}/defi/v2/tokens/top_traders", headers=self._h(chain),
                             params={"address": token_address, "time_frame": time_frame,
                                     "sort_by": "volume", "limit": limit}, timeout=15)
            data = r.json().get("data", []) if r.status_code == 200 else []
            for t in data:
                t["token_address"] = token_address
                t["chain"] = chain
            return data
        except Exception as e:
            return [{"error": str(e)}]

    def get_new_listings(self, chain: str = DEFAULT_CHAIN, limit: int = 50) -> List[Dict]:
        try:
            r = requests.get(f"{self.BASE_URL}/defi/v2/tokens/new_listing", headers=self._h(chain),
                             params={"limit": limit}, timeout=15)
            data = r.json().get("data", []) if r.status_code == 200 else []
            for t in data:
                t["chain"] = chain
            return data
        except Exception as e:
            return [{"error": str(e)}]

    def get_token_creation_info(self, token_address: str, chain: str = DEFAULT_CHAIN) -> Dict:
        try:
            r = requests.get(f"{self.BASE_URL}/defi/token_creation_info", headers=self._h(chain),
                             params={"address": token_address}, timeout=15)
            return r.json().get("data", {}) if r.status_code == 200 else {}
        except Exception as e:
            return {"error": str(e)}

    def get_holder_list(self, token_address: str, chain: str = DEFAULT_CHAIN, limit: int = 100) -> List[Dict]:
        try:
            r = requests.get(f"{self.BASE_URL}/defi/v3/token/holder", headers=self._h(chain),
                             params={"address": token_address, "limit": limit}, timeout=15)
            return r.json().get("data", {}).get("holders", []) if r.status_code == 200 else []
        except Exception as e:
            return [{"error": str(e)}]

    def get_wallet_pnl_details(self, wallet_address: str, chain: str = DEFAULT_CHAIN, limit: int = 100) -> List[Dict]:
        try:
            h = {**self._h(chain), "Content-Type": "application/json"}
            r = requests.post(f"{self.BASE_URL}/wallet/v2/pnl/details", headers=h,
                              json={"wallet": wallet_address, "limit": limit}, timeout=15)
            return r.json().get("data", []) if r.status_code == 200 else []
        except Exception as e:
            return [{"error": str(e)}]

    def get_trader_txs(self, wallet_address: str, chain: str = DEFAULT_CHAIN,
                       start_time: int = None, end_time: int = None, limit: int = 50) -> List[Dict]:
        params = {"wallet": wallet_address, "limit": limit}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        try:
            r = requests.get(f"{self.BASE_URL}/trader/txs/seek_by_time", headers=self._h(chain),
                             params=params, timeout=15)
            return r.json().get("data", {}).get("items", []) if r.status_code == 200 else []
        except Exception as e:
            return [{"error": str(e)}]

    def get_ohlcv(self, token_address: str, chain: str = DEFAULT_CHAIN,
                  timeframe: str = "1h", from_time: int = None, to_time: int = None) -> List[Dict]:
        params = {"address": token_address, "timeframe": timeframe}
        if from_time:
            params["from_time"] = from_time
        if to_time:
            params["to_time"] = to_time
        try:
            r = requests.get(f"{self.BASE_URL}/defi/v3/ohlcv", headers=self._h(chain),
                             params=params, timeout=15)
            return r.json().get("data", []) if r.status_code == 200 else []
        except Exception as e:
            return [{"error": str(e)}]

    def get_wallet_token_list(self, wallet_address: str, chain: str = DEFAULT_CHAIN) -> List[Dict]:
        try:
            r = requests.get(f"{self.BASE_URL}/v1/wallet/token_list", headers=self._h(chain),
                             params={"wallet": wallet_address}, timeout=15)
            return r.json().get("data", {}).get("tokens", []) if r.status_code == 200 else []
        except Exception as e:
            return [{"error": str(e)}]

    def get_wallet_tx_list(self, wallet_address: str, chain: str = DEFAULT_CHAIN,
                           page: int = 1, page_size: int = 20) -> List[Dict]:
        try:
            r = requests.get(f"{self.BASE_URL}/v1/wallet/tx_list", headers=self._h(chain),
                             params={"wallet": wallet_address, "page": page, "page_size": page_size},
                             timeout=15)
            return r.json().get("data", {}).get("txList", []) if r.status_code == 200 else []
        except Exception as e:
            return [{"error": str(e)}]


class WhaleTracker:
    def __init__(self, api_key: Optional[str] = None, default_chain: str = DEFAULT_CHAIN):
        self.api = BirdeyeAPI(api_key)
        self.default_chain = default_chain.lower()
        self.tracked_wallets: Dict[str, WhaleWallet] = {}
        self.watchlist: List[Dict] = []

    def _resolve_chain(self, chain: Optional[str]) -> str:
        c = (chain or self.default_chain).lower()
        if c not in SUPPORTED_CHAINS:
            raise ValueError(f"Unsupported chain '{c}'. Supported: {', '.join(SUPPORTED_CHAINS)}")
        return c

    def add_to_watchlist(self, wallet_address: str, chain: Optional[str] = None) -> str:
        chain = self._resolve_chain(chain)
        entry = {"address": wallet_address, "chain": chain}
        if entry not in self.watchlist:
            self.watchlist.append(entry)
            return f"Added {wallet_address} ({chain}) to watchlist. Total: {len(self.watchlist)}"
        return "Wallet already in watchlist."

    def analyze_wallet(self, wallet_address: str, chain: Optional[str] = None) -> WhaleWallet:
        chain = self._resolve_chain(chain)
        portfolio = self.api.get_wallet_portfolio(wallet_address, chain)
        tokens = portfolio.get("tokens", [])
        total_value = sum(float(t.get("value_usd", 0)) for t in tokens)
        most_held = sorted(tokens, key=lambda x: float(x.get("value_usd", 0)), reverse=True)[:10]
        wallet = WhaleWallet(
            address=wallet_address, chain=chain, total_holdings=total_value,
            recent_activity=[], most_held_tokens=most_held,
            last_updated=datetime.now().isoformat(),
        )
        self.tracked_wallets[f"{chain}:{wallet_address}"] = wallet
        return wallet

    def find_whale_trades(self, min_value_usd: float = 10000, chain: Optional[str] = None) -> List[Dict]:
        chain = self._resolve_chain(chain)
        trending = self.api.get_trending_tokens(chain)
        whale_trades = []
        for token in trending[:10]:
            token_address = token.get("address", "")
            if not token_address:
                continue
            trades = self.api.get_token_trades(token_address, chain, limit=50)
            for trade in trades:
                value_usd = float(trade.get("value_usd", 0))
                if value_usd >= min_value_usd:
                    whale_trades.append({
                        "chain": chain,
                        "token": token.get("symbol", "Unknown"),
                        "token_address": token_address,
                        "wallet": trade.get("owner", ""),
                        "type": trade.get("type", ""),
                        "value_usd": value_usd,
                        "timestamp": trade.get("block_unix_time", 0),
                    })
        return sorted(whale_trades, key=lambda x: x["value_usd"], reverse=True)

    def find_hidden_gems(self, min_lp_size: float = 2000, min_volume_1h: float = 10000,
                         max_age_hours: int = 24, chain: Optional[str] = None) -> List[Dict]:
        chain = self._resolve_chain(chain)
        trending = self.api.get_trending_tokens(chain, time_frame="1h")
        gems = []
        for token in trending:
            token_address = token.get("address", "")
            if not token_address:
                continue
            overview = self.api.get_token_overview(token_address, chain)
            security = self.api.get_token_security(token_address, chain)
            liquidity = float(overview.get("liquidity", 0))
            volume_1h = float(overview.get("v1h", 0))
            created_at = overview.get("created_at", 0)
            age_hours = (time.time() - created_at) / 3600 if created_at else 999
            is_mintable = security.get("is_mintable", True)
            top_holders = security.get("top_10_holder_percent", 100)
            if (liquidity >= min_lp_size and volume_1h >= min_volume_1h and
                    age_hours <= max_age_hours and not is_mintable and top_holders < 50):
                gems.append({
                    "chain": chain,
                    "token": token.get("symbol", "Unknown"),
                    "address": token_address,
                    "liquidity_usd": liquidity,
                    "volume_1h_usd": volume_1h,
                    "age_hours": round(age_hours, 2),
                    "holder_count": overview.get("holder", 0),
                    "price": overview.get("price", 0),
                    "score": self._gem_score(overview, security),
                })
        return sorted(gems, key=lambda x: x["score"], reverse=True)

    def _gem_score(self, overview: Dict, security: Dict) -> float:
        score = 0.0
        v1h  = float(overview.get("v1h", 0))
        v24h = float(overview.get("v24h", 1))
        if v24h > 0:
            score += min((v1h / (v24h / 24)) * 10, 30)
        score += min(float(overview.get("holder_change_24h", 0)) / 50, 30)
        score += min(float(overview.get("liquidity", 0)) / 1000, 20)
        if not security.get("is_mintable"):
            score += 10
        if security.get("top_10_holder_percent", 100) < 30:
            score += 10
        return min(score, 100)

    def analyze_pump_dump_signals(self, token_address: str, chain: Optional[str] = None) -> TokenSignal:
        chain = self._resolve_chain(chain)
        overview = self.api.get_token_overview(token_address, chain)
        trades   = self.api.get_token_trades(token_address, chain, limit=100)
        security = self.api.get_token_security(token_address, chain)
        red_flags, green_flags = [], []

        large_sells = [t for t in trades if t.get("type") == "sell" and float(t.get("value_usd", 0)) > 5000]
        large_buys  = [t for t in trades if t.get("type") == "buy"  and float(t.get("value_usd", 0)) > 5000]
        if len(large_sells) > len(large_buys) * 2:
            red_flags.append("Large wallet outflows detected")
        if float(overview.get("liquidity_change_24h", 0)) < -20:
            red_flags.append("LP pulled significantly")
        holder_change = float(overview.get("holder_change_24h", 0))
        if holder_change < -100:
            red_flags.append("Holder count dropping")
        elif holder_change > 500:
            green_flags.append("New holders spike")
        v1h  = float(overview.get("v1h", 0))
        v24h = float(overview.get("v24h", 1))
        if v24h > 0 and (v1h / (v24h / 24)) > 10:
            green_flags.append("Volume 10x in 1H")
        if sum(1 for t in large_buys if len(t.get("owner", "")) > 30) > 3:
            green_flags.append("Smart wallets accumulating")
        dev = security.get("owner_address", "")
        if dev and any(t.get("owner") == dev and t.get("type") == "sell" for t in trades):
            red_flags.append("Dev wallet selling")

        if len(red_flags) > len(green_flags):
            sig, conf, inds = "dump", min(len(red_flags) / 5, 1.0), red_flags
        else:
            sig, conf, inds = "pump", min(len(green_flags) / 5, 1.0), green_flags

        return TokenSignal(
            token_address=token_address, token_name=overview.get("symbol", "Unknown"),
            chain=chain, signal_type=sig, confidence=conf,
            indicators=inds, timestamp=datetime.now().isoformat(),
        )

    def run_daily_scan(self, chains: Optional[List[str]] = None) -> Dict:
        target_chains = [self._resolve_chain(c) for c in (chains or list(SUPPORTED_CHAINS.keys()))]
        results = {
            "timestamp": datetime.now().isoformat(),
            "chains_scanned": target_chains,
            "trending_analysis": [], "whale_trades": [],
            "hidden_gems": [], "watchlist_updates": [],
            "profitable_traders": {}, "security_alerts": [],
        }

        for chain in target_chains:
            try:
                traders = self.api.get_profitable_traders(chain=chain, time_frame="7D", limit=20)
                if traders and "error" not in traders[0]:
                    results["profitable_traders"][chain] = traders
            except Exception:
                pass

            trending = self.api.get_trending_tokens(chain, time_frame="24h")
            for token in trending[:20]:
                token_address = token.get("address", "")
                if not token_address:
                    continue
                security = self.api.get_token_security(token_address, chain)
                rug_risk, risk_factors = 0, []
                if security.get("is_mintable"):
                    rug_risk += 30; risk_factors.append("mintable")
                if security.get("freeze_authority"):
                    rug_risk += 25; risk_factors.append("freeze_authority")
                top10 = security.get("top_10_holder_percent", 0)
                if top10 > 50: rug_risk += 25
                if top10 > 80: rug_risk += 20
                if rug_risk >= 50:
                    results["security_alerts"].append({
                        "chain": chain, "token": token.get("symbol", "Unknown"),
                        "address": token_address, "rug_risk_score": rug_risk,
                        "risk_factors": risk_factors, "risk_level": "HIGH",
                    })
                signal = self.analyze_pump_dump_signals(token_address, chain)
                sd = signal.to_dict()
                sd["rug_risk_score"] = rug_risk
                results["trending_analysis"].append({
                    "chain": chain, "token": token.get("symbol"),
                    "address": token_address, "signal": sd,
                })

            results["whale_trades"].extend(self.find_whale_trades(min_value_usd=10000, chain=chain))
            results["hidden_gems"].extend(self.find_hidden_gems(chain=chain))

        results["whale_trades"].sort(key=lambda x: x["value_usd"], reverse=True)
        results["hidden_gems"].sort(key=lambda x: x["score"], reverse=True)
        results["whale_trades"] = results["whale_trades"][:10]
        results["hidden_gems"]  = results["hidden_gems"][:10]

        for entry in self.watchlist:
            wallet_data = self.analyze_wallet(entry["address"], entry["chain"])
            try:
                pnl = self.api.get_wallet_pnl(entry["address"], entry["chain"])
                pnl_summary = {
                    "realized_profit":   pnl.get("realized_profit", 0),
                    "unrealized_profit": pnl.get("unrealized_profit", 0),
                    "win_rate":          pnl.get("win_rate", 0),
                    "total_trades":      pnl.get("total_trades", 0),
                }
            except Exception:
                pnl_summary = {}
            results["watchlist_updates"].append({
                "chain": entry["chain"], "wallet": entry["address"],
                "total_holdings": wallet_data.total_holdings,
                "top_tokens": wallet_data.most_held_tokens[:5],
                "pnl_summary": pnl_summary,
            })

        return results

    def format_report(self, scan_results: Dict) -> str:
        chains = ", ".join(c.upper() for c in scan_results.get("chains_scanned", []))
        report = [
            "=" * 70,
            "AZALYST WHALE TRACKING REPORT",
            f"Generated : {scan_results['timestamp']}",
            f"Chains    : {chains}",
            "=" * 70, "",
        ]
        report.append("TRENDING TOKEN SIGNALS")
        report.append("-" * 70)
        for item in scan_results["trending_analysis"][:15]:
            s = item["signal"]
            report.append(
                f"[{item['chain'].upper():10}] {(item['token'] or '?'):12} "
                f"{s['signal_type'].upper():5} {s['confidence']:.0%}  "
                f"{', '.join(s['indicators'][:2])}"
            )
        report.append("")
        report.append("TOP WHALE TRADES")
        report.append("-" * 70)
        for t in scan_results["whale_trades"]:
            w = t["wallet"]
            short = f"{w[:6]}...{w[-4:]}" if len(w) > 12 else w
            report.append(
                f"[{t['chain'].upper():10}] ${t['value_usd']:>12,.0f}  "
                f"{t['type'].upper():5}  {t['token']:12}  {short}"
            )
        report.append("")
        report.append("HIDDEN GEMS")
        report.append("-" * 70)
        for g in scan_results["hidden_gems"]:
            report.append(
                f"[{g['chain'].upper():10}] {g['token']:12}  Score {g['score']:5.1f}/100  "
                f"Age {g['age_hours']:.1f}h  Vol1H ${g['volume_1h_usd']:,.0f}  "
                f"LP ${g['liquidity_usd']:,.0f}"
            )
        report.append("")
        if scan_results.get("security_alerts"):
            report.append("SECURITY ALERTS")
            report.append("-" * 70)
            for a in scan_results["security_alerts"]:
                report.append(
                    f"[{a['chain'].upper():10}] {a['token']:12}  "
                    f"Risk {a['rug_risk_score']:3}/100  {', '.join(a['risk_factors'])}"
                )
            report.append("")
        if scan_results.get("profitable_traders"):
            report.append("TOP PROFITABLE TRADERS")
            report.append("-" * 70)
            for chain, traders in scan_results["profitable_traders"].items():
                for i, t in enumerate(traders[:5], 1):
                    w = t.get("wallet_address", t.get("address", "?"))
                    short = f"{w[:6]}...{w[-4:]}" if len(w) > 12 else w
                    report.append(
                        f"[{chain.upper():10}] #{i}  {short}  "
                        f"PnL ${t.get('pnl', 0):,.0f}  Vol ${t.get('volume', 0):,.0f}"
                    )
            report.append("")
        return "\n".join(report)


# ── Agent-facing utility functions ──────────────────────────────────────────

def track_whale(wallet_address: str, chain: str = DEFAULT_CHAIN, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key, default_chain=chain)
    result = tracker.add_to_watchlist(wallet_address, chain)
    wallet_data = tracker.analyze_wallet(wallet_address, chain)
    return f"{result}\n\nWallet Analysis ({chain}):\n" + json.dumps(wallet_data.to_dict(), indent=2)


def find_pumps(chain: str = DEFAULT_CHAIN, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key, default_chain=chain)
    gems = tracker.find_hidden_gems(chain=chain)
    if not gems:
        return f"No hidden gems found on {chain} matching current criteria."
    lines = [f"POTENTIAL PUMP TOKENS — {chain.upper()}\n"]
    for i, g in enumerate(gems[:10], 1):
        lines.append(f"{i:2}. {g['token']:12}  Score {g['score']:5.1f}/100  "
                     f"${g['volume_1h_usd']:,.0f} vol  {g['age_hours']:.1f}h old")
    return "\n".join(lines)


def analyze_token(token_address: str, chain: str = DEFAULT_CHAIN, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key, default_chain=chain)
    signal = tracker.analyze_pump_dump_signals(token_address, chain)
    lines = [
        f"TOKEN SIGNAL — {chain.upper()}",
        f"Token      : {signal.token_name}",
        f"Signal     : {signal.signal_type.upper()}",
        f"Confidence : {signal.confidence:.0%}",
        "", "Indicators :",
    ]
    for ind in signal.indicators:
        lines.append(f"  - {ind}")
    return "\n".join(lines)


def daily_scan(chains: Optional[List[str]] = None, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    results = tracker.run_daily_scan(chains=chains)
    try:
        os.makedirs("reports", exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(f"reports/daily_scan_{ts}.json", "w") as f:
            json.dump(results, f, indent=2, default=str)
    except Exception:
        pass
    return tracker.format_report(results)


def get_profitable_traders(chain: str = DEFAULT_CHAIN, time_frame: str = "7D",
                            api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    traders = tracker.api.get_profitable_traders(chain=chain, time_frame=time_frame)
    if not traders or "error" in traders[0]:
        return f"Error fetching profitable traders: {traders}"
    try:
        os.makedirs("reports", exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d")
        with open(f"reports/profitable_traders_{ts}.json", "w") as f:
            json.dump({"chain": chain, "time_frame": time_frame, "traders": traders}, f, indent=2)
    except Exception:
        pass
    lines = [f"TOP PROFITABLE TRADERS ({chain.upper()}, {time_frame})\n"]
    for i, t in enumerate(traders[:20], 1):
        w = t.get("wallet_address", t.get("address", "?"))
        short = f"{w[:8]}...{w[-6:]}" if len(w) > 16 else w
        lines.append(f"{i:2}. {short}  PnL ${t.get('pnl', 0):,.2f}  "
                     f"Vol ${t.get('volume', 0):,.2f}  Trades {t.get('trade_count', t.get('trades', 0))}")
    return "\n".join(lines)


def get_wallet_pnl(wallet_address: str, chain: str = DEFAULT_CHAIN, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    data = tracker.api.get_wallet_pnl(wallet_address, chain=chain)
    if not data or "error" in data:
        return f"Error fetching wallet PnL: {data}"
    r = data.get("realized_profit", 0)
    u = data.get("unrealized_profit", 0)
    return "\n".join([
        f"WALLET PnL ({chain.upper()})",
        f"Wallet            : {wallet_address[:8]}...{wallet_address[-6:]}",
        f"Realized Profit   : ${r:,.2f}",
        f"Unrealized Profit : ${u:,.2f}",
        f"Total PnL         : ${r + u:,.2f}",
        f"Win Rate          : {data.get('win_rate', 0) * 100:.1f}%",
        f"Total Trades      : {data.get('total_trades', 0)}",
    ])


def get_top_traders(token_address: str, chain: str = DEFAULT_CHAIN,
                    time_frame: str = "24h", api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    traders = tracker.api.get_top_traders(token_address, chain=chain, time_frame=time_frame)
    if not traders or "error" in traders[0]:
        return f"Error fetching top traders: {traders}"
    lines = [f"TOP TRADERS ({chain.upper()}, {time_frame})\n"]
    for i, t in enumerate(traders[:10], 1):
        w = t.get("wallet_address", t.get("address", "?"))
        short = f"{w[:8]}...{w[-6:]}" if len(w) > 16 else w
        lines.append(f"{i:2}. {short}  Vol ${t.get('volume', 0):,.2f}  "
                     f"PnL ${t.get('pnl', 0):,.2f}")
    return "\n".join(lines)


def check_token_security(token_address: str, chain: str = DEFAULT_CHAIN, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    sec = tracker.api.get_token_security(token_address, chain=chain)
    if not sec or "error" in sec:
        return f"Error fetching token security: {sec}"
    is_mintable = sec.get("is_mintable", False)
    freeze_auth = sec.get("freeze_authority", False)
    top10_pct   = sec.get("top_10_holder_percent", 0)
    rug_risk, factors = 0, []
    if is_mintable:   rug_risk += 30; factors.append("Mintable supply")
    if freeze_auth:   rug_risk += 25; factors.append("Freeze authority")
    if top10_pct > 50: rug_risk += 25; factors.append(f"Top-10 hold {top10_pct}%")
    if top10_pct > 80: rug_risk += 20; factors.append("Extreme concentration")
    level = "HIGH" if rug_risk >= 50 else "MEDIUM" if rug_risk >= 25 else "LOW"
    return "\n".join([
        f"TOKEN SECURITY ({chain.upper()}) — {level}",
        f"Token          : {token_address[:8]}...{token_address[-6:]}",
        f"Risk Score     : {rug_risk}/100",
        f"Mintable       : {'Yes [RISK]' if is_mintable else 'No  [OK]'}",
        f"Freeze Auth    : {'Yes [RISK]' if freeze_auth else 'No  [OK]'}",
        f"Top-10 Holders : {top10_pct}%",
        f"Risk Factors   : {', '.join(factors) if factors else 'None'}",
    ])


def get_new_listings(chain: str = DEFAULT_CHAIN, limit: int = 50, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    listings = tracker.api.get_new_listings(chain=chain, limit=limit)
    if not listings or "error" in listings[0]:
        return f"Error fetching new listings: {listings}"
    lines = [f"NEW LISTINGS ({chain.upper()})\n"]
    for i, t in enumerate(listings[:20], 1):
        addr = t.get("address", "")
        short = f"{addr[:8]}...{addr[-6:]}" if len(addr) > 16 else addr
        age_min = round((time.time() - t.get("created_at", 0)) / 60, 1) if t.get("created_at") else "?"
        lines.append(f"{i:2}. {t.get('symbol','?'):12} {short}  {age_min} min old")
    return "\n".join(lines)


def get_token_creation_info(token_address: str, chain: str = DEFAULT_CHAIN, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    info = tracker.api.get_token_creation_info(token_address, chain=chain)
    if not info or "error" in info:
        return f"Error fetching creation info: {info}"
    dep = info.get("deployer", "")
    short_dep = f"{dep[:8]}...{dep[-6:]}" if len(dep) > 16 else dep
    created = info.get("created_at", 0)
    return "\n".join([
        f"TOKEN CREATION INFO",
        f"Token    : {token_address[:8]}...{token_address[-6:]}",
        f"Deployer : {short_dep}",
        f"Created  : {datetime.fromtimestamp(created).isoformat() if created else 'Unknown'}",
        f"Supply   : {info.get('initial_supply', 0):,}",
    ])


def get_holder_list(token_address: str, chain: str = DEFAULT_CHAIN,
                    limit: int = 100, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    holders = tracker.api.get_holder_list(token_address, chain=chain, limit=limit)
    if not holders or "error" in holders[0]:
        return f"Error fetching holders: {holders}"
    lines = [f"TOP HOLDERS — {token_address[:8]}...\n"]
    for i, h in enumerate(holders[:15], 1):
        w = h.get("owner", "")
        short = f"{w[:8]}...{w[-6:]}" if len(w) > 16 else w
        lines.append(f"{i:2}. {short}  {h.get('percent', 0):.2f}%  ({h.get('balance', 0):,.0f})")
    return "\n".join(lines)


def get_wallet_pnl_details(wallet_address: str, chain: str = DEFAULT_CHAIN,
                            limit: int = 100, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    details = tracker.api.get_wallet_pnl_details(wallet_address, chain=chain, limit=limit)
    if not details or "error" in details[0]:
        return f"Error fetching PnL details: {details}"
    lines = [f"WALLET PnL DETAILS — {wallet_address[:8]}...\n"]
    total = 0.0
    for d in details[:15]:
        pnl = d.get("realized_pnl", 0)
        total += pnl
        sign = "+" if pnl >= 0 else ""
        lines.append(f"  {d.get('token_symbol','?'):12}  {sign}${pnl:,.2f}")
    lines.append(f"\nTotal: ${total:,.2f}")
    return "\n".join(lines)


def get_trader_txs(wallet_address: str, chain: str = DEFAULT_CHAIN,
                   start_time: int = None, end_time: int = None,
                   limit: int = 50, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    txs = tracker.api.get_trader_txs(wallet_address, chain=chain,
                                     start_time=start_time, end_time=end_time, limit=limit)
    if not txs or "error" in txs[0]:
        return f"Error fetching transactions: {txs}"
    lines = [f"TRADER TXs — {wallet_address[:8]}...\n"]
    for tx in txs[:15]:
        lines.append(f"  {tx.get('type','?').upper():5}  {tx.get('token_symbol','?'):12}  "
                     f"${tx.get('value_usd', 0):,.2f}")
    return "\n".join(lines)


def get_ohlcv(token_address: str, chain: str = DEFAULT_CHAIN,
              timeframe: str = "1h", api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    candles = tracker.api.get_ohlcv(token_address, chain=chain, timeframe=timeframe)
    if not candles or "error" in candles[0]:
        return f"Error fetching OHLCV: {candles}"
    lines = [f"OHLCV — {token_address[:8]}...  ({timeframe})\n"]
    for c in candles[-10:]:
        t = datetime.fromtimestamp(c.get("time", 0)).strftime("%m/%d %H:%M")
        lines.append(f"  {t}  O:{c.get('o',0):.6f}  H:{c.get('h',0):.6f}  "
                     f"L:{c.get('l',0):.6f}  C:{c.get('c',0):.6f}")
    return "\n".join(lines)


def get_wallet_token_list(wallet_address: str, chain: str = DEFAULT_CHAIN, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    tokens = tracker.api.get_wallet_token_list(wallet_address, chain=chain)
    if not tokens or "error" in tokens[0]:
        return f"Error fetching token list: {tokens}"
    lines = [f"WALLET HOLDINGS — {wallet_address[:8]}...\n"]
    total = 0.0
    for t in tokens[:15]:
        v = float(t.get("value_usd", 0))
        total += v
        lines.append(f"  {t.get('symbol','?'):12}  ${v:,.2f}")
    lines.append(f"\nTotal: ${total:,.2f}")
    return "\n".join(lines)


def get_wallet_tx_list(wallet_address: str, chain: str = DEFAULT_CHAIN,
                       page: int = 1, page_size: int = 20, api_key: Optional[str] = None) -> str:
    tracker = WhaleTracker(api_key)
    txs = tracker.api.get_wallet_tx_list(wallet_address, chain=chain, page=page, page_size=page_size)
    if not txs or "error" in txs[0]:
        return f"Error fetching tx list: {txs}"
    lines = [f"WALLET TXs — {wallet_address[:8]}...\n"]
    for tx in txs[:15]:
        sig  = tx.get("signature", "")[:8] + "..."
        bt   = tx.get("block_time", 0)
        ts   = datetime.fromtimestamp(bt).strftime("%m/%d %H:%M") if bt else "?"
        lines.append(f"  [{ts}]  {sig}  {tx.get('type','?').upper()}")
    return "\n".join(lines)
