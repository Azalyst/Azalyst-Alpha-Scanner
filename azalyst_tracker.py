"""
Azalyst Alpha Scanner — Multi-Chain Tracker
Sources: DexScreener + GeckoTerminal + GoPlus + Helius (Solana)
Chains: Solana, Ethereum, Base, Arbitrum, BNB, Avalanche, Polygon, Optimism, zkSync
"""

from __future__ import annotations

import os
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("azalyst_tracker")

# ---------------------------------------------------------------------------
# API base URLs
# ---------------------------------------------------------------------------
DS_BASE = "https://api.dexscreener.com"
GT_BASE = "https://api.geckoterminal.com/api/v2"
GP_BASE = "https://api.gopluslabs.io/api/v1"
HELIUS_RPC_BASE = "https://mainnet.helius-rpc.com"
HELIUS_API_BASE = "https://api.helius.xyz/v0"

# ---------------------------------------------------------------------------
# Chain mappings
# ---------------------------------------------------------------------------
GT_NETWORK: Dict[str, str] = {
    "solana": "solana",
    "ethereum": "eth",
    "base": "base",
    "arbitrum": "arbitrum",
    "bnb": "bsc",
    "avalanche": "avax",
    "polygon": "polygon_pos",
    "optimism": "optimism",
    "zksync": "zksync",
}

DS_CHAIN: Dict[str, str] = {
    "solana": "solana",
    "ethereum": "ethereum",
    "base": "base",
    "arbitrum": "arbitrum",
    "bnb": "bsc",
    "avalanche": "avalanche",
    "polygon": "polygon",
    "optimism": "optimism",
    "zksync": "zksync",
}

GP_CHAIN_ID: Dict[str, str] = {
    "ethereum": "1",
    "bnb": "56",
    "polygon": "137",
    "avalanche": "43114",
    "arbitrum": "42161",
    "base": "8453",
    "optimism": "10",
    "zksync": "324",
    "solana": "solana",
}

SUPPORTED_CHAINS: List[str] = list(GT_NETWORK.keys())


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _get(url: str, params: Optional[Dict] = None, headers: Optional[Dict] = None,
         timeout: int = 20) -> Optional[Dict]:
    """HTTP GET with basic error handling; returns parsed JSON or None."""
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("GET %s failed: %s", url, exc)
        return None


def _post(url: str, payload: Dict, timeout: int = 20) -> Optional[Dict]:
    """HTTP POST JSON with basic error handling."""
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("POST %s failed: %s", url, exc)
        return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# AzalystAPI
# ---------------------------------------------------------------------------
class AzalystAPI:
    """
    Multi-source crypto data API client.
    Aggregates DexScreener, GeckoTerminal, GoPlus, and Helius.

    Parameters
    ----------
    api_key : str, optional
        Helius API key (for Solana-specific endpoints).
        Falls back to HELIUS_API_KEY environment variable.
    """

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key: str = api_key or os.environ.get("HELIUS_API_KEY", "")
        self._pair_cache: Dict[str, str] = {}  # token_address -> pair_address

    # ------------------------------------------------------------------
    # Trending tokens
    # ------------------------------------------------------------------
    def get_trending_tokens(self, chain: str, time_frame: str = "24h") -> List[Dict]:
        """Return trending tokens for *chain* via GeckoTerminal trending_pools."""
        network = GT_NETWORK.get(chain.lower())
        if not network:
            logger.warning("Unsupported chain for trending: %s", chain)
            return []

        url = f"{GT_BASE}/networks/{network}/trending_pools"
        data = _get(url, params={"include": "base_token"})
        if not data:
            return []

        results: List[Dict] = []
        for pool in data.get("data", []):
            attrs = pool.get("attributes", {})
            relationships = pool.get("relationships", {})

            # Try to get base token info from included data
            base_token_data = relationships.get("base_token", {}).get("data", {})
            token_address = ""
            if base_token_data:
                token_id = base_token_data.get("id", "")
                # id format: "solana_<address>" or "eth_<address>"
                parts = token_id.split("_", 1)
                token_address = parts[1] if len(parts) == 2 else token_id

            results.append({
                "address": token_address,
                "symbol": attrs.get("base_token_price_quote_token", ""),
                "name": attrs.get("name", ""),
                "price": _safe_float(attrs.get("base_token_price_usd")),
                "liquidity": _safe_float(attrs.get("reserve_in_usd")),
                "v24h": _safe_float(attrs.get("volume_usd", {}).get("h24") if isinstance(attrs.get("volume_usd"), dict) else attrs.get("volume_usd_h24")),
                "price_change_24h_pct": _safe_float(attrs.get("price_change_percentage", {}).get("h24") if isinstance(attrs.get("price_change_percentage"), dict) else 0),
                "chain": chain,
            })
        return results

    # ------------------------------------------------------------------
    # Wallet portfolio
    # ------------------------------------------------------------------
    def get_wallet_portfolio(self, wallet: str, chain: str) -> Dict:
        """Return wallet portfolio. Helius for Solana; empty for other chains."""
        chain_l = chain.lower()
        if chain_l != "solana" or not self.api_key:
            return {}

        url = f"{HELIUS_API_BASE}/addresses/{wallet}/balances"
        data = _get(url, params={"api-key": self.api_key})
        if not data:
            return {}

        tokens = []
        for tok in data.get("tokens", []):
            tokens.append({
                "symbol": tok.get("symbol", ""),
                "mint": tok.get("mint", ""),
                "amount": _safe_float(tok.get("amount")),
                "decimals": tok.get("decimals", 0),
            })
        return {
            "wallet": wallet,
            "chain": chain,
            "sol_balance": _safe_float(data.get("nativeBalance", 0)) / 1e9,
            "tokens": tokens,
        }

    # ------------------------------------------------------------------
    # Token overview
    # ------------------------------------------------------------------
    def get_token_overview(self, address: str, chain: str) -> Dict:
        """
        Return normalized token overview from DexScreener.
        Keys: address, symbol, name, price, liquidity, v1h, v24h, v5m,
              price_change_1h_pct, price_change_24h_pct, price_change_5m_pct,
              fdv, mc, created_at, pair_address, holder, holder_change_24h
        """
        url = f"{DS_BASE}/latest/dex/tokens/{address}"
        data = _get(url)
        if not data:
            return {"error": "DexScreener request failed", "address": address}

        ds_chain = DS_CHAIN.get(chain.lower(), chain.lower())
        pairs = [
            p for p in data.get("pairs", [])
            if p.get("chainId", "").lower() == ds_chain
        ]
        if not pairs:
            # Fall back to any pair
            pairs = data.get("pairs", [])
        if not pairs:
            return {"error": "No pairs found", "address": address}

        # Sort by liquidity to get the best pair
        pairs.sort(key=lambda p: _safe_float(p.get("liquidity", {}).get("usd", 0)), reverse=True)
        pair = pairs[0]

        pair_address = pair.get("pairAddress", "")
        if pair_address:
            self._pair_cache[address] = pair_address

        base = pair.get("baseToken", {})
        liquidity = pair.get("liquidity", {})
        volume = pair.get("volume", {})
        price_change = pair.get("priceChange", {})
        txns = pair.get("txns", {})

        return {
            "address": address,
            "symbol": base.get("symbol", ""),
            "name": base.get("name", ""),
            "price": _safe_float(pair.get("priceUsd")),
            "liquidity": _safe_float(liquidity.get("usd")),
            "v1h": _safe_float(volume.get("h1")),
            "v24h": _safe_float(volume.get("h24")),
            "v5m": _safe_float(volume.get("m5")),
            "price_change_1h_pct": _safe_float(price_change.get("h1")),
            "price_change_24h_pct": _safe_float(price_change.get("h24")),
            "price_change_5m_pct": _safe_float(price_change.get("m5")),
            "fdv": _safe_float(pair.get("fdv")),
            "mc": _safe_float(pair.get("marketCap")),
            "created_at": pair.get("pairCreatedAt", 0),
            "pair_address": pair_address,
            "holder": 0,           # Not available on DexScreener
            "holder_change_24h": 0,
        }

    # ------------------------------------------------------------------
    # Token trades
    # ------------------------------------------------------------------
    def get_token_trades(self, address: str, chain: str, limit: int = 100) -> List[Dict]:
        """
        Return recent trades via GeckoTerminal pool trades.
        Keys: type, side, value_usd, owner, block_unix_time
        """
        network = GT_NETWORK.get(chain.lower())
        if not network:
            return []

        # Resolve pair address - check cache first, then DexScreener
        pair_address = self._pair_cache.get(address)
        if not pair_address:
            overview = self.get_token_overview(address, chain)
            pair_address = overview.get("pair_address", "")
        if not pair_address:
            return []

        url = f"{GT_BASE}/networks/{network}/pools/{pair_address}/trades"
        data = _get(url, params={"trade_volume_in_usd_greater_than": 0})
        if not data:
            return []

        trades = []
        for trade in data.get("data", [])[:limit]:
            attrs = trade.get("attributes", {})
            kind = attrs.get("kind", "buy")
            trades.append({
                "type": "buy" if kind == "buy" else "sell",
                "side": kind,
                "value_usd": _safe_float(attrs.get("volume_in_usd")),
                "owner": attrs.get("tx_from_address", ""),
                "block_unix_time": int(
                    datetime.fromisoformat(
                        attrs["block_timestamp"].replace("Z", "+00:00")
                    ).timestamp()
                ) if attrs.get("block_timestamp") else 0,
            })
        return trades

    # ------------------------------------------------------------------
    # Token security
    # ------------------------------------------------------------------
    def get_token_security(self, address: str, chain: str) -> Dict:
        """
        Return token security info via GoPlus.
        Keys: is_mintable, freeze_authority, top_10_holder_percent, owner_address
        """
        chain_id = GP_CHAIN_ID.get(chain.lower())
        if not chain_id:
            return {"error": "Unsupported chain for GoPlus", "address": address}

        if chain_id == "solana":
            url = f"{GP_BASE}/solana/token_security"
            data = _get(url, params={"contract_addresses": address})
        else:
            url = f"{GP_BASE}/token_security/{chain_id}"
            data = _get(url, params={"contract_addresses": address})

        if not data or data.get("code") != 1:
            return {"error": "GoPlus request failed", "address": address}

        result = data.get("result", {})
        token_info = result.get(address.lower()) or result.get(address) or {}

        if chain_id == "solana":
            return {
                "is_mintable": int(token_info.get("mintable", 0)),
                "freeze_authority": token_info.get("freezeable", "0"),
                "top_10_holder_percent": _safe_float(token_info.get("top10HolderPercent")),
                "owner_address": token_info.get("ownerAddress", ""),
            }
        else:
            return {
                "is_mintable": int(token_info.get("is_mintable", 0)),
                "freeze_authority": token_info.get("owner_change_balance", "0"),
                "top_10_holder_percent": _safe_float(token_info.get("holder_count")) and _safe_float(
                    # GoPlus returns top10HolderRatio for EVM
                    token_info.get("top10HolderRatio", 0)
                ) * 100,
                "owner_address": token_info.get("owner_address", ""),
            }

    # ------------------------------------------------------------------
    # Profitable traders (not available free)
    # ------------------------------------------------------------------
    def get_profitable_traders(self, chain: str, time_frame: str = "7D",
                                limit: int = 20) -> List[Dict]:
        """Not available via free APIs. Returns empty list."""
        return []

    # ------------------------------------------------------------------
    # Wallet PnL
    # ------------------------------------------------------------------
    def get_wallet_pnl(self, wallet: str, chain: str) -> Dict:
        """
        Return wallet PnL summary.
        Keys: realized_profit, unrealized_profit, win_rate, total_trades
        Helius Solana only; returns empty structure for other chains.
        """
        if chain.lower() != "solana" or not self.api_key:
            return {
                "realized_profit": 0.0,
                "unrealized_profit": 0.0,
                "win_rate": 0.0,
                "total_trades": 0,
            }

        url = f"{HELIUS_API_BASE}/addresses/{wallet}/transactions"
        data = _get(url, params={"api-key": self.api_key, "type": "SWAP", "limit": 100})
        if not data or not isinstance(data, list):
            return {"realized_profit": 0.0, "unrealized_profit": 0.0,
                    "win_rate": 0.0, "total_trades": 0}

        wins = 0
        total = 0
        realized = 0.0
        for tx in data:
            events = tx.get("events", {})
            swap = events.get("swap", {})
            if not swap:
                continue
            total += 1
            # Approximate PnL from tokenOutputs vs tokenInputs
            out_val = sum(_safe_float(t.get("tokenAmount", 0)) for t in swap.get("tokenOutputs", []))
            in_val = sum(_safe_float(t.get("tokenAmount", 0)) for t in swap.get("tokenInputs", []))
            pnl = out_val - in_val
            realized += pnl
            if pnl > 0:
                wins += 1

        return {
            "realized_profit": realized,
            "unrealized_profit": 0.0,
            "win_rate": (wins / total * 100) if total > 0 else 0.0,
            "total_trades": total,
        }

    # ------------------------------------------------------------------
    # Top traders (not available free)
    # ------------------------------------------------------------------
    def get_top_traders(self, address: str, chain: str, time_frame: str = "24h",
                         limit: int = 10) -> List[Dict]:
        """Not available via free APIs. Returns empty list."""
        return []

    # ------------------------------------------------------------------
    # New listings
    # ------------------------------------------------------------------
    def get_new_listings(self, chain: str, limit: int = 50) -> List[Dict]:
        """
        Return newly listed tokens via GeckoTerminal new_pools.
        Keys: address, symbol, chain, created_at
        """
        network = GT_NETWORK.get(chain.lower())
        if not network:
            return []

        url = f"{GT_BASE}/networks/{network}/new_pools"
        data = _get(url, params={"include": "base_token"})
        if not data:
            return []

        results: List[Dict] = []
        for pool in data.get("data", [])[:limit]:
            attrs = pool.get("attributes", {})
            relationships = pool.get("relationships", {})
            base_token_data = relationships.get("base_token", {}).get("data", {})

            token_address = ""
            if base_token_data:
                token_id = base_token_data.get("id", "")
                parts = token_id.split("_", 1)
                token_address = parts[1] if len(parts) == 2 else token_id

            created_raw = attrs.get("pool_created_at", "")
            created_ts = 0
            if created_raw:
                try:
                    created_ts = int(
                        datetime.fromisoformat(
                            created_raw.replace("Z", "+00:00")
                        ).timestamp()
                    )
                except Exception:
                    pass

            results.append({
                "address": token_address,
                "symbol": attrs.get("name", "").split(" / ")[0],
                "chain": chain,
                "created_at": created_ts,
            })
        return results

    # ------------------------------------------------------------------
    # Token creation info
    # ------------------------------------------------------------------
    def get_token_creation_info(self, address: str, chain: str) -> Dict:
        """
        Return token creation info from DexScreener pairCreatedAt.
        Keys: deployer, created_at, initial_supply
        """
        url = f"{DS_BASE}/latest/dex/tokens/{address}"
        data = _get(url)
        if not data:
            return {"deployer": "", "created_at": 0, "initial_supply": 0}

        ds_chain = DS_CHAIN.get(chain.lower(), chain.lower())
        pairs = [
            p for p in data.get("pairs", [])
            if p.get("chainId", "").lower() == ds_chain
        ]
        if not pairs:
            pairs = data.get("pairs", [])

        if not pairs:
            return {"deployer": "", "created_at": 0, "initial_supply": 0}

        pairs.sort(key=lambda p: _safe_float(p.get("liquidity", {}).get("usd", 0)), reverse=True)
        pair = pairs[0]

        return {
            "deployer": pair.get("info", {}).get("header", ""),
            "created_at": pair.get("pairCreatedAt", 0),
            "initial_supply": 0,  # Not available on DexScreener
        }

    # ------------------------------------------------------------------
    # Holder list
    # ------------------------------------------------------------------
    def get_holder_list(self, address: str, chain: str, limit: int = 100) -> List[Dict]:
        """
        Return top holders list.
        Keys: owner, percent, balance
        Helius getTokenLargestAccounts for Solana; GoPlus for EVM.
        """
        chain_l = chain.lower()

        if chain_l == "solana":
            return self._helius_holder_list(address, limit)
        else:
            return self._goplus_holder_list(address, chain_l, limit)

    def _helius_holder_list(self, address: str, limit: int) -> List[Dict]:
        if not self.api_key:
            return []
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenLargestAccounts",
            "params": [address],
        }
        url = f"{HELIUS_RPC_BASE}/?api-key={self.api_key}"
        data = _post(url, payload)
        if not data:
            return []

        accounts = data.get("result", {}).get("value", [])
        total = sum(_safe_float(a.get("uiAmount", 0)) for a in accounts)

        holders = []
        for acc in accounts[:limit]:
            amount = _safe_float(acc.get("uiAmount", 0))
            holders.append({
                "owner": acc.get("address", ""),
                "percent": (amount / total * 100) if total > 0 else 0.0,
                "balance": amount,
            })
        return holders

    def _goplus_holder_list(self, address: str, chain: str, limit: int) -> List[Dict]:
        chain_id = GP_CHAIN_ID.get(chain)
        if not chain_id:
            return []
        url = f"{GP_BASE}/token_security/{chain_id}"
        data = _get(url, params={"contract_addresses": address})
        if not data or data.get("code") != 1:
            return []

        result = data.get("result", {})
        token_info = result.get(address.lower()) or result.get(address) or {}
        holders_raw = token_info.get("holders", [])

        holders = []
        for h in holders_raw[:limit]:
            holders.append({
                "owner": h.get("address", ""),
                "percent": _safe_float(h.get("percent", 0)) * 100,
                "balance": _safe_float(h.get("balance", 0)),
            })
        return holders

    # ------------------------------------------------------------------
    # Wallet PnL details
    # ------------------------------------------------------------------
    def get_wallet_pnl_details(self, wallet: str, chain: str,
                                limit: int = 100) -> List[Dict]:
        """
        Return per-token PnL breakdown.
        Keys: token_symbol, realized_pnl
        Helius Solana only.
        """
        if chain.lower() != "solana" or not self.api_key:
            return []

        url = f"{HELIUS_API_BASE}/addresses/{wallet}/transactions"
        data = _get(url, params={"api-key": self.api_key, "type": "SWAP", "limit": limit})
        if not data or not isinstance(data, list):
            return []

        pnl_map: Dict[str, float] = {}
        for tx in data:
            events = tx.get("events", {})
            swap = events.get("swap", {})
            if not swap:
                continue
            for tok in swap.get("tokenOutputs", []):
                symbol = tok.get("symbol") or tok.get("mint", "")[:8]
                pnl_map[symbol] = pnl_map.get(symbol, 0.0) + _safe_float(tok.get("tokenAmount"))
            for tok in swap.get("tokenInputs", []):
                symbol = tok.get("symbol") or tok.get("mint", "")[:8]
                pnl_map[symbol] = pnl_map.get(symbol, 0.0) - _safe_float(tok.get("tokenAmount"))

        return [
            {"token_symbol": sym, "realized_pnl": pnl}
            for sym, pnl in sorted(pnl_map.items(), key=lambda x: abs(x[1]), reverse=True)
        ]

    # ------------------------------------------------------------------
    # Trader transactions
    # ------------------------------------------------------------------
    def get_trader_txs(self, wallet: str, chain: str, start_time: Optional[int] = None,
                        end_time: Optional[int] = None, limit: int = 50) -> List[Dict]:
        """
        Return trader transaction history via Helius.
        Helius Solana only; returns empty list for other chains.
        """
        if chain.lower() != "solana" or not self.api_key:
            return []

        params: Dict[str, Any] = {"api-key": self.api_key, "limit": min(limit, 100)}
        if start_time:
            params["before"] = start_time
        if end_time:
            params["until"] = end_time

        url = f"{HELIUS_API_BASE}/addresses/{wallet}/transactions"
        data = _get(url, params=params)
        if not data or not isinstance(data, list):
            return []
        return data

    # ------------------------------------------------------------------
    # OHLCV
    # ------------------------------------------------------------------
    def get_ohlcv(self, address: str, chain: str, timeframe: str = "1h",
                   from_time: Optional[int] = None,
                   to_time: Optional[int] = None) -> List[Dict]:
        """
        Return OHLCV candles via GeckoTerminal.
        Keys: time, o, h, l, c, v
        Timeframe mapping: "1h" → "hour", "1d" → "day", "15m" → "minute"
        """
        network = GT_NETWORK.get(chain.lower())
        if not network:
            return []

        tf_map = {
            "1m": "minute", "3m": "minute", "5m": "minute",
            "15m": "minute", "30m": "minute",
            "1h": "hour", "4h": "hour",
            "1d": "day", "1w": "day",
        }
        gt_tf = tf_map.get(timeframe, "hour")

        # Resolve pair address
        pair_address = self._pair_cache.get(address)
        if not pair_address:
            overview = self.get_token_overview(address, chain)
            pair_address = overview.get("pair_address", "")
        if not pair_address:
            return []

        url = f"{GT_BASE}/networks/{network}/pools/{pair_address}/ohlcv/{gt_tf}"
        params: Dict[str, Any] = {"limit": 1000}
        if to_time:
            params["before_timestamp"] = to_time

        data = _get(url, params=params)
        if not data:
            return []

        candles_raw = (
            data.get("data", {})
            .get("attributes", {})
            .get("ohlcv_list", [])
        )

        candles = []
        for c in candles_raw:
            # GeckoTerminal: [timestamp, open, high, low, close, volume]
            if len(c) < 6:
                continue
            ts = int(c[0])
            if from_time and ts < from_time:
                continue
            if to_time and ts > to_time:
                continue
            candles.append({
                "time": ts,
                "o": _safe_float(c[1]),
                "h": _safe_float(c[2]),
                "l": _safe_float(c[3]),
                "c": _safe_float(c[4]),
                "v": _safe_float(c[5]),
            })
        return candles

    # ------------------------------------------------------------------
    # Wallet token list
    # ------------------------------------------------------------------
    def get_wallet_token_list(self, wallet: str, chain: str) -> List[Dict]:
        """
        Return tokens held in a wallet.
        Keys: symbol, value_usd
        Helius Solana; empty for other chains.
        """
        if chain.lower() != "solana" or not self.api_key:
            return []

        url = f"{HELIUS_API_BASE}/addresses/{wallet}/balances"
        data = _get(url, params={"api-key": self.api_key})
        if not data:
            return []

        tokens = []
        for tok in data.get("tokens", []):
            tokens.append({
                "symbol": tok.get("symbol", tok.get("mint", "")[:8]),
                "value_usd": _safe_float(tok.get("pricePerToken", 0)) *
                             _safe_float(tok.get("amount", 0)),
            })
        return tokens

    # ------------------------------------------------------------------
    # Wallet transaction list
    # ------------------------------------------------------------------
    def get_wallet_tx_list(self, wallet: str, chain: str, page: int = 1,
                            page_size: int = 20) -> List[Dict]:
        """
        Return paginated wallet transaction list via Helius.
        Helius Solana only.
        """
        if chain.lower() != "solana" or not self.api_key:
            return []

        url = f"{HELIUS_API_BASE}/addresses/{wallet}/transactions"
        data = _get(url, params={
            "api-key": self.api_key,
            "limit": page_size,
        })
        if not data or not isinstance(data, list):
            return []
        return data


# ---------------------------------------------------------------------------
# AzalystTracker — whale / pattern detection
# ---------------------------------------------------------------------------
class AzalystTracker:
    """
    High-level tracker that uses AzalystAPI to scan for whale activity,
    pump patterns, and token analytics.
    """

    def __init__(self, api_key: Optional[str] = None,
                 min_whale_usd: float = 10_000.0) -> None:
        self.api = AzalystAPI(api_key=api_key)
        self.min_whale_usd = min_whale_usd
        logger.info("AzalystTracker initialized (min_whale_usd=%s)", min_whale_usd)

    # ------------------------------------------------------------------
    def track_whale(self, wallet: str, chain: str = "solana") -> Dict:
        """
        Track a wallet's recent activity and holdings.
        Returns a summary dict.
        """
        portfolio = self.api.get_wallet_portfolio(wallet, chain)
        pnl = self.api.get_wallet_pnl(wallet, chain)
        txs = self.api.get_trader_txs(wallet, chain, limit=20)

        large_txs = []
        for tx in txs:
            val = 0.0
            events = tx.get("events", {})
            swap = events.get("swap", {})
            if swap:
                for t in swap.get("tokenInputs", []) + swap.get("tokenOutputs", []):
                    val = max(val, _safe_float(t.get("tokenAmount", 0)))
            if val >= self.min_whale_usd:
                large_txs.append({
                    "signature": tx.get("signature", ""),
                    "timestamp": tx.get("timestamp", 0),
                    "value_usd": val,
                    "type": tx.get("type", ""),
                })

        return {
            "wallet": wallet,
            "chain": chain,
            "portfolio": portfolio,
            "pnl": pnl,
            "large_transactions": large_txs,
            "total_large_txs": len(large_txs),
        }

    # ------------------------------------------------------------------
    def find_pumps(self, chain: str = "solana",
                   min_price_change_pct: float = 20.0,
                   min_volume_usd: float = 50_000.0) -> List[Dict]:
        """
        Scan trending tokens for pump candidates.
        Returns tokens with significant price movement and volume.
        """
        trending = self.api.get_trending_tokens(chain)
        pumps = []

        for token in trending:
            address = token.get("address", "")
            if not address:
                continue

            try:
                overview = self.api.get_token_overview(address, chain)
                if "error" in overview:
                    continue

                price_chg = abs(_safe_float(overview.get("price_change_1h_pct")))
                volume = _safe_float(overview.get("v1h"))

                if price_chg >= min_price_change_pct and volume >= min_volume_usd:
                    pumps.append({
                        **overview,
                        "pump_score": price_chg * (volume / 100_000),
                    })

                time.sleep(0.15)  # Basic rate limiting
            except Exception as exc:
                logger.debug("find_pumps: skipping %s — %s", address, exc)

        pumps.sort(key=lambda x: x.get("pump_score", 0), reverse=True)
        return pumps

    # ------------------------------------------------------------------
    def analyze_token(self, address: str, chain: str = "solana") -> Dict:
        """
        Full token analysis: overview + security + recent trades.
        """
        overview = self.api.get_token_overview(address, chain)
        security = self.api.get_token_security(address, chain)
        trades = self.api.get_token_trades(address, chain, limit=50)
        holders = self.api.get_holder_list(address, chain, limit=20)
        creation = self.api.get_token_creation_info(address, chain)

        # Summarize trade activity
        buy_vol = sum(t["value_usd"] for t in trades if t.get("type") == "buy")
        sell_vol = sum(t["value_usd"] for t in trades if t.get("type") == "sell")

        return {
            "overview": overview,
            "security": security,
            "creation": creation,
            "holders": holders,
            "trade_summary": {
                "buy_volume_usd": buy_vol,
                "sell_volume_usd": sell_vol,
                "buy_sell_ratio": buy_vol / sell_vol if sell_vol > 0 else 0.0,
                "recent_trades": len(trades),
            },
        }

    # ------------------------------------------------------------------
    def daily_scan(self, chain: str = "solana",
                   limit_per_source: int = 20) -> Dict:
        """
        Run a daily scan: trending tokens + new listings + security checks.
        """
        logger.info("Daily scan starting for chain: %s", chain)

        trending = self.api.get_trending_tokens(chain)[:limit_per_source]
        new_listings = self.api.get_new_listings(chain, limit=limit_per_source)

        scan_results = []
        for token in trending + new_listings:
            address = token.get("address", "")
            if not address:
                continue
            try:
                security = self.api.get_token_security(address, chain)
                overview = self.api.get_token_overview(address, chain)
                scan_results.append({
                    "address": address,
                    "symbol": token.get("symbol", ""),
                    "chain": chain,
                    "security_flags": {
                        "mintable": security.get("is_mintable", 0),
                        "top10_concentration": security.get("top_10_holder_percent", 0),
                    },
                    "price": overview.get("price", 0),
                    "v24h": overview.get("v24h", 0),
                    "price_change_24h_pct": overview.get("price_change_24h_pct", 0),
                })
                time.sleep(0.2)
            except Exception as exc:
                logger.debug("daily_scan: error on %s — %s", address, exc)

        return {
            "chain": chain,
            "scan_date": datetime.now(timezone.utc).isoformat(),
            "trending_count": len(trending),
            "new_listings_count": len(new_listings),
            "tokens_analyzed": len(scan_results),
            "results": scan_results,
        }


# ---------------------------------------------------------------------------
# Exported utility functions (same signatures as birdeye_tracker.py)
# ---------------------------------------------------------------------------

def _get_api(api_key: Optional[str] = None) -> AzalystAPI:
    key = api_key or os.environ.get("HELIUS_API_KEY", "")
    return AzalystAPI(api_key=key)


def track_whale(wallet: str, chain: str = "solana",
                api_key: Optional[str] = None) -> Dict:
    """Track whale wallet activity using Azalyst Alpha Scanner."""
    tracker = AzalystTracker(api_key=api_key or os.environ.get("HELIUS_API_KEY", ""))
    return tracker.track_whale(wallet, chain)


def find_pumps(chain: str = "solana",
               min_price_change_pct: float = 20.0,
               min_volume_usd: float = 50_000.0,
               api_key: Optional[str] = None) -> List[Dict]:
    """Find pump candidates on a given chain using Azalyst Alpha Scanner."""
    tracker = AzalystTracker(api_key=api_key or os.environ.get("HELIUS_API_KEY", ""))
    return tracker.find_pumps(chain, min_price_change_pct, min_volume_usd)


def analyze_token(address: str, chain: str = "solana",
                  api_key: Optional[str] = None) -> Dict:
    """Full token analysis using Azalyst Alpha Scanner."""
    tracker = AzalystTracker(api_key=api_key or os.environ.get("HELIUS_API_KEY", ""))
    return tracker.analyze_token(address, chain)


def daily_scan(chain: str = "solana", limit_per_source: int = 20,
               api_key: Optional[str] = None) -> Dict:
    """Run daily scan using Azalyst Alpha Scanner."""
    tracker = AzalystTracker(api_key=api_key or os.environ.get("HELIUS_API_KEY", ""))
    return tracker.daily_scan(chain, limit_per_source)


def get_profitable_traders(chain: str = "solana", time_frame: str = "7D",
                            limit: int = 20,
                            api_key: Optional[str] = None) -> List[Dict]:
    """Not available via free APIs. Returns empty list."""
    return _get_api(api_key).get_profitable_traders(chain, time_frame, limit)


def get_wallet_pnl(wallet: str, chain: str = "solana",
                   api_key: Optional[str] = None) -> Dict:
    """Get wallet PnL summary using Azalyst Alpha Scanner (Helius/Solana)."""
    return _get_api(api_key).get_wallet_pnl(wallet, chain)


def get_top_traders(address: str, chain: str = "solana",
                    time_frame: str = "24h", limit: int = 10,
                    api_key: Optional[str] = None) -> List[Dict]:
    """Not available via free APIs. Returns empty list."""
    return _get_api(api_key).get_top_traders(address, chain, time_frame, limit)


def check_token_security(address: str, chain: str = "solana",
                          api_key: Optional[str] = None) -> Dict:
    """Check token security via GoPlus using Azalyst Alpha Scanner."""
    return _get_api(api_key).get_token_security(address, chain)


def get_new_listings(chain: str = "solana", limit: int = 50,
                     api_key: Optional[str] = None) -> List[Dict]:
    """Fetch new token listings via GeckoTerminal using Azalyst Alpha Scanner."""
    return _get_api(api_key).get_new_listings(chain, limit)


def get_token_creation_info(address: str, chain: str = "solana",
                             api_key: Optional[str] = None) -> Dict:
    """Get token creation info via DexScreener using Azalyst Alpha Scanner."""
    return _get_api(api_key).get_token_creation_info(address, chain)


def get_holder_list(address: str, chain: str = "solana", limit: int = 100,
                    api_key: Optional[str] = None) -> List[Dict]:
    """Get top holder list via Helius/GoPlus using Azalyst Alpha Scanner."""
    return _get_api(api_key).get_holder_list(address, chain, limit)


def get_wallet_pnl_details(wallet: str, chain: str = "solana",
                            limit: int = 100,
                            api_key: Optional[str] = None) -> List[Dict]:
    """Get per-token PnL breakdown via Helius using Azalyst Alpha Scanner."""
    return _get_api(api_key).get_wallet_pnl_details(wallet, chain, limit)


def get_trader_txs(wallet: str, chain: str = "solana",
                   start_time: Optional[int] = None,
                   end_time: Optional[int] = None,
                   limit: int = 50,
                   api_key: Optional[str] = None) -> List[Dict]:
    """Get trader transactions via Helius using Azalyst Alpha Scanner."""
    return _get_api(api_key).get_trader_txs(wallet, chain, start_time, end_time, limit)


def get_ohlcv(address: str, chain: str = "solana", timeframe: str = "1h",
              from_time: Optional[int] = None, to_time: Optional[int] = None,
              api_key: Optional[str] = None) -> List[Dict]:
    """Get OHLCV candle data via GeckoTerminal using Azalyst Alpha Scanner."""
    return _get_api(api_key).get_ohlcv(address, chain, timeframe, from_time, to_time)


def get_wallet_token_list(wallet: str, chain: str = "solana",
                           api_key: Optional[str] = None) -> List[Dict]:
    """Get tokens in wallet via Helius using Azalyst Alpha Scanner."""
    return _get_api(api_key).get_wallet_token_list(wallet, chain)


def get_wallet_tx_list(wallet: str, chain: str = "solana",
                        page: int = 1, page_size: int = 20,
                        api_key: Optional[str] = None) -> List[Dict]:
    """Get paginated wallet transaction list via Helius using Azalyst Alpha Scanner."""
    return _get_api(api_key).get_wallet_tx_list(wallet, chain, page, page_size)
