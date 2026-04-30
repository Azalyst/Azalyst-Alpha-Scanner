"""paper_trader.py - Azalyst Alpha Paper Trader
Tracks token signal entries/exits and P&L for dashboard display."""
import json, logging, os, math
from datetime import datetime, timezone
from typing import Dict, List, Optional

log = logging.getLogger("azalyst_alpha.trader")

class Portfolio:
    def __init__(self, file: str = "portfolio.json"):
        self.file = file
        self.open: List[Dict] = []
        self.closed: List[Dict] = []
        self.cash = 5000.0
        self.deposited = 5000.0
        self.counter = 0
        if os.path.exists(file):
            try:
                d = json.loads(open(file).read())
                self.open = d.get("open", [])
                self.closed = d.get("closed", [])
                self.cash = d.get("cash", 5000.0)
                self.counter = d.get("counter", 0)
            except: pass

    def save(self):
        json.dump({"open": self.open, "closed": self.closed, "cash": round(self.cash,2), "counter": self.counter}, open(self.file,"w"), indent=2)

    def enter(self, symbol: str, price: float, units: float, conf: int) -> Optional[Dict]:
        cost = round(price * units, 2)
        if cost > self.cash or cost < 10: return None
        self.counter += 1; self.cash -= cost
        p = {"id": f"A{self.counter:04d}", "symbol": symbol, "entry": price, "current": price, "units": units, "invested": cost, "date": datetime.now(timezone.utc).isoformat(), "conf": conf}
        self.open.append(p); self.save(); return p

    def update(self, prices: Dict[str, float]):
        for p in self.open:
            if p["symbol"] in prices: p["current"] = prices[p["symbol"]]

    def get_summary(self) -> Dict:
        inv = sum(p["invested"] for p in self.open)
        cur = sum(p["current"]*p["units"] for p in self.open)
        unrl = round(cur - inv, 2)
        cp = sum(t.get("pnl",0) for t in self.closed)
        val = round(self.cash + cur, 2)
        ret = round((val-self.deposited)/self.deposited*100, 2) if self.deposited else 0
        wins = len([t for t in self.closed if t.get("pnl",0) > 0])
        return {"cash": round(self.cash,2), "invested": inv, "value": cur, "unrealised": unrl, "closed_pnl": round(cp,2), "portfolio_value": val, "total_return_pct": ret, "open_count": len(self.open), "closed_count": len(self.closed), "win_rate": round(wins/len(self.closed)*100,1) if self.closed else 0}