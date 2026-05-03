# Azalyst Whale Tracking - Complete Guide

## Overview

This agent now includes comprehensive whale tracking capabilities for Azalyst.so, implementing the complete workflow for tracking whale wallets, detecting pump/dump signals, and finding hidden gem tokens before they moon.

---

## 🎯 Complete Workflow (Aaj Pump Pakdna Hai Toh)

### Step 1: Whale Ka Wallet Pakad (Find Whale Wallets)

Use the "Find Trades" feature to track large value transactions:

```
/agent find_pumps
```

This scans for:
- Tokens with unusual volume spikes
- Large buy/sell transactions (>$10K)
- Smart money movements
- Early accumulation patterns

**Workflow:**
1. Agent checks trending tokens (24H)
2. Identifies unusual volume/price spikes
3. Extracts whale wallet addresses from large trades
4. Returns top whale movements sorted by value

### Step 2: Wallet Deep Dive

Once you have a whale wallet address:

```
/agent track_whale wallet_address=<WALLET_ADDRESS>
```

**This returns:**
- Total portfolio value
- Top 10 held tokens
- Recent transaction history
- Accumulation/distribution patterns

**Example:**
```
/agent track_whale wallet_address=7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU
```

### Step 3: Pump Aane Se Pehle Signals (Pre-Pump Detection)

Analyze specific tokens for pump signals:

```
/agent analyze_token token_address=<TOKEN_ADDRESS>
```

**Checks for:**
- ✅ **GREEN FLAGS (Pump Signals)**
  - Smart wallets accumulating
  - Volume 10x in 1H
  - New holders spike (>500 in 24h)
  - Whale bought newly launched token
  - Low holder concentration (<30% top 10)

- ⚠️ **RED FLAGS (Dump Signals)**
  - Large wallet outflows
  - LP pulled suddenly (>20% drop)
  - Holder count dropping
  - Dev wallet selling
  - Community exit pattern

**Example:**
```
/agent analyze_token token_address=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
```

### Step 4: Hidden Gem Filter (Early Pump Detection)

Find tokens with pump potential BEFORE they trend:

```
/agent find_pumps
```

**Filters Applied:**
- Token age <24 hours
- Liquidity pool >$2K
- Volume growth >$10K in 1H
- Holder growth pattern
- Security checks (not mintable, fair distribution)

**Returns:**
- Top 10 gems ranked by score (0-100)
- Age, volume, liquidity metrics
- Holder count and growth rate
- Risk assessment

### Step 5: Daily Comprehensive Scan

Run complete analysis across all metrics:

```
/agent daily_scan
```

**This generates a full report with:**
1. Trending token analysis (top 20)
2. Whale trade movements (top 10)
3. Hidden gems (top 10)
4. Watchlist updates (all tracked wallets)

---

## 🛠️ Available Tools

### 1. `track_whale`

**Description:** Add whale wallet to tracking and analyze holdings

**Parameters:**
- `wallet_address` (required): Solana wallet address
- `api_key` (optional): Helius API key (uses HELIUS_API_KEY env var by default)

**Returns:**
- Watchlist confirmation
- Total portfolio value
- Top held tokens
- Recent activity summary

**Example:**
```tool_call
{"tool": "track_whale", "args": {"wallet_address": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"}}
```

---

### 2. `find_pumps`

**Description:** Scan for potential pump tokens using hidden gem filters

**Parameters:**
- `api_key` (optional): Helius API key

**Returns:**
- Top 10 potential pump tokens
- Score (0-100) based on multiple factors
- Volume, liquidity, age metrics
- Holder statistics

**Scoring Factors:**
- Volume spike (0-30 points)
- Holder growth (0-30 points)
- Liquidity strength (0-20 points)
- Security bonus (0-20 points)

**Example:**
```tool_call
{"tool": "find_pumps", "args": {}}
```

---

### 3. `analyze_token`

**Description:** Deep analysis of token for pump/dump signals

**Parameters:**
- `token_address` (required): Solana token address
- `api_key` (optional): Helius API key

**Returns:**
- Signal type: "pump" or "dump"
- Confidence score (0-100%)
- List of indicators (green/red flags)
- Timestamp of analysis

**Red Flags (Dump Signals):**
- Large wallet outflows
- LP pulled significantly (>20%)
- Holder count dropping
- Dev wallet selling
- Community exit pattern

**Green Flags (Pump Signals):**
- Smart wallets accumulating
- Volume 10x+ in 1 hour
- New holders spike (>500)
- Whale early entry
- Fair token distribution

**Example:**
```tool_call
{"tool": "analyze_token", "args": {"token_address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"}}
```

---

### 4. `daily_scan`

**Description:** Run complete daily whale tracking workflow

**Parameters:**
- `api_key` (optional): Helius API key

**Returns:**
Comprehensive report with:
- 📊 Trending token analysis (top 20 with signals)
- 🐋 Top whale trades (sorted by value)
- 💎 Hidden gems (filtered and scored)
- 📝 Watchlist updates (all tracked wallets)

**Example:**
```tool_call
{"tool": "daily_scan", "args": {}}
```

---

## 🔧 Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Helius API Key (Optional)

Get a free API key from [Azalyst.so](https://Azalyst.so)

```bash
export HELIUS_API_KEY=your_api_key_here
```

**Note:** The agent works without an API key but may have rate limits. With an API key, you get higher limits and more features.

### 3. Add to GitHub Actions

Add `HELIUS_API_KEY` to your repository secrets:

`Settings → Secrets and variables → Actions → New repository secret`

| Name | Value |
|---|---|
| `HELIUS_API_KEY` | your Helius API key |

---

## 📊 Signal Interpretation Guide

### Pump Signal (🚀)

**Confidence >70%:** Strong buy signal
- Multiple green flags
- Smart money accumulating
- Viral holder growth
- Volume momentum

**Action:** Consider entry, monitor closely

**Confidence 40-70%:** Moderate signal
- Mixed indicators
- Some positive trends
- Requires additional confirmation

**Action:** Watch and wait for stronger signal

### Dump Signal (⚠️)

**Confidence >70%:** Strong sell/avoid signal
- Large outflows detected
- LP instability
- Community exodus
- Dev selling

**Action:** Exit or avoid entry

**Confidence 40-70%:** Caution signal
- Some red flags present
- Potential reversal risk

**Action:** Reduce position, monitor closely

---

## 🎯 Practical Example Workflows

### Workflow 1: Morning Pump Hunter

```bash
# Run this every morning
/agent daily_scan

# Review report, pick top 3 gems
# Analyze each one in detail
/agent analyze_token token_address=<GEM_1>
/agent analyze_token token_address=<GEM_2>
/agent analyze_token token_address=<GEM_3>

# Track promising whale wallets from the scan
/agent track_whale wallet_address=<WHALE_FROM_SCAN>
```

### Workflow 2: Real-Time Whale Following

```bash
# Find current whale trades
/agent find_pumps

# Pick a token with unusual activity
/agent analyze_token token_address=<ACTIVE_TOKEN>

# If signal is strong, track the whale
/agent track_whale wallet_address=<WHALE_BUYER>

# Monitor whale's other positions
/agent daily_scan
```

### Workflow 3: Token Deep Dive

```bash
# Got a tip on a token? Analyze it
/agent analyze_token token_address=<TOKEN>

# Check who's buying
/agent find_pumps

# If whales are in, track them
/agent track_whale wallet_address=<WHALE>
```

---

## ⚠️ Risk Disclaimer

**IMPORTANT:** Whale tracking is informational only, NOT financial advice.

- Pump aane se pehle pata lagana mushkil hai
- Past performance ≠ future results
- Whales can dump without warning
- Always manage risk
- Never full port karo
- DYOR (Do Your Own Research)

**Use this tool for:**
- Education and learning
- Pattern recognition
- Market awareness
- Risk assessment

**DO NOT use this tool for:**
- Guaranteed pump predictions
- Financial advice
- Unverified trading signals
- High-risk leverage

---

## 🔍 Advanced Features

### Custom Filters

Modify `Azalyst_tracker.py` to customize:

```python
# In find_hidden_gems()
tracker.find_hidden_gems(
    min_lp_size=5000,      # Increase minimum liquidity
    min_volume_1h=20000,   # Higher volume requirement
    max_age_hours=12       # Newer tokens only
)
```

### Alert Configuration

Generate Telegram alert config:

```python
from Azalyst_tracker import WhaleTracker

tracker = WhaleTracker(api_key)
config = tracker.generate_alert_config(
    volume_threshold=20000,  # Alert on >$20K volume spike
    whale_threshold=10000    # Alert on >$10K whale moves
)
print(json.dumps(config, indent=2))
```

### Watchlist Management

```python
# Add multiple whales
tracker.add_to_watchlist("wallet1...")
tracker.add_to_watchlist("wallet2...")
tracker.add_to_watchlist("wallet3...")

# Run daily updates
results = tracker.run_daily_scan()
```

---

## 📈 Success Metrics

Track your performance:

1. **Hit Rate:** % of pump signals that actually pumped
2. **Average Gain:** Average % gain from signal to peak
3. **False Positives:** Pump signals that failed
4. **Early Entry:** How early you caught the move

Keep a log of all signals and outcomes to improve filtering over time.

---

## 🤝 Contributing

To improve the whale tracking:

1. Add more data sources
2. Improve scoring algorithm
3. Add machine learning models
4. Enhance alert system
5. Build backtesting framework

---

## 📚 Additional Resources

- [Helius API Documentation](https://docs.Azalyst.so)
- [Solana Explorer](https://solscan.io)
- [Token Security Checker](https://rugcheck.xyz)

---

## Support

For issues or questions:
1. Check the agent logs
2. Verify API key is set
3. Test with simple tokens first
4. Review error messages carefully

**Remember:** This is a tool for education and awareness. Always verify information independently and never invest more than you can afford to lose.
