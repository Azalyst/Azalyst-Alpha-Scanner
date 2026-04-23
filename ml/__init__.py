"""
Birdeye behavioral ML pipeline.

Mines frequent wallet-action sequences (e.g. whale_buy → anonymous_buy × N)
and trains a supervised classifier that predicts whether a signal will
reach its target move within the evaluation horizon.

Runs on the same SQLite DB (`data/birdeye_quant.db`) that
`quant_signal_engine.py` already populates. No new data collection
required — the model uses `top_traders`, `trade_aggs`, `token_snapshots`,
`signals`, and `signal_outcomes`.

CLI entrypoint: `python -m ml {schema|cluster|events|mine|train|score|all}`
"""
