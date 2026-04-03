import os


def _env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    if v is None or not v.strip():
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or not v.strip():
        return default
    try:
        return int(v)
    except ValueError:
        return default


class AnalyzerConfig:
    def __init__(self) -> None:
        self.trader_url = os.environ.get("TRADER_URL", "").strip()
        self.trader_timeout_sec = _env_float("TRADER_TIMEOUT_SEC", 0.5)

        self.trader_max_trade_usd = _env_float("TRADER_MAX_TRADE_USD", 5.0)
        self.poly_bankroll_usd = _env_float("POLY_BANKROLL_USD", 100.0)
        self.pred_bankroll_usd = _env_float("PRED_BANKROLL_USD", 100.0)

        self.min_stake_usd = _env_float("MIN_STAKE_USD", 1.0)
        self.min_profit_usd = _env_float("MIN_PROFIT_USD", 0.0)

        self.orderbook_levels = _env_int("ORDERBOOK_LEVELS", 25)
        self.vwap_max_levels = _env_int("VWAP_MAX_LEVELS", 25)
        self.poly_vwap_buffer_bps = _env_float("POLY_VWAP_BUFFER_BPS", 0.0)
        self.pred_vwap_buffer_bps = _env_float("PRED_VWAP_BUFFER_BPS", 0.0)

        # BID+ASK fee / edge params
        self.predict_fee_bps = _env_float("PREDICT_FEE_BPS", 0.0)
        # Polymarket taker fee rate: actual fee = feeRate * p * (1-p).
        # Default 0.072 = Crypto category rate per Polymarket docs.
        # Prefer dynamic rate from CLOB API (passed via tick meta); this is fallback.
        self.poly_fee_rate = _env_float("POLY_FEE_RATE", 0.072)
        self.ba_safety_buffer_bps = _env_float("BA_SAFETY_BUFFER_BPS", 0.0)
        self.ba_min_net_edge_bps = _env_float("BA_MIN_NET_EDGE_BPS", 0.0)

        # BID+ASK hard price caps
        self.predict_max_bid_price = _env_float("PREDICT_MAX_BID_PRICE", 0.49)
        self.poly_max_hedge_price = _env_float("POLY_MAX_HEDGE_PRICE", 0.58)

        # Reserves: keep this much USD aside per bankroll
        self.pred_reserve_usd = _env_float("PRED_RESERVE_USD", 0.50)
        self.poly_reserve_usd = _env_float("POLY_RESERVE_USD", 0.75)

        # Bank utilization: skip if poly cost > this fraction of poly bankroll
        self.poly_bank_util_max = _env_float("POLY_BANK_UTIL_MAX", 0.85)

        # Minimum order sizes (same as trader .env values)
        self.poly_min_order_usd = _env_float("POLY_MIN_ORDER_USD", 1.0)
        self.predict_min_order_usd = _env_float("PREDICT_MIN_ORDER_USD", 0.9)


CONFIG = AnalyzerConfig()
