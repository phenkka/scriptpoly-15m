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


CONFIG = AnalyzerConfig()
