import os


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


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


class TraderConfig:
    def __init__(self) -> None:
        self.test_mode = _env_bool("TRADER_TEST_MODE", False)
        self.dry_run = _env_bool("TRADER_DRY_RUN", True)
        self.max_trade_usd = _env_float("TRADER_MAX_TRADE_USD", 1.0)
        self.timeout_sec = _env_float("TRADER_TIMEOUT_SEC", 2.0)
        self.trades_file = os.environ.get("TRADER_TRADES_FILE", "/data/trades.jsonl")

        self.poly_min_order_usd = _env_float("POLY_MIN_ORDER_USD", 1.0)
        self.predict_min_order_usd = _env_float("PREDICT_MIN_ORDER_USD", 1.0)

        self.predict_slippage_bps = _env_int("PREDICT_SLIPPAGE_BPS", 0)
        self.predict_fill_timeout_sec = _env_float("PREDICT_FILL_TIMEOUT_SEC", 6.0)
        self.predict_fill_poll_interval_sec = _env_float("PREDICT_FILL_POLL_INTERVAL_SEC", 0.2)
        self.predict_market_cooldown_sec = _env_float("PREDICT_MARKET_COOLDOWN_SEC", 900.0)

        self.predict_proxy_url = os.environ.get("PREDICT_PROXY_URL", "").strip() or None


CONFIG = TraderConfig()
