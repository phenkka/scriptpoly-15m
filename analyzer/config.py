import json
import os
from pathlib import Path

# ── Live settings: /data/settings.json overrides env on every read ────────────
_SETTINGS_PATH = Path("/data/settings.json")
_settings_cache: dict = {}
_settings_mtime: float = -1.0


def _reload_if_needed() -> None:
    global _settings_cache, _settings_mtime
    try:
        mtime = _SETTINGS_PATH.stat().st_mtime
        if mtime != _settings_mtime:
            _settings_cache = json.loads(_SETTINGS_PATH.read_text())
            _settings_mtime = mtime
    except Exception:
        pass


def sget_float(key: str, default: float) -> float:
    _reload_if_needed()
    v = _settings_cache.get(key, os.environ.get(key))
    if v is None:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def sget_int(key: str, default: int) -> int:
    _reload_if_needed()
    v = _settings_cache.get(key, os.environ.get(key))
    if v is None:
        return default
    try:
        return int(v)
    except (ValueError, TypeError):
        return default


class AnalyzerConfig:
    def __init__(self) -> None:
        # Static at startup:
        self.trader_url = os.environ.get("TRADER_URL", "").strip()
        self.trader_timeout_sec = sget_float("TRADER_TIMEOUT_SEC", 0.5)

    # ── Tunable at runtime via /data/settings.json ────────────────────────────
    @property
    def trader_max_trade_usd(self) -> float:
        return sget_float("TRADER_MAX_TRADE_USD", 5.0)

    @property
    def poly_bankroll_usd(self) -> float:
        return sget_float("POLY_BANKROLL_USD", 10.0)

    @property
    def pred_bankroll_usd(self) -> float:
        return sget_float("PRED_BANKROLL_USD", 10.0)

    @property
    def min_stake_usd(self) -> float:
        return sget_float("MIN_STAKE_USD", 1.0)

    @property
    def min_profit_usd(self) -> float:
        return sget_float("MIN_PROFIT_USD", 0.0)

    @property
    def orderbook_levels(self) -> int:
        return sget_int("ORDERBOOK_LEVELS", 25)

    @property
    def vwap_max_levels(self) -> int:
        return sget_int("VWAP_MAX_LEVELS", 25)

    @property
    def poly_vwap_buffer_bps(self) -> float:
        return sget_float("POLY_VWAP_BUFFER_BPS", 30.0)

    @property
    def pred_vwap_buffer_bps(self) -> float:
        return sget_float("PRED_VWAP_BUFFER_BPS", 200.0)

    @property
    def predict_fee_bps(self) -> float:
        return sget_float("PREDICT_FEE_BPS", 0.0)

    @property
    def poly_fee_rate(self) -> float:
        return sget_float("POLY_FEE_RATE", 0.072)

    @property
    def ba_safety_buffer_bps(self) -> float:
        return sget_float("BA_SAFETY_BUFFER_BPS", 300.0)

    @property
    def ba_min_net_edge_bps(self) -> float:
        return sget_float("BA_MIN_NET_EDGE_BPS", 0.0)

    @property
    def predict_max_bid_price(self) -> float:
        return sget_float("PREDICT_MAX_BID_PRICE", 0.99)

    @property
    def poly_max_hedge_price(self) -> float:
        return sget_float("POLY_MAX_HEDGE_PRICE", 0.99)

    @property
    def pred_reserve_usd(self) -> float:
        return sget_float("PRED_RESERVE_USD", 0.50)

    @property
    def poly_reserve_usd(self) -> float:
        return sget_float("POLY_RESERVE_USD", 0.75)

    @property
    def poly_bank_util_max(self) -> float:
        return sget_float("POLY_BANK_UTIL_MAX", 0.85)

    @property
    def poly_min_order_usd(self) -> float:
        return sget_float("POLY_MIN_ORDER_USD", 1.0)

    @property
    def predict_min_order_usd(self) -> float:
        return sget_float("PREDICT_MIN_ORDER_USD", 0.9)


CONFIG = AnalyzerConfig()
