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


def sget_bool(key: str, default: bool) -> bool:
    _reload_if_needed()
    v = _settings_cache.get(key, os.environ.get(key))
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


class TraderConfig:
    def __init__(self) -> None:
        # Static at startup (connection/paths — intentionally not hot-reloaded):
        self.timeout_sec = sget_float("TRADER_TIMEOUT_SEC", 2.0)
        self.trades_file = os.environ.get("TRADER_TRADES_FILE", "/data/trades.jsonl")
        self.predict_proxy_url = os.environ.get("PREDICT_PROXY_URL", "").strip() or None

    # ── Tunable at runtime via /data/settings.json ────────────────────────────
    @property
    def test_mode(self) -> bool:
        return sget_bool("TRADER_TEST_MODE", False)

    @property
    def dry_run(self) -> bool:
        return sget_bool("TRADER_DRY_RUN", False)

    @property
    def max_trade_usd(self) -> float:
        return sget_float("TRADER_MAX_TRADE_USD", 5.0)

    @property
    def poly_min_order_usd(self) -> float:
        return sget_float("POLY_MIN_ORDER_USD", 1.0)

    @property
    def predict_min_order_usd(self) -> float:
        return sget_float("PREDICT_MIN_ORDER_USD", 0.9)

    @property
    def predict_slippage_bps(self) -> int:
        return sget_int("PREDICT_SLIPPAGE_BPS", 200)

    @property
    def predict_fill_timeout_sec(self) -> float:
        return sget_float("PREDICT_FILL_TIMEOUT_SEC", 3.0)

    @property
    def predict_fill_poll_interval_sec(self) -> float:
        return sget_float("PREDICT_FILL_POLL_INTERVAL_SEC", 0.1)

    @property
    def predict_limit_fill_timeout_sec(self) -> float:
        return sget_float("PREDICT_LIMIT_FILL_TIMEOUT_SEC", 30.0)

    @property
    def predict_limit_poll_interval_sec(self) -> float:
        return sget_float("PREDICT_LIMIT_POLL_INTERVAL_SEC", 0.5)

    @property
    def predict_market_cooldown_sec(self) -> float:
        return sget_float("PREDICT_MARKET_COOLDOWN_SEC", 30.0)


CONFIG = TraderConfig()
