from __future__ import annotations

from datetime import datetime, timezone
import itertools
import json
import os
import time
import traceback
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import requests

import sys as _sys
_sys.path.insert(0, "/app")
try:
    from notify import notify
except ImportError:
    def notify(text: str, **_: object) -> None:  # type: ignore[misc]
        pass

from predict_sdk import (
    BuildOrderInput,
    Book,
    ChainId,
    LimitHelperInput,
    MarketHelperValueInput,
    OrderBuilder,
    OrderBuilderOptions,
    Side,
)
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from py_clob_client.clob_types import MarketOrderArgs, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

from trader.config import CONFIG as CFG

# Патч глобального httpx клиента py_clob_client для поддержки прокси
_proxy_url = os.environ.get("PROXY_URL", "").strip()
if _proxy_url:
    import py_clob_client.http_helpers.helpers as _clob_helpers
    _clob_helpers._http_client = httpx.Client(
        http2=True,
        timeout=httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0),
        mounts={
            "http://": httpx.HTTPTransport(proxy=_proxy_url, retries=1),
            "https://": httpx.HTTPTransport(proxy=_proxy_url, retries=1),
        },
    )
else:
    import py_clob_client.http_helpers.helpers as _clob_helpers
    _clob_helpers._http_client = httpx.Client(
        http2=True,
        timeout=httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0),
    )


# ---------------------------------------------------------------------------
# Predict client: persistent session, cached JWT and builder
# ---------------------------------------------------------------------------
import threading as _threading


class _PredictClient:
    """Singleton с персистентной сессией, кешированным JWT и OrderBuilder.

    Экономит 2-3 последовательных HTTP round-trip перед каждым ордером:
      - GET /auth/message + POST /auth  (~800ms) — повторяется только по истечении JWT
      - OrderBuilder.make()              (~мс)    — создаётся один раз
      - GET /markets/{id}               (~400ms)  — кешируется по market_id
    """

    _JWT_TTL_SEC = 3 * 3600  # JWT живёт несколько часов; обновляем раз в 3ч

    def __init__(self) -> None:
        self._lock = _threading.RLock()  # RLock — реентрантный, safe для get() внутри get_market()
        self._session: requests.Session | None = None
        self._builder: OrderBuilder | None = None
        self._jwt: str | None = None
        self._jwt_ts: float = 0.0
        self._market_cache: dict[int, dict[str, Any]] = {}

    def _make_session(self) -> requests.Session:
        s = requests.Session()
        api_key = os.environ.get("PREDICT_API_KEY", "").strip()
        s.headers.update({"Accept": "application/json", "x-api-key": api_key})
        proxy = os.environ.get("PREDICT_PROXY_URL", "").strip()
        if proxy:
            s.proxies.update({"http": proxy, "https": proxy})
        return s

    def _make_builder(self) -> OrderBuilder:
        private_key = _normalize_hex_key(os.environ.get("PREDICT_PRIVATE_KEY", ""))
        predict_account = os.environ.get("PREDICT_ACCOUNT", "").strip() or None
        chain_id = _get_predict_chain_id()
        return OrderBuilder.make(
            chain_id,
            private_key,
            OrderBuilderOptions(predict_account=predict_account) if predict_account else None,
        )

    def _refresh_jwt(self, session: requests.Session, builder: OrderBuilder) -> str:
        private_key = _normalize_hex_key(os.environ.get("PREDICT_PRIVATE_KEY", ""))
        predict_account = os.environ.get("PREDICT_ACCOUNT", "").strip() or None
        token = _predict_get_jwt(session, private_key, predict_account=predict_account, builder=builder)
        session.headers.update({"Authorization": f"Bearer {token}"})
        return token

    def get(self) -> tuple[requests.Session, OrderBuilder]:
        """Возвращает (session, builder) с актуальным JWT. Thread-safe."""
        with self._lock:
            if self._session is None:
                self._session = self._make_session()
            if self._builder is None:
                self._builder = self._make_builder()
            now = time.time()
            if self._jwt is None or (now - self._jwt_ts) > self._JWT_TTL_SEC:
                self._jwt = self._refresh_jwt(self._session, self._builder)
                self._jwt_ts = now
                print(f"[TRADER] predict_jwt_refreshed ts={self._jwt_ts:.0f}")
            return self._session, self._builder

    def get_market(self, market_id: int) -> dict[str, Any]:
        """Возвращает данные рынка, кешируя по market_id."""
        with self._lock:
            if market_id not in self._market_cache:
                session, _ = self.get()
                self._market_cache[market_id] = _predict_market(session, market_id)
            return self._market_cache[market_id]

    def invalidate_jwt(self) -> None:
        """Принудительное обновление JWT при следующем get()."""
        with self._lock:
            self._jwt = None
            self._jwt_ts = 0.0

    def invalidate_market(self, market_id: int) -> None:
        with self._lock:
            self._market_cache.pop(market_id, None)


class _PredictMonitorSession:
    """Read-only сессия на PREDICT_API_KEY_2 для мониторинговых запросов.

    Используется в ghost_fill_watch и _late_fill_watcher чтобы не расходовать
    лимиты основного ключа (KEY_1: 500 req/min) на поллинг статуса ордеров.
    KEY_2 имеет 1000 req/min. JWT получается через тот же приватный ключ.
    Если KEY_2 не задан, автоматически деградирует на основную сессию KEY_1.
    """

    _JWT_TTL_SEC = 3 * 3600

    def __init__(self) -> None:
        self._lock = _threading.Lock()
        self._session: requests.Session | None = None
        self._jwt: str | None = None
        self._jwt_ts: float = 0.0

    def get(self) -> requests.Session:
        with self._lock:
            key2 = os.environ.get("PREDICT_API_KEY_2", "").strip()
            if not key2:
                # No KEY_2 — reuse main client session (already has JWT)
                session, _ = _predict_client.get()
                return session

            if self._session is None:
                s = requests.Session()
                s.headers.update({"Accept": "application/json", "x-api-key": key2})
                proxy = os.environ.get("PREDICT_PROXY_URL", "").strip()
                if proxy:
                    s.proxies.update({"http": proxy, "https": proxy})
                self._session = s

            now = time.time()
            if self._jwt is None or (now - self._jwt_ts) > self._JWT_TTL_SEC:
                private_key = _normalize_hex_key(os.environ.get("PREDICT_PRIVATE_KEY", ""))
                predict_account = os.environ.get("PREDICT_ACCOUNT", "").strip() or None
                _, builder = _predict_client.get()
                self._jwt = _predict_get_jwt(
                    self._session, private_key,
                    predict_account=predict_account, builder=builder
                )
                self._session.headers.update({"Authorization": f"Bearer {self._jwt}"})
                self._jwt_ts = now
                print("[TRADER] predict_monitor_session: JWT refreshed using KEY_2")

            return self._session


_predict_client = _PredictClient()
_predict_monitor = _PredictMonitorSession()


class OpportunityLeg(BaseModel):
    source: str
    side: str
    ts: str
    ask: float
    ask_sz: float
    pool_usd: float
    shares: float
    stake_usd: float
    token_id: str | None = None
    market_id: int | None = None
    title: str | None = None


class Opportunity(BaseModel):
    type: str
    label: str
    sum: float
    edge: float
    roi: float
    shares: float
    stake_usd: float
    payout_usd: float
    profit_usd: float
    legs: list[OpportunityLeg]
    sent_at: str
    analyzer_calc_at: str | None = None
    analyzer_tick_ts_max: str | None = None
    poly_dynamic_fee: float | None = None
    poly_fee_rate: float | None = None
    predict_fee_bps: float | None = None
    safety_buffer_bps: float | None = None
    predict_max_bid_price: float | None = None
    end_date: str | None = None


app = FastAPI()

_TRACE_COUNTER = itertools.count(1)


@app.on_event("startup")
def _startup_warmup() -> None:
    """Прогреваем predict клиент при старте: получаем JWT и создаём builder заранее.
    Это убирает задержку ~1s на первом реальном трейде.
    """
    import threading
    def _warmup() -> None:
        try:
            _predict_client.get()
            print("[TRADER] predict_client warmed up (JWT + builder ready)")
        except Exception as e:
            print(f"[TRADER] predict_client warmup failed (non-fatal): {e}")
    threading.Thread(target=_warmup, daemon=True).start()

    # Restore fill-state for Telegram reply chaining across container restarts
    _load_ba_fill_state()
    print(f"[TRADER] ba_fill_state loaded entries={len(_ba_fill_state)}")

    # Check for Predict orders that were in-flight when the container last stopped.
    # Runs in a background thread so it doesn't block server startup.
    threading.Thread(target=_check_inflight_on_startup, daemon=True, name="inflight_startup_check").start()

    # Background late-fill watcher: detects ghost fills that arrive after the 60s
    # ghost_fill_watch window has expired (real BSC confirmation lag edge case).
    threading.Thread(target=_late_fill_watcher, daemon=True, name="late_fill_watcher").start()

    # VPN watchdog: проверяем доступность прокси каждые 60 секунд.
    # Если прокси недоступен — создаём /data/halt_vpn и уведомляем.
    # Как только восстановился — удаляем файл и уведомляем.
    _vpn_check_url = os.environ.get("VPN_CHECK_URL", "https://clob.polymarket.com/health").strip()
    _vpn_check_interval = float(os.environ.get("VPN_CHECK_INTERVAL_SEC", "60") or "60")
    _halt_vpn_path = Path(os.environ.get("TRADER_TRADES_FILE", "/data/trades.jsonl")).parent / "halt_vpn"

    def _vpn_watchdog() -> None:
        # Инициализируем состояние из файла — если файл уже есть после rebuild, сразу считаем что был down
        _was_down = _halt_vpn_path.exists()
        while True:
            if not _proxy_url:
                time.sleep(_vpn_check_interval)
                continue  # прокси не настроен — проверять нечего
            _ok = False
            try:
                _r = httpx.get(
                    _vpn_check_url,
                    proxy=_proxy_url,
                    timeout=httpx.Timeout(10.0, connect=5.0, read=8.0),
                )
                _ok = _r.status_code < 500
            except Exception as _ve:
                print(f"[TRADER][VPN_WATCHDOG] check_failed err={_ve}")
            if not _ok and not _was_down:
                _was_down = True
                _halt_vpn_path.write_text("vpn_down")
                print("[TRADER][VPN_WATCHDOG] VPN DOWN — halt_vpn created")
                notify(
                    "🔴 <b>VPN IS INACTIVE</b>\n"
                )
            elif _ok and _was_down:
                _was_down = False
                _halt_vpn_path.unlink(missing_ok=True)
                print("[TRADER][VPN_WATCHDOG] VPN RESTORED — halt_vpn removed")
                notify(
                    "🟢 <b>VPN IS ACTIVE</b>\n"
                )
            time.sleep(_vpn_check_interval)

    threading.Thread(target=_vpn_watchdog, daemon=True, name="vpn_watchdog").start()

    # API health watchdogs: poll Predict and Polymarket health endpoints.
    # If either is down — create halt_api file and stop trading until recovery.
    _halt_api_path = Path(os.environ.get("TRADER_TRADES_FILE", "/data/trades.jsonl")).parent / "halt_api"
    _api_check_interval = float(os.environ.get("API_HEALTH_CHECK_INTERVAL_SEC", "30") or "30")

    _PREDICT_HEALTH_URL = "https://api.predict.fun/v1/health"
    _POLY_HEALTH_URL = "https://clob.polymarket.com/health"

    def _api_health_watchdog() -> None:
        _was_down = _halt_api_path.exists()
        _down_reason = ""
        while True:
            time.sleep(_api_check_interval)
            _predict_ok = False
            _poly_ok = False
            _reason = ""
            try:
                _r = httpx.get(_PREDICT_HEALTH_URL, timeout=httpx.Timeout(8.0, connect=4.0))
                _predict_ok = _r.status_code < 500
            except Exception as _e:
                _reason = f"predict: {_e}"
                print(f"[TRADER][API_WATCHDOG] predict_health_failed err={_e}")
            try:
                _kw: dict = {}
                if _proxy_url:
                    _kw["proxy"] = _proxy_url
                _r2 = httpx.get(_POLY_HEALTH_URL, timeout=httpx.Timeout(8.0, connect=4.0), **_kw)
                _poly_ok = _r2.status_code < 500
            except Exception as _e2:
                if not _reason:
                    _reason = f"poly: {_e2}"
                print(f"[TRADER][API_WATCHDOG] poly_health_failed err={_e2}")

            _all_ok = _predict_ok and _poly_ok
            if not _all_ok and not _was_down:
                _was_down = True
                _down_reason = _reason or f"predict_ok={_predict_ok} poly_ok={_poly_ok}"
                _halt_api_path.write_text(_down_reason)
                print(f"[TRADER][API_WATCHDOG] API DOWN — halt_api created reason={_down_reason}")
                notify(
                    "🔴 <b>API УПАЛ — БОТ ОСТАНОВЛЕН</b>\n"
                    "\n"
                    f"predict_ok={_predict_ok} poly_ok={_poly_ok}\n"
                    f"<code>{_down_reason[:200]}</code>\n"
                    f"Проверка каждые {_api_check_interval:.0f}s — возобновлю автоматически\n"
                )
            elif _all_ok and _was_down:
                _was_down = False
                _halt_api_path.unlink(missing_ok=True)
                print("[TRADER][API_WATCHDOG] API RESTORED — halt_api removed")
                notify(
                    "🟢 <b>API ВОССТАНОВЛЕН — БОТ ВОЗОБНОВИЛ РАБОТУ</b>\n"
                    "\n"
                    "predict ✅  polymarket ✅\n"
                )

    threading.Thread(target=_api_health_watchdog, daemon=True, name="api_health_watchdog").start()


# In-memory cooldown to prevent repeated buys during testing.
_predict_market_last_buy_ts: dict[int, float] = {}
# Набор market_id которые сейчас в процессе исполнения — блокирует параллельные трейды
_predict_market_in_flight: set[int] = set()
_predict_market_in_flight_lock = _threading.Lock()
# Disk-persisted in-flight orders: survives container restarts so we can detect
# unhedged Predict positions after crash/restart.
# key: str(market_id)  value: {market_id, order_hash, order_id, token_id, shares, ts}
_INFLIGHT_ORDERS_FILE = Path("/data/predict_inflight.json")
_inflight_orders_file_lock = _threading.Lock()
# Late-fill watcher: orders cancelled by poly_hedge_no_edge whose ghost_fill_watch
# timed out (60s). Background thread checks these for up to 10 min.
# key: order_hash  value: {order_hash, market_id, token_id, shares, ts}
_LATE_WATCH_FILE = Path("/data/predict_late_watch.json")
_late_watch_file_lock = _threading.Lock()
# ── BSC direct verification ──────────────────────────────────────────────────
# CTF Exchange contract on BSC Mainnet (source: predict_sdk/constants.py)
_BSC_CTF_EXCHANGE = "0x8BC070BEdAB741406F4B1Eb65A72bee27894B689"
# Public BSC RPCs — tried in order on failure
# NOTE: eth_getLogs is disabled on all public BSC RPCs (returns -32005).
#       We use eth_call with getOrderStatus(bytes32) instead.
_BSC_RPCS = [
    "https://bsc-dataseed.bnbchain.org/",
    "https://bsc-dataseed1.defibit.io/",
    "https://bsc-dataseed2.defibit.io/",
    "https://bsc-dataseed1.ninicoin.io/",
]
# Also check NegRisk exchange (some Predict markets route through it)
_BSC_NEG_RISK_CTF_EXCHANGE = "0x365fb81bd4A24D6303cd2F19c349dE6894D8d58A"
# keccak256("getOrderStatus(bytes32)")[:4] — computed once on first use
_ORDER_STATUS_SELECTOR: str | None = None
# State for grouping repeated HEDGE FILLED notifications in the same market
# key: poly token_id  value: (message_id, cumulative_pnl, fill_count, timestamp)
_BA_FILL_STATE_FILE = Path("/data/ba_fill_state.json")
_ba_fill_state: dict[str, tuple[int, float, int, float]] = {}


def _save_inflight_order(market_id: int, order_hash: str, order_id: str | None, token_id: str | None, shares: float) -> None:
    """Write a Predict order to the on-disk in-flight registry."""
    try:
        with _inflight_orders_file_lock:
            data: dict = {}
            if _INFLIGHT_ORDERS_FILE.exists():
                try:
                    data = json.loads(_INFLIGHT_ORDERS_FILE.read_text())
                except Exception:
                    data = {}
            data[str(market_id)] = {
                "market_id": market_id,
                "order_hash": order_hash,
                "order_id": order_id,
                "token_id": token_id,
                "shares": shares,
                "ts": time.time(),
            }
            _INFLIGHT_ORDERS_FILE.write_text(json.dumps(data))
    except Exception as _e:
        print(f"[TRADER] save_inflight_order error={_e}")


def _remove_inflight_order(market_id: int) -> None:
    """Remove a market_id entry from the on-disk in-flight registry."""
    try:
        with _inflight_orders_file_lock:
            if not _INFLIGHT_ORDERS_FILE.exists():
                return
            try:
                data = json.loads(_INFLIGHT_ORDERS_FILE.read_text())
            except Exception:
                return
            data.pop(str(market_id), None)
            if data:
                _INFLIGHT_ORDERS_FILE.write_text(json.dumps(data))
            else:
                _INFLIGHT_ORDERS_FILE.unlink(missing_ok=True)
    except Exception as _e:
        print(f"[TRADER] remove_inflight_order error={_e}")


def _check_inflight_on_startup() -> None:
    """On startup, look for Predict orders that were in-flight when container last died.

    If any such orders exist (< 10 min old), cancel all open Predict orders and notify the
    user so they can check for unhedged positions manually.
    """
    try:
        if not _INFLIGHT_ORDERS_FILE.exists():
            return
        try:
            data = json.loads(_INFLIGHT_ORDERS_FILE.read_text())
        except Exception:
            _INFLIGHT_ORDERS_FILE.unlink(missing_ok=True)
            return
        if not data:
            _INFLIGHT_ORDERS_FILE.unlink(missing_ok=True)
            return

        cutoff = time.time() - 600  # only care about orders < 10 min old
        fresh = {k: v for k, v in data.items() if isinstance(v, dict) and float(v.get("ts", 0)) > cutoff}
        # Delete stale entries regardless
        if not fresh:
            _INFLIGHT_ORDERS_FILE.unlink(missing_ok=True)
            print("[TRADER][STARTUP] inflight_file stale entries cleared")
            return

        print(f"[TRADER][STARTUP] ⚠️  inflight orders found at startup count={len(fresh)}: {list(fresh.keys())}")

        # Cancel all open Predict orders (the orders may still be open on-chain)
        try:
            session, _ = _predict_client.get()
            n_cancelled = _predict_cancel_all_open_orders(session)
            print(f"[TRADER][STARTUP] cancelled open orders n={n_cancelled}")
        except Exception as _ce:
            print(f"[TRADER][STARTUP] cancel_open_orders error={_ce}")
            n_cancelled = -1

        # Build notification
        lines = []
        for entry in fresh.values():
            mkt = entry.get("market_id", "?")
            oh = str(entry.get("order_hash") or "?")[:12]
            sh = entry.get("shares", "?")
            age_s = int(time.time() - float(entry.get("ts", time.time())))
            lines.append(
                f"▸ Market: <code>{mkt}</code>  Hash: <code>{oh}...</code>\n"
                f"  Shares: <b>{sh}</b>  Age: <i>{age_s}s</i>"
            )

        cancel_line = f"\n🗑 Cancelled open orders: <b>{n_cancelled}</b>" if n_cancelled >= 0 else ""
        notify(
            "⚠️ <b>RESTART: IN-FLIGHT PREDICT ORDERS</b>\n"
            "<i>Container restarted mid-order — unhedged position possible</i>\n"
            "\n"
            + "\n\n".join(lines)
            + cancel_line
        )

        # Clean up the file after alerting
        _INFLIGHT_ORDERS_FILE.unlink(missing_ok=True)

    except Exception as _e:
        print(f"[TRADER][STARTUP] check_inflight error={_e}")


def _parse_predict_filled_wei(resp: dict[str, Any]) -> int:
    """Parse amountFilled (wei) from Predict GET /orders/{hash} response."""
    data = resp.get("data") if isinstance(resp, dict) else None
    if not isinstance(data, dict):
        return 0
    try:
        return max(0, int(str(data.get("amountFilled") or "0")))
    except (ValueError, TypeError):
        return 0


def _late_watch_save(order_hash: str, market_id: int, token_id: str | None, shares: float) -> None:
    """Register a cancelled Predict order for background late-fill monitoring."""
    try:
        with _late_watch_file_lock:
            data: dict = {}
            if _LATE_WATCH_FILE.exists():
                try:
                    data = json.loads(_LATE_WATCH_FILE.read_text())
                except Exception:
                    data = {}
            data[order_hash] = {
                "order_hash": order_hash,
                "market_id": market_id,
                "token_id": token_id,
                "shares": shares,
                "ts": time.time(),
            }
            _LATE_WATCH_FILE.write_text(json.dumps(data))
    except Exception as _e:
        print(f"[TRADER] late_watch_save error={_e}")


def _auto_hedge_late_fill(token_id: str | None, shares: float, market_id: int) -> str:
    """Emergency Poly hedge after detecting a late BSC fill.

    Fetches live Poly orderbook, computes VWAP, places FOK market buy (fak_fallback=True).
    Returns a short status string for logging/notify.
    """
    if not token_id:
        return "skip:no_token_id"
    if shares <= 0:
        return "skip:shares=0"
    try:
        book = _polymarket_book(token_id)
        vwap = _vwap_from_poly_book(book, shares)
        if vwap is None or vwap <= 0:
            return "skip:no_liquidity"
        stake_usd = shares * vwap * 1.02  # 2% slippage buffer
        leg = OpportunityLeg(
            source="late_fill_recovery",
            side="BUY",
            ts=datetime.utcnow().isoformat(),
            ask=vwap,
            ask_sz=shares,
            pool_usd=0.0,
            shares=shares,
            stake_usd=stake_usd,
            token_id=token_id,
            market_id=market_id,
        )
        result = _place_polymarket_fok_market_buy(leg, fak_fallback=True)
        resp = result.get("response") or {}
        status = resp.get("status", "?")
        return f"ok:{status} stake=${stake_usd:.2f} vwap={vwap:.2f}"
    except Exception as _e:
        return f"error:{_e}"


def _bsc_check_order_filled(order_hash_hex: str, entry_shares: float) -> float:
    """Check if a Predict order was fully filled on BSC via eth_call to getOrderStatus.

    Uses CTFExchange.getOrderStatus(bytes32) → (bool isFilledOrCancelled, uint256 remaining).
    eth_getLogs is NOT used — public BSC RPCs reject it with -32005 (limit exceeded).

    Predict uses OFF-CHAIN cancellations (no on-chain cancelOrder tx), so:
      {isFilledOrCancelled=true, remaining=0} → order was FULLY FILLED on-chain.
      {isFilledOrCancelled=false, remaining=0} → never touched on-chain (cancelled off-chain).
      {isFilledOrCancelled=true, remaining>0} → cancelled on-chain after partial fill.
      {isFilledOrCancelled=false, remaining>0} → partially filled, still open.

    Returns entry_shares if fully filled, 0.0 otherwise or on any error.
    Checks both CTF Exchange and NegRisk CTF Exchange.
    """
    global _ORDER_STATUS_SELECTOR
    try:
        if _ORDER_STATUS_SELECTOR is None:
            from eth_utils import keccak as eth_keccak
            _ORDER_STATUS_SELECTOR = "0x" + eth_keccak(text="getOrderStatus(bytes32)")[:4].hex()

        if not order_hash_hex.startswith("0x"):
            order_hash_hex = "0x" + order_hash_hex
        call_data = _ORDER_STATUS_SELECTOR + order_hash_hex[2:].zfill(64)

        contracts = [_BSC_CTF_EXCHANGE, _BSC_NEG_RISK_CTF_EXCHANGE]
        for _rpc in _BSC_RPCS:
            for _contract in contracts:
                try:
                    resp = requests.post(
                        _rpc,
                        json={
                            "jsonrpc": "2.0",
                            "method": "eth_call",
                            "params": [{"to": _contract, "data": call_data}, "latest"],
                            "id": 1,
                        },
                        timeout=5,
                    )
                    result = resp.json()
                    if "error" in result:
                        continue
                    raw = result.get("result", "")
                    if len(raw) < 2 + 128:  # 0x + 2×32bytes
                        continue
                    raw_bytes = bytes.fromhex(raw[2:])
                    is_fc = bool(int.from_bytes(raw_bytes[0:32], "big"))
                    remaining = int.from_bytes(raw_bytes[32:64], "big")
                    if is_fc and remaining == 0:
                        # Fully filled on-chain
                        cname = "CTF" if _contract == _BSC_CTF_EXCHANGE else "NEG_RISK"
                        print(f"[TRADER][BSC_CHECK] FILLED hash={order_hash_hex[:18]}... contract={cname}")
                        return entry_shares
                except Exception:
                    continue
            # If we got a valid response from first RPC that wasn't filled, stop
            break
    except Exception as _e:
        print(f"[TRADER][BSC_CHECK] err={_e}")
    return 0.0


def _late_fill_watcher() -> None:
    """Background thread: checks cancelled Predict order hashes for late fills.

    When ghost_fill_watch (60s) times out without finding a fill, the order hash
    is saved to _LATE_WATCH_FILE. This thread keeps polling those hashes every 15s
    for up to 10 minutes. If a late fill is detected it sends a Telegram alert —
    the position on Predict is unhedged and requires manual intervention.
    """
    _POLL_INTERVAL = 15.0
    _MAX_WATCH_SEC = 1800  # 30 minutes — Predict API can lag BSC by several minutes
    while True:
        time.sleep(_POLL_INTERVAL)
        try:
            if not _LATE_WATCH_FILE.exists():
                continue
            with _late_watch_file_lock:
                try:
                    data = json.loads(_LATE_WATCH_FILE.read_text())
                except Exception:
                    continue
            if not data:
                _LATE_WATCH_FILE.unlink(missing_ok=True)
                continue

            now = time.time()
            cutoff = now - _MAX_WATCH_SEC
            to_remove: list[str] = []
            try:
                session = _predict_monitor.get()
            except Exception as _se:
                print(f"[TRADER][LATE_WATCH] predict_monitor error={_se}")
                continue

            for oh, entry in list(data.items()):
                age = now - float(entry.get("ts", 0))
                mkt_id = entry.get("market_id", "?")
                if float(entry.get("ts", 0)) < cutoff:
                    # Entry expired after _MAX_WATCH_SEC — Predict API never showed a fill.
                    # Do a final BSC on-chain check to distinguish:
                    #   - real fill (is_fc=True, remaining=0) → strong alert, unhedged position
                    #   - clean cancel (never seen on-chain) → mild alert, position likely not open
                    bsc_shares = _bsc_check_order_filled(oh, float(entry.get("shares", 0)))
                    if bsc_shares > 0:
                        _hedge_st = _auto_hedge_late_fill(entry.get("token_id"), bsc_shares, int(mkt_id) if str(mkt_id).isdigit() else 0)
                        print(
                            f"[TRADER][LATE_WATCH] 🔴 BSC_FILL_CONFIRMED_ON_EXPIRY "
                            f"hash={oh[:14]}... market_id={mkt_id} "
                            f"bsc_shares={bsc_shares:.4f} age={age:.0f}s hedge={_hedge_st}"
                        )
                        notify(
                            f"🔴 <b>BSC fill confirmed — auto-hedge</b>\n"
                            f"<i>Watcher expired, Predict API showed 0 for 30 min</i>\n"
                            f"\n"
                            f"▸ Shares: <b>{bsc_shares:.3f}</b>\n"
                            f"▸ Hash: <code>{oh[:18]}...</code>\n"
                            f"▸ Market: <code>{mkt_id}</code>\n"
                            f"\n"
                            f"Poly hedge: <code>{_hedge_st}</code>"
                        )
                    else:
                        print(
                            f"[TRADER][LATE_WATCH] ℹ️ WATCH_EXPIRED_BSC_EMPTY "
                            f"hash={oh[:14]}... market_id={mkt_id} "
                            f"shares={entry.get('shares', '?')} age={age:.0f}s"
                        )
                        # No notify — BSC empty = clean cancel, position not open
                    to_remove.append(oh)
                    continue
                try:
                    resp = _predict_get_order_by_hash(session, oh)
                    filled_wei = _parse_predict_filled_wei(resp)
                    if filled_wei > 0:
                        shares = filled_wei / 10 ** 18
                        _hedge_st = _auto_hedge_late_fill(entry.get("token_id"), shares, int(mkt_id) if str(mkt_id).isdigit() else 0)
                        print(
                            f"[TRADER][LATE_WATCH] ⚠️ LATE GHOST FILL DETECTED (API) "
                            f"hash={oh[:14]}... market_id={mkt_id} "
                            f"shares={shares:.4f} age={age:.0f}s hedge={_hedge_st}"
                        )
                        notify(
                            f"🟡 <b>Late ghost fill — auto-hedge triggered</b>\n"
                            f"<i>Predict API reported fill +{age:.0f}s after cancel</i>\n"
                            f"\n"
                            f"▸ Shares: <b>{shares:.3f}</b>\n"
                            f"▸ Hash: <code>{oh[:18]}...</code>\n"
                            f"▸ Market: <code>{mkt_id}</code>\n"
                            f"\n"
                            f"Poly hedge: <code>{_hedge_st}</code>"
                        )
                        to_remove.append(oh)
                    else:
                        # Predict API still shows 0 — check BSC directly as fallback.
                        # The API indexer can lag on-chain confirms by several minutes.
                        bsc_shares = _bsc_check_order_filled(oh, float(entry.get("shares", 0)))
                        if bsc_shares > 0:
                            _hedge_st = _auto_hedge_late_fill(entry.get("token_id"), bsc_shares, int(mkt_id) if str(mkt_id).isdigit() else 0)
                            print(
                                f"[TRADER][LATE_WATCH] 🔴 BSC_FILL_DETECTED (API lag!) "
                                f"hash={oh[:14]}... market_id={mkt_id} "
                                f"bsc_shares={bsc_shares:.4f} age={age:.0f}s hedge={_hedge_st}"
                            )
                            notify(
                                f"🔴 <b>BSC fill (API lag) — auto-hedge triggered</b>\n"
                                f"<i>On-chain fill detected, Predict API lag = {age:.0f}s</i>\n"
                                f"\n"
                                f"▸ Shares: <b>{bsc_shares:.3f}</b>\n"
                                f"▸ Hash: <code>{oh[:18]}...</code>\n"
                                f"▸ Market: <code>{mkt_id}</code>\n"
                                f"\n"
                                f"Poly hedge: <code>{_hedge_st}</code>"
                            )
                            to_remove.append(oh)
                except Exception as _pe:
                    print(f"[TRADER][LATE_WATCH] check_error hash={oh[:14]} err={_pe}")

            if to_remove:
                with _late_watch_file_lock:
                    try:
                        data = json.loads(_LATE_WATCH_FILE.read_text())
                        for k in to_remove:
                            data.pop(k, None)
                        if data:
                            _LATE_WATCH_FILE.write_text(json.dumps(data))
                        else:
                            _LATE_WATCH_FILE.unlink(missing_ok=True)
                    except Exception:
                        pass
        except Exception as _e:
            print(f"[TRADER][LATE_WATCH] loop_error={_e}")


def _load_ba_fill_state() -> None:
    """Load persisted fill state from disk on startup."""
    try:
        if _BA_FILL_STATE_FILE.exists():
            raw = json.loads(_BA_FILL_STATE_FILE.read_text())
            cutoff = time.time() - 1800  # drop entries older than GROUP_TTL
            for k, v in raw.items():
                if isinstance(v, list) and len(v) == 4 and float(v[3]) > cutoff:
                    _ba_fill_state[k] = (int(v[0]), float(v[1]), int(v[2]), float(v[3]))
    except Exception:
        pass


def _save_ba_fill_state() -> None:
    """Persist fill state to disk so replies survive container restarts."""
    try:
        _BA_FILL_STATE_FILE.write_text(
            json.dumps({k: list(v) for k, v in _ba_fill_state.items()})
        )
    except Exception:
        pass

# In-memory hourly P&L log: list of (unix_ts, net_pnl) for the last hour
_trade_pnl_log: list[tuple[float, float]] = []
_pnl_checkpoint_ts: float = time.time() - 3600  # rolling 1-hour window, survives restarts


def _pnl_last_hour() -> tuple[float, int]:
    """Return (sum_net_pnl, count) for trades since _pnl_checkpoint_ts, reading from file."""
    cutoff = _pnl_checkpoint_ts
    success_file = os.environ.get("TRADER_SUCCESS_TRADES_FILE", "/data/trades_success.jsonl")
    p = Path(success_file)
    if not p.exists():
        return 0.0, 0
    total, count = 0.0, 0
    try:
        for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if not row.get("ok"):
                    continue
                ts_str = row.get("ts", "")
                if not ts_str:
                    continue
                ts = datetime.fromisoformat(ts_str.rstrip("Z")).timestamp()
                if ts < cutoff:
                    continue
                # Use stored net_pnl if available (most accurate)
                if "net_pnl" in row:
                    trade_pnl = float(row["net_pnl"])
                else:
                    lr = row.get("live_hedge_recheck") or {}
                    hq = float(lr.get("hedge_qty") or 0)
                    pb = float(lr.get("pred_bid") or 0)
                    vwap = float(lr.get("live_poly_vwap") or 0)
                    lf = float(lr.get("live_poly_fee") or 0)
                    pred_fee_bps = float(os.environ.get("PREDICT_FEE_BPS", "0") or "0")
                    gross = hq * (1.0 - pb - vwap)
                    trade_pnl = gross - lf * hq - pred_fee_bps / 10_000 * pb * hq
                total += trade_pnl
                count += 1
            except Exception:
                pass
    except Exception:
        pass
    return total, count


def _fmt_usd(x: float | int | None) -> str:
    if x is None:
        return "n/a"
    try:
        return f"${float(x):.4f}"
    except Exception:
        return "n/a"


def _fetch_poly_position(token_id: str, timeout: float = 3.0) -> tuple[float, float] | None:
    """Fetch current position for token_id from Polymarket data API.

    Returns (total_shares, avg_price) or None on failure.
    """
    wallet = os.environ.get("POLY_FUNDER", "").strip()
    if not wallet or not token_id:
        return None
    try:
        proxy = os.environ.get("PROXY_URL", "").strip() or None
        proxies = {"http": proxy, "https": proxy} if proxy else None
        resp = requests.get(
            "https://data-api.polymarket.com/positions",
            params={"user": wallet, "sizeThreshold": -1, "limit": 500},
            timeout=timeout,
            proxies=proxies,
        )
        if resp.status_code != 200:
            return None
        positions = resp.json()
        for pos in positions:
            if str(pos.get("asset", "") or pos.get("token_id", "")) == str(token_id):
                size = float(pos.get("size", 0) or pos.get("shares", 0) or 0)
                avg = float(pos.get("avgPrice", 0) or pos.get("avg_price", 0) or 0)
                return size, avg
        return None
    except Exception:
        return None


def _leg_summary(leg: "OpportunityLeg") -> dict[str, Any]:
    return {
        "source": leg.source,
        "side": leg.side,
        "ask": float(leg.ask),
        "ask_sz": float(leg.ask_sz),
        "shares": float(leg.shares),
        "stake_usd": float(leg.stake_usd),
        "pool_usd": float(leg.pool_usd),
        "market_id": leg.market_id,
        "token_id": leg.token_id,
    }


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _test_mode() -> bool:
    return bool(CFG.test_mode)


def _get_max_trade_usd() -> float:
    return float(CFG.max_trade_usd)


def _get_poly_min_order_usd() -> float:
    return float(CFG.poly_min_order_usd)


def _get_predict_min_order_usd() -> float:
    return float(CFG.predict_min_order_usd)


def _cap_opportunity(opp: Opportunity) -> Opportunity:
    max_usd = _get_max_trade_usd()
    if max_usd <= 0:
        return opp
    if opp.stake_usd <= max_usd + 1e-9:
        return opp

    scale = max_usd / max(1e-12, opp.stake_usd)
    legs: list[OpportunityLeg] = []
    for l in opp.legs:
        legs.append(
            OpportunityLeg(
                **{**l.model_dump(), "shares": l.shares * scale, "stake_usd": l.stake_usd * scale}
            )
        )

    return Opportunity(
        **{**opp.model_dump(), "shares": opp.shares * scale, "stake_usd": opp.stake_usd * scale,
           "payout_usd": opp.payout_usd * scale, "profit_usd": opp.profit_usd * scale, "legs": legs}
    )


def _wei_from_float(x: float) -> int:
    return int(round(x * 10**18))


def _get_predict_market_cooldown_sec() -> float:
    return float(CFG.predict_market_cooldown_sec)


def _normalize_hex_key(k: str) -> str:
    k = k.strip()
    if not k:
        return k
    if not k.startswith("0x"):
        return "0x" + k
    return k


def _predict_market(session: requests.Session, market_id: int) -> dict[str, Any]:
    r = session.get(f"https://api.predict.fun/v1/markets/{market_id}", timeout=5)
    r.raise_for_status()
    j = r.json()
    if not j.get("success"):
        raise RuntimeError(f"predict_get_market_failed market_id={market_id}")
    data = j.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(f"predict_get_market_bad_response market_id={market_id}")
    return data


def _predict_orderbook(session: requests.Session, market_id: int) -> dict[str, Any]:
    r = session.get(f"https://api.predict.fun/v1/markets/{market_id}/orderbook", timeout=5)
    r.raise_for_status()
    j = r.json()
    if not isinstance(j, dict) or not j.get("success"):
        raise RuntimeError(f"predict_get_orderbook_failed market_id={market_id}")
    data = j.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(f"predict_get_orderbook_bad_response market_id={market_id}")
    return data


def _predict_get_order_by_hash(session: requests.Session, order_hash: str) -> dict[str, Any]:
    order_hash = (order_hash or "").strip()
    if not order_hash:
        raise RuntimeError("predict_missing_order_hash")
    r = session.get(f"https://api.predict.fun/v1/orders/{order_hash}", timeout=5)
    if not r.ok:
        raise RuntimeError(f"predict_get_order_http_{r.status_code}: {r.text[:500]}")
    j = r.json()
    if not isinstance(j, dict):
        raise RuntimeError("predict_get_order_bad_response")
    return j


def _predict_remove_orders(session: requests.Session, ids: list[str]) -> dict[str, Any]:
    ids = [str(x).strip() for x in (ids or []) if str(x).strip()]
    if not ids:
        raise RuntimeError("predict_missing_order_ids")
    r = session.post(
        "https://api.predict.fun/v1/orders/remove",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"data": {"ids": ids}}),
        timeout=10,
    )
    if not r.ok:
        raise RuntimeError(f"predict_remove_order_http_{r.status_code}: {r.text[:500]}")
    j = r.json()
    if not isinstance(j, dict):
        raise RuntimeError("predict_remove_order_bad_response")
    return j


def _predict_cancel_all_open_orders(session: requests.Session) -> int:
    """Fetch all OPEN orders on Predict and cancel them to release locked collateral.

    Returns number of orders cancelled.
    """
    try:
        r = session.get(
            "https://api.predict.fun/v1/orders",
            params={"status": "OPEN", "limit": "100"},
            timeout=10,
        )
        if not r.ok:
            print(f"[TRADER] predict_cancel_all_open_orders fetch_failed http={r.status_code}")
            return 0
        data = r.json()
        orders = []
        if isinstance(data, dict):
            orders = data.get("data") or data.get("orders") or []
        elif isinstance(data, list):
            orders = data
        if not orders:
            return 0
        ids = [str(o.get("orderId") or o.get("id") or "").strip() for o in orders if isinstance(o, dict)]
        ids = [x for x in ids if x]
        if not ids:
            return 0
        _predict_remove_orders(session, ids)
        print(f"[TRADER] predict_cancel_all_open_orders cancelled={len(ids)} ids={ids[:5]}")
        return len(ids)
    except Exception as _e:
        print(f"[TRADER] predict_cancel_all_open_orders error={_e}")
        return 0


def _polymarket_book(token_id: str) -> dict[str, Any]:
    r = requests.get("https://clob.polymarket.com/book", params={"token_id": token_id}, timeout=5)
    if not r.ok:
        raise RuntimeError(f"polymarket_book_http_{r.status_code}: {r.text[:500]}")
    j = r.json()
    if not isinstance(j, dict):
        raise RuntimeError("polymarket_book_bad_response")
    return j


_poly_fee_rate_cache: dict[str, float] = {}
_POLY_FEE_RATE_FALLBACK = 0.072  # Crypto category default


def _fetch_poly_fee_rate(token_id: str) -> float:
    """Fetch taker fee rate from Polymarket CLOB API. Caches per token_id."""
    cached = _poly_fee_rate_cache.get(token_id)
    if cached is not None:
        return cached
    try:
        r = requests.get("https://clob.polymarket.com/fee-rate", params={"token_id": token_id}, timeout=5)
        r.raise_for_status()
        data = r.json()
        rate_bps = float(data.get("fee_rate_bps", data.get("feeRateBps", 0)) or 0)
        rate = rate_bps / 10_000 if rate_bps > 1 else rate_bps
        if rate <= 0:
            rate = _POLY_FEE_RATE_FALLBACK
        _poly_fee_rate_cache[token_id] = rate
        return rate
    except Exception:
        _poly_fee_rate_cache[token_id] = _POLY_FEE_RATE_FALLBACK
        return _POLY_FEE_RATE_FALLBACK


def _vwap_from_poly_book(book: dict[str, Any], shares: float) -> float | None:
    """Calculate VWAP from live Polymarket asks book for a given number of shares.
    Returns None if insufficient liquidity."""
    asks_raw = book.get("asks") or []
    if not asks_raw or shares <= 0:
        return None
    # Sort asks by price ascending
    levels: list[tuple[float, float]] = []
    for a in asks_raw:
        try:
            p = float(a.get("price", 0))
            sz = float(a.get("size", 0))
            if p > 0 and sz > 0:
                levels.append((p, sz))
        except (ValueError, TypeError):
            continue
    levels.sort(key=lambda x: x[0])
    remaining = shares
    cost = 0.0
    got = 0.0
    for p, sz in levels:
        if remaining <= 0:
            break
        take = min(remaining, sz)
        cost += take * p
        got += take
        remaining -= take
    if got <= 0:
        return None
    return cost / got


def _predict_token_id_for_side(market: dict[str, Any], side: str) -> str:
    outcomes = market.get("outcomes")
    if not isinstance(outcomes, list):
        raise RuntimeError("predict_market_missing_outcomes")

    want = "up" if side == "up" else "down"
    for o in outcomes:
        if not isinstance(o, dict):
            continue
        name = str(o.get("name", "")).strip().lower()
        if name == want:
            token_id = o.get("onChainId")
            if token_id:
                return str(token_id)

    for o in outcomes:
        if not isinstance(o, dict):
            continue
        name = str(o.get("name", "")).strip().lower()
        if want == "up" and name in {"yes", "y"}:
            token_id = o.get("onChainId")
            if token_id:
                return str(token_id)
        if want == "down" and name in {"no", "n"}:
            token_id = o.get("onChainId")
            if token_id:
                return str(token_id)

    raise RuntimeError(f"predict_outcome_not_found side={side}")


def _dump_obj(x: Any) -> Any:
    if x is None:
        return None

    if hasattr(x, "model_dump"):
        return x.model_dump()
    if hasattr(x, "dict"):
        try:
            return x.dict()  # type: ignore[no-any-return]
        except Exception:
            pass
    if hasattr(x, "__dict__"):
        return dict(x.__dict__)
    return x


def _dt_to_epoch_s(dt: datetime | None) -> float | None:
    if dt is None:
        return None
    try:
        return dt.replace(tzinfo=None).timestamp()
    except Exception:
        return None


def _predict_order_to_api(order_obj: dict[str, Any]) -> dict[str, Any]:
    # Predict REST expects camelCase fields (see dev.predict.fun "Create an order")
    # while SDK objects can be snake_case depending on version.
    mapping = {
        "token_id": "tokenId",
        "maker_amount": "makerAmount",
        "taker_amount": "takerAmount",
        "fee_rate_bps": "feeRateBps",
        "signature_type": "signatureType",
    }

    out: dict[str, Any] = {}
    for k, v in order_obj.items():
        out[mapping.get(k, k)] = v

    # Ensure mandatory keys are present in the expected naming.
    if "tokenId" not in out and "token_id" in order_obj:
        out["tokenId"] = order_obj["token_id"]
    if "makerAmount" not in out and "maker_amount" in order_obj:
        out["makerAmount"] = order_obj["maker_amount"]
    if "takerAmount" not in out and "taker_amount" in order_obj:
        out["takerAmount"] = order_obj["taker_amount"]
    if "feeRateBps" not in out and "fee_rate_bps" in order_obj:
        out["feeRateBps"] = order_obj["fee_rate_bps"]

    # Convert some known numeric fields to int if they come as strings.
    for nkey in ("side", "signatureType"):
        if nkey in out and isinstance(out[nkey], str) and out[nkey].isdigit():
            out[nkey] = int(out[nkey])

    return out


def _parse_iso_dt(s: str) -> datetime | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s[:-1] + "+00:00")
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _predict_resp_is_filled(resp: Any) -> bool:
    if resp is None:
        return False
    if not isinstance(resp, dict):
        return False

    # Internal wrapper shape: {filled: bool, create: {...}, get: {...}, ...}
    filled_flag = resp.get("filled")
    if filled_flag is True:
        return True

    for nested_key in ("get", "create"):
        nested = resp.get(nested_key)
        if not isinstance(nested, dict):
            continue
        if _predict_resp_is_filled(nested):
            return True

    data = resp.get("data")
    if isinstance(data, dict):
        for k in ("status", "state"):
            v = data.get(k)
            if isinstance(v, str) and v.strip().lower() in {"filled", "matched", "executed"}:
                return True

        filled = data.get("filledAmount") or data.get("filled")
        if isinstance(filled, (int, float)) and float(filled) > 0:
            return True

        amount_filled = data.get("amountFilled")
        if amount_filled is not None:
            try:
                if int(str(amount_filled)) > 0:
                    return True
            except Exception:
                pass

        status = str(data.get("status") or "").upper()
        if status in {"FILLED", "MATCHED", "COMPLETED", "SETTLED"}:
            return True

    return False


def _extract_predict_filled_shares(predict_result: dict[str, Any], requested_shares: float | None = None) -> float | None:
    """Возвращает фактически исполненные shares из результата predict ордера.

    Парсит data.amountFilled (USDT wei) из GET /v1/orders/{hash} ответа и
    переводит в shares через maker/taker ratio.
    Возвращает None если нельзя определить.
    """
    resp = predict_result.get("response") or {}
    last_get = resp.get("get")
    if isinstance(last_get, dict):
        data = last_get.get("data")
        if isinstance(data, dict):
            amount_filled = data.get("amountFilled")
            if amount_filled is not None:
                try:
                    maker_filled_wei = int(str(amount_filled))
                except (ValueError, TypeError):
                    pass
                else:
                    try:
                        order = data.get("order") or {}
                        maker_amt = int(str(order.get("makerAmount") or "0"))
                        taker_amt = int(str(order.get("takerAmount") or "0"))
                        if maker_amt > 0 and taker_amt > 0 and maker_filled_wei > 0:
                            # Predict API docs are ambiguous about whether amountFilled is in maker or taker units.
                            # Compute both and pick the more plausible value.
                            shares_if_amount_is_taker = maker_filled_wei / 10**18
                            taker_filled_wei = (maker_filled_wei * taker_amt) // maker_amt
                            shares_if_amount_is_maker = taker_filled_wei / 10**18

                            if requested_shares is not None and requested_shares > 0:
                                # Prefer values near requested_shares and never above it (overfill would be a bug).
                                candidates = [
                                    (shares_if_amount_is_taker, abs(shares_if_amount_is_taker - requested_shares)),
                                    (shares_if_amount_is_maker, abs(shares_if_amount_is_maker - requested_shares)),
                                ]
                                candidates.sort(key=lambda x: x[1])
                                chosen = candidates[0][0]
                                return min(chosen, float(requested_shares))

                            # Without requested_shares, return the smaller non-negative value to avoid over-hedging.
                            return max(0.0, min(shares_if_amount_is_taker, shares_if_amount_is_maker))
                    except Exception:
                        pass

    # Fallback: amountFilled ещё не пришёл в GET (blockchain latency),
    # но для FOK с create.success=True takerAmount == фактически исполненные shares.
    taker_wei_s = predict_result.get("taker_amount_wei")
    if taker_wei_s is not None:
        try:
            shares = int(str(taker_wei_s)) / 10**18
            if shares > 0:
                if requested_shares is not None and requested_shares > 0:
                    return min(shares, float(requested_shares))
                return shares
        except (ValueError, TypeError):
            pass

    return None


def _append_jsonl(path_s: str, row: dict[str, Any]) -> None:
    p = Path(path_s)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _preflight(opp: Opportunity, poly_leg: OpportunityLeg, pred_leg: OpportunityLeg) -> list[str]:
    errs: list[str] = []

    if opp.type != "bid_ask_arbitrage":
        errs.append(f"unsupported_type:{opp.type}")

    if poly_leg.token_id is None or not str(poly_leg.token_id).strip():
        errs.append("polymarket_missing_token_id")

    if pred_leg.market_id is None:
        errs.append("predict_missing_market_id")

    if not os.environ.get("POLY_PRIVATE_KEY", "").strip():
        errs.append("missing_env:POLY_PRIVATE_KEY")
    if not os.environ.get("POLY_FUNDER", "").strip():
        errs.append("missing_env:POLY_FUNDER")
    if not os.environ.get("PREDICT_API_KEY", "").strip():
        errs.append("missing_env:PREDICT_API_KEY")
    if not os.environ.get("PREDICT_PRIVATE_KEY", "").strip():
        errs.append("missing_env:PREDICT_PRIVATE_KEY")

    try:
        _ = _get_predict_chain_id()
    except Exception as e:
        errs.append(f"predict_chain_id_invalid:{e}")

    return errs


def _preflight_test(opp: Opportunity, poly_leg: OpportunityLeg, pred_leg: OpportunityLeg) -> list[str]:
    errs: list[str] = []
    if opp.type != "bid_ask_arbitrage":
        errs.append(f"unsupported_type:{opp.type}")
    if poly_leg.token_id is None or not str(poly_leg.token_id).strip():
        errs.append("polymarket_missing_token_id")
    if pred_leg.market_id is None:
        errs.append("predict_missing_market_id")
    return errs


def _get_predict_chain_id() -> ChainId:
    name = os.environ.get("PREDICT_CHAIN_ID", "BNB_MAINNET").strip()
    if not name:
        name = "BNB_MAINNET"
    try:
        return getattr(ChainId, name)
    except AttributeError:
        raise RuntimeError(f"unknown_chain_id:{name}")


def _place_polymarket_fok_market_buy(leg: OpportunityLeg, fak_fallback: bool = False) -> dict[str, Any]:
    """Place a FOK market buy on Polymarket.

    If fak_fallback=True and FOK fails with 'couldn't be fully filled',
    retries immediately with FAK (Fill-And-Kill) which fills whatever is available.
    Used for emergency hedge even at a loss.
    """
    private_key = _normalize_hex_key(os.environ.get("POLY_PRIVATE_KEY", ""))
    funder = os.environ.get("POLY_FUNDER", "").strip()
    signature_type_s = os.environ.get("POLY_SIGNATURE_TYPE", "0").strip()
    poly_api_key = os.environ.get("POLY_API_KEY", "").strip()
    poly_secret = os.environ.get("POLY_SECRET", "").strip()
    poly_passphrase = os.environ.get("POLY_PASSPHRASE", "").strip()

    if not private_key:
        raise RuntimeError("missing_env:POLY_PRIVATE_KEY")
    if not funder:
        raise RuntimeError("missing_env:POLY_FUNDER")
    if not leg.token_id:
        raise RuntimeError("missing_token_id")

    try:
        signature_type = int(signature_type_s)
    except ValueError:
        signature_type = 0

    host = "https://clob.polymarket.com"

    def _build_client() -> ClobClient:
        c = ClobClient(
            host=host,
            chain_id=137,
            key=private_key,
            signature_type=signature_type,
            funder=funder,
        )
        if poly_api_key and poly_secret and poly_passphrase:
            c.set_api_creds(ApiCreds(api_key=poly_api_key, api_secret=poly_secret, api_passphrase=poly_passphrase))
        else:
            c.set_api_creds(c.create_or_derive_api_creds())
        return c

    try:
        client = _build_client()
        mo = MarketOrderArgs(
            token_id=str(leg.token_id),
            amount=float(leg.stake_usd),
            side=BUY,
            order_type=OrderType.FOK,
        )
        signed = client.create_market_order(mo)
        resp = client.post_order(signed, OrderType.FOK)
        return {
            "token_id": leg.token_id,
            "amount_usd": float(leg.stake_usd),
            "response": resp,
            "order_type": "FOK",
        }
    except Exception as e:
        fok_err_str = str(e)
        is_fok_kill = "couldn't be fully filled" in fok_err_str or "FOK" in fok_err_str
        if fak_fallback and is_fok_kill:
            print(
                f"[TRADER][POLY][FAK_FALLBACK] FOK killed, retrying with FAK "
                f"token_id={leg.token_id} amount_usd={float(leg.stake_usd):.4f}"
            )
            try:
                client2 = _build_client()
                mo_fak = MarketOrderArgs(
                    token_id=str(leg.token_id),
                    amount=float(leg.stake_usd),
                    side=BUY,
                    order_type=OrderType.FAK,
                )
                signed_fak = client2.create_market_order(mo_fak)
                resp_fak = client2.post_order(signed_fak, OrderType.FAK)
                return {
                    "token_id": leg.token_id,
                    "amount_usd": float(leg.stake_usd),
                    "response": resp_fak,
                    "order_type": "FAK",
                    "fok_error": fok_err_str,
                }
            except Exception as e2:
                proxy_url = os.environ.get("PROXY_URL", "").strip()
                print(
                    "[TRADER][POLY][FAK_ERROR] "
                    f"token_id={leg.token_id} amount_usd={float(leg.stake_usd):.6f} "
                    f"proxy_set={bool(proxy_url)} err={e2}"
                )
                raise
        proxy_url = os.environ.get("PROXY_URL", "").strip()
        print(
            "[TRADER][POLY][ERROR] "
            f"host={host} token_id={leg.token_id} amount_usd={float(leg.stake_usd):.6f} "
            f"proxy_set={bool(proxy_url)} err_type={type(e).__name__} err={e}"
        )
        print("[TRADER][POLY][TRACE]\n" + traceback.format_exc())
        raise


def _place_polymarket_limit_buy_exact_shares(
    token_id: str,
    shares: float,
    price: float,
    *,
    private_key: str,
    funder: str,
    signature_type: int,
    poly_api_key: str,
    poly_secret: str,
    poly_passphrase: str,
    fak_fallback: bool = True,
) -> dict[str, Any]:
    """Place a limit BUY on Polymarket for an exact share count.

    Uses OrderArgs (share-based) instead of MarketOrderArgs (USD-based),
    so the credited shares == requested shares with no LP-fee deduction.
    Price should be set slightly above live ask to ensure immediate fill.
    """
    host = "https://clob.polymarket.com"

    def _build_client() -> ClobClient:
        c = ClobClient(
            host=host,
            chain_id=137,
            key=private_key,
            signature_type=signature_type,
            funder=funder,
        )
        if poly_api_key and poly_secret and poly_passphrase:
            c.set_api_creds(ApiCreds(api_key=poly_api_key, api_secret=poly_secret, api_passphrase=poly_passphrase))
        else:
            c.set_api_creds(c.create_or_derive_api_creds())
        return c

    import math as _math
    # Poly CLOB enforces 0.001 tick size — round UP to nearest 0.001 to guarantee fill.
    # NOTE: FOK/FAK ("market buy") orders require makerAmount at max 2-decimal USDC precision,
    # i.e. size*price must be a multiple of $0.01. For arbitrary share counts this is nearly
    # impossible to satisfy. GTC limit orders do NOT have this constraint, and since our price
    # is set 2% above live VWAP the order fills immediately as a taker against resting asks.
    _price_ticked = _math.ceil(price * 1000) / 1000
    order_args = OrderArgs(
        token_id=token_id,
        price=min(0.99, _price_ticked),
        size=round(shares, 4),
        side=BUY,
    )
    _GTC_FILL_POLL_INTERVAL = 0.5
    _GTC_FILL_TIMEOUT = float(os.environ.get("POLY_GTC_FILL_TIMEOUT_SEC", "10"))
    try:
        client = _build_client()
        signed = client.create_order(order_args)
        resp = client.post_order(signed, OrderType.GTC)
        resp_status = (resp.get("status") or "").lower()

        if resp_status == "matched":
            return {
                "token_id": token_id,
                "shares_requested": shares,
                "price": price,
                "response": resp,
                "order_type": "GTC",
            }

        if resp_status == "live":
            # Resting order — price was below current ask despite 2% buffer (rare).
            # Poll briefly; cancel and raise if unfilled so the incident handler can retry.
            order_id = resp.get("orderID", "")
            _deadline = time.time() + _GTC_FILL_TIMEOUT
            while time.time() < _deadline:
                time.sleep(_GTC_FILL_POLL_INTERVAL)
                try:
                    _r = _clob_helpers._http_client.get(
                        f"https://clob.polymarket.com/order/{order_id}", timeout=5.0
                    )
                    _order_data = _r.json()
                    _live_status = (_order_data.get("status") or "").lower()
                    if _live_status in ("matched", "filled"):
                        print(
                            f"[TRADER][POLY][GTC_FILLED] order_id={order_id} "
                            f"after_poll elapsed={(time.time() - (_deadline - _GTC_FILL_TIMEOUT)):.1f}s"
                        )
                        resp = _order_data
                        return {
                            "token_id": token_id,
                            "shares_requested": shares,
                            "price": price,
                            "response": resp,
                            "order_type": "GTC",
                        }
                except Exception:
                    pass
            # Timed out — cancel the resting order
            if order_id:
                try:
                    client.cancel(order_id)
                    print(f"[TRADER][POLY][GTC_CANCEL] cancelled live order order_id={order_id}")
                except Exception as _ce:
                    print(f"[TRADER][POLY][GTC_CANCEL_ERR] order_id={order_id} err={_ce}")
            raise RuntimeError(
                f"poly_gtc_not_filled: order went live and did not fill within "
                f"{_GTC_FILL_TIMEOUT:.0f}s (order_id={order_id})"
            )

        # Unexpected status
        raise RuntimeError(f"poly_gtc_unexpected_status: {resp_status} resp={resp}")

    except Exception as _e:
        proxy_url = os.environ.get("PROXY_URL", "").strip()
        print(
            "[TRADER][POLY][LIMIT_ERROR] "
            f"token_id={token_id} shares={shares:.4f} price={price:.4f} "
            f"proxy_set={bool(proxy_url)} err_type={type(_e).__name__} err={_e}"
        )
        print("[TRADER][POLY][LIMIT_TRACE]\n" + traceback.format_exc())
        raise


def _predict_get_jwt(
    session: requests.Session,
    private_key: str,
    predict_account: str | None = None,
    builder: OrderBuilder | None = None,
) -> str:
    """Получает JWT токен predict.fun через подпись auth/message.

    Для Smart Wallet (predict_account задан): подписывает через
    builder.sign_predict_account_message и передаёт predict_account как signer.
    Для EOA: klassное подписание через eth_account.
    """
    r = session.get("https://api.predict.fun/v1/auth/message", timeout=10)
    r.raise_for_status()
    msg = r.json()["data"]["message"]

    if predict_account and builder is not None:
        # Smart Wallet flow — SDK сам формирует EIP-1271 подпись
        signature = builder.sign_predict_account_message(msg)
        signer = predict_account
    else:
        # EOA flow
        from eth_account import Account
        from eth_account.messages import encode_defunct
        acct = Account.from_key(private_key)
        sig_hex = acct.sign_message(encode_defunct(text=msg)).signature.hex()
        signature = "0x" + sig_hex if not sig_hex.startswith("0x") else sig_hex
        signer = acct.address

    if not str(signature).startswith("0x"):
        signature = "0x" + str(signature)

    r2 = session.post(
        "https://api.predict.fun/v1/auth",
        json={"signer": signer, "message": msg, "signature": signature},
        timeout=10,
    )
    r2.raise_for_status()
    return r2.json()["data"]["token"]


def _place_predict_limit_buy(
    leg: OpportunityLeg,
    *,
    poly_hedge_ask: float = 0.0,
    poly_token_id: str | None = None,
    predict_fee_bps: float = 0.0,
    poly_fee_rate: float = 0.072,
    safety_buffer_bps: float = 0.0,
    trace_id: int | None = None,
) -> dict[str, Any]:
    """Post a LIMIT BID on Predict with queue-aware pricing + cancel/replace + partial fill tracking.

    Args:
        poly_hedge_ask: poly VWAP ask used for net-edge guard.
        predict_fee_bps: predict fee bps for net-edge calc.
        poly_fee_rate: Polymarket taker fee rate (fee = rate * p * (1-p)).
        safety_buffer_bps: safety buffer bps for net-edge calc.

    Returns a dict with:
      - response.filled: True if any shares were filled
      - response.partial_fills: list of {ts, delta_shares, cumulative_shares}
      - response.quote_meta: {quote_age_ms, replace_count, cancel_reason, first_fill_ts, ...}
      - standard fields: chain_id, market, token_id, request, response
    """
    _trace = f"[{trace_id}]" if trace_id is not None else ""

    if leg.market_id is None:
        raise RuntimeError("predict_missing_market_id")

    session, builder = _predict_client.get()
    market = _predict_client.get_market(int(leg.market_id))
    chain_id = _get_predict_chain_id()
    fee_rate_bps = int(market.get("feeRateBps") or 0)
    is_neg_risk = bool(market.get("isNegRisk"))
    is_yield_bearing = bool(market.get("isYieldBearing"))
    token_id = leg.token_id or _predict_token_id_for_side(market, leg.side)

    fill_timeout_sec = float(os.environ.get("PREDICT_LIMIT_FILL_TIMEOUT_SEC", "30.0") or "30.0")
    poll_interval_sec = float(os.environ.get("PREDICT_LIMIT_POLL_INTERVAL_SEC", "0.5") or "0.5")
    quote_ttl_sec = float(os.environ.get("PREDICT_QUOTE_TTL_SEC", "10.0") or "10.0")
    max_replaces = int(os.environ.get("PREDICT_QUOTE_MAX_REPLACE", "3") or "3")

    def _build_and_post(bid_price: float) -> dict[str, Any]:
        """Build, sign, and POST a LIMIT BUY order at bid_price. Returns API response."""
        # Snap to tick grid: arbitrary sub-cent prices cause makerAmount/takerAmount ratio
        # to diverge from pricePerShare (integer truncation), resulting in API 400 amounts_mismatch.
        _tick = float(os.environ.get("PREDICT_TICK_SIZE", "0.01") or "0.01")
        bid_price = round(int(bid_price / _tick) * _tick, 6)
        price_per_share_wei = _wei_from_float(bid_price)
        quantity_wei = _wei_from_float(float(leg.shares))
        amounts = builder.get_limit_order_amounts(
            LimitHelperInput(
                side=Side.BUY,
                price_per_share_wei=price_per_share_wei,
                quantity_wei=quantity_wei,
            )
        )
        order = builder.build_order(
            "LIMIT",
            BuildOrderInput(
                side=Side.BUY,
                token_id=str(token_id),
                maker_amount=str(amounts.maker_amount),
                taker_amount=str(amounts.taker_amount),
                fee_rate_bps=fee_rate_bps,
            ),
        )
        typed_data = builder.build_typed_data(order, is_neg_risk=is_neg_risk, is_yield_bearing=is_yield_bearing)
        signed_order = builder.sign_typed_data_order(typed_data)
        signed_dump = _dump_obj(signed_order)
        if not isinstance(signed_dump, dict):
            raise RuntimeError("predict_signed_order_bad")
        order_obj = signed_dump.get("order") if isinstance(signed_dump.get("order"), dict) else None
        signature = signed_dump.get("signature")
        if not order_obj or not signature:
            order_obj = {k: v for k, v in signed_dump.items() if k != "signature"}
            signature = signed_dump.get("signature")
        if not signature:
            raise RuntimeError("predict_missing_signature")
        if not str(signature).startswith("0x"):
            signature = "0x" + str(signature)
        if "signature" not in order_obj:
            order_obj["signature"] = signature
        order_api = _predict_order_to_api(order_obj)

        payload = {
            "data": {
                "pricePerShare": str(amounts.price_per_share),
                "strategy": "LIMIT",
                "slippageBps": "0",
                "order": order_api,
            }
        }
        r = session.post(
            "https://api.predict.fun/v1/orders",
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=float(CFG.timeout_sec),
        )
        if not r.ok:
            if r.status_code == 401:
                _predict_client.invalidate_jwt()
            raise RuntimeError(f"predict_order_http_{r.status_code}: {r.text[:500]}")
        out = r.json()
        if not out.get("success"):
            raise RuntimeError(f"predict_create_order_failed resp={out}")
        return out, payload

    def _get_filled_wei(get_resp: dict[str, Any] | None) -> int:
        """Extract amountFilled (wei) from GET order response."""
        if get_resp is None:
            return 0
        data = get_resp.get("data") if isinstance(get_resp, dict) else None
        if not isinstance(data, dict):
            return 0
        try:
            return max(0, int(str(data.get("amountFilled") or "0")))
        except (ValueError, TypeError):
            return 0

    def _get_status(get_resp: dict[str, Any] | None) -> str:
        if get_resp is None:
            return ""
        data = get_resp.get("data") if isinstance(get_resp, dict) else None
        if not isinstance(data, dict):
            return ""
        return str(data.get("status") or "").upper()

    def _check_predict_book() -> tuple[float | None, float, float | None]:
        """Fetch predict orderbook and return (best_bid_price, best_bid_size, best_ask_price) for our side.
        Uses flat bids/asks (UP-primary book) and computes complement for DOWN side.
        Returns (None, 0.0, None) on error or empty book."""
        try:
            ob = _predict_orderbook(session, int(leg.market_id))
            # The flat bids[]/asks[] are for UP token (same convention as collector).
            # outcomes[] use a different/inverted naming — don't use them.
            bids_flat = ob.get("bids") or []
            asks_flat = ob.get("asks") or []
            if leg.side == "up":
                best_bid = max(bids_flat, key=lambda b: float(b[0])) if bids_flat else None
                best_ask = min(asks_flat, key=lambda a: float(a[0])) if asks_flat else None
                return (
                    float(best_bid[0]) if best_bid else None,
                    float(best_bid[1]) if best_bid else 0.0,
                    float(best_ask[0]) if best_ask else None,
                )
            else:
                # DOWN bids = complement of lowest UP ask; DOWN asks = complement of highest UP bid
                best_up_ask = min(asks_flat, key=lambda a: float(a[0])) if asks_flat else None
                best_up_bid = max(bids_flat, key=lambda b: float(b[0])) if bids_flat else None
                dn_bid = round(1.0 - float(best_up_ask[0]), 6) if best_up_ask else None
                dn_bid_sz = float(best_up_ask[1]) if best_up_ask else 0.0
                dn_ask = round(1.0 - float(best_up_bid[0]), 6) if best_up_bid else None
                return (dn_bid, dn_bid_sz, dn_ask)
        except Exception:
            pass
        return None, 0.0, None

    # Backward-compat shim used in outbid-check loop
    def _check_predict_best_bid() -> tuple[float | None, float]:
        bb, sz, _ = _check_predict_book()
        return bb, sz

    # ── Queue-aware bid pricing ──
    tick_size = float(os.environ.get("PREDICT_TICK_SIZE", "0.01") or "0.01")
    queue_threshold_usd = float(os.environ.get("PREDICT_QUEUE_THRESHOLD_USD", "20.0") or "20.0")
    hard_max_queue_usd = float(os.environ.get("PREDICT_HARD_MAX_QUEUE_USD", "100.0") or "100.0")

    # max_bid: highest bid price where net_edge > 0 after all fees
    # poly fee = feeRate * p * (1-p); using analyzer's poly_hedge_ask as p
    _fee_mult = 1.0 + predict_fee_bps / 10_000
    _poly_dynamic_fee = poly_fee_rate * poly_hedge_ask * (1.0 - poly_hedge_ask)
    max_bid_by_edge = (1.0 - poly_hedge_ask - _poly_dynamic_fee - safety_buffer_bps / 10_000) / _fee_mult if _fee_mult > 0 else 0.0
    _predict_max_bid_price = float(os.environ.get("PREDICT_MAX_BID_PRICE", "0.99") or "0.99")
    max_bid = min(max_bid_by_edge, _predict_max_bid_price)

    def _queue_price(best_bid: float, best_bid_sz: float) -> tuple[float | None, dict[str, Any]]:
        """Determine bid price considering queue ahead.
        Returns (bid_price, meta). bid_price=None means skip.

        Strategy: bid aggressively at min(analyzer_bid, max_bid) — our maximum profitable price.
        This maximises fill probability because we go to the top of the book in one shot.
        Only if max_bid == best_bid (no room to improve) do we fall back to queue-size check.
        """
        q_meta: dict[str, Any] = {
            "best_bid": round(best_bid, 6),
            "best_bid_sz": round(best_bid_sz, 2),
            "max_bid": round(max_bid, 6),
            "queue_ahead_usd": round(best_bid * best_bid_sz, 2),
            "tick_size": tick_size,
        }
        _passive_ticks_miss = int(os.environ.get("PREDICT_PASSIVE_BID_MAX_TICKS_MISS", "5") or "5")
        _ticks_behind = round((best_bid - max_bid) / tick_size) if best_bid > max_bid else 0
        if best_bid > max_bid:
            if _ticks_behind >= _passive_ticks_miss:
                q_meta["decision"] = "skip_best_bid_exceeds_max"
                return None, q_meta
            # Within passive threshold: place at max_bid and wait.
            # cancel_poly_no_edge will kill it if Poly moves against us;
            # replace logic will bump it up if Poly gets cheaper.
            q_meta["decision"] = "bid_passive"
            q_meta["ticks_behind"] = _ticks_behind
            return round(max_bid, 6), q_meta
        # Bid minimally: just 1 tick above best_bid.
        # If outbid, the replace loop will climb 1 tick at a time up to max_bid.
        target_bid = round(best_bid + tick_size, 6)
        if target_bid > max_bid:
            target_bid = round(max_bid, 6)
        if target_bid > best_bid:
            ticks = round((target_bid - best_bid) / tick_size)
            q_meta["decision"] = "bid_at_target"
            q_meta["ticks_improved"] = ticks
            return target_bid, q_meta
        # target_bid == best_bid (max_bid == best_bid, no room to step above)
        queue_usd = best_bid * best_bid_sz
        if queue_usd > hard_max_queue_usd:
            q_meta["decision"] = "skip_hard_max_queue"
            return None, q_meta
        q_meta["decision"] = "join"
        return best_bid, q_meta

    # Fetch live orderbook for queue-aware initial pricing
    analyzer_bid = float(leg.ask)  # analyzer's recommended bid = pred_bid_top
    live_best_bid, live_best_bid_sz, live_best_ask = _check_predict_book()
    queue_pricing_meta: dict[str, Any] = {"analyzer_bid": analyzer_bid}

    if live_best_bid is not None and live_best_bid > 0:
        chosen_price, q_meta = _queue_price(live_best_bid, live_best_bid_sz)
        queue_pricing_meta.update(q_meta)
        if chosen_price is None:
            # Can't profitably quote → return skip
            queue_pricing_meta["action"] = "skip"
            if q_meta.get("decision") == "skip_best_bid_exceeds_max":
                try:
                    print(
                        f"[PREDICT_LIMIT]{_trace} max_bid_breakdown market_id={leg.market_id} "
                        f"poly_ask={poly_hedge_ask:.4f} poly_fee_rate={poly_fee_rate:.5f} "
                        f"poly_dyn_fee={_poly_dynamic_fee:.5f} "
                        f"safety_buffer_bps={float(safety_buffer_bps):.1f} "
                        f"predict_fee_bps={float(predict_fee_bps):.1f} "
                        f"max_bid_by_edge={max_bid_by_edge:.6f} "
                        f"max_bid_cap={_predict_max_bid_price:.4f} "
                        f"max_bid={max_bid:.6f} fee_mult={_fee_mult:.5f}"
                    )
                except Exception:
                    pass
            print(
                f"[PREDICT_LIMIT]{_trace} queue_skip market_id={leg.market_id} "
                f"best_bid={live_best_bid:.4f} sz={live_best_bid_sz:.1f} "
                f"queue=${live_best_bid * live_best_bid_sz:.1f} max_bid={max_bid:.4f} "
                f"reason={q_meta.get('decision')}"
            )
            return {
                "chain_id": str(chain_id),
                "market": {
                    "id": market.get("id"),
                    "title": market.get("title"),
                    "feeRateBps": fee_rate_bps,
                },
                "token_id": token_id,
                "request": None,
                "response": {
                    "filled": False,
                    "orderId": None,
                    "orderHash": None,
                    "partial_fills": [],
                    "total_filled_wei": 0,
                    "total_filled_shares": 0.0,
                    "quote_meta": {"cancel_reason": f"queue_skip:{q_meta.get('decision')}", "queue_pricing": queue_pricing_meta},
                },
            }
        # Enforce minimum order value: price * shares >= PREDICT_MIN_ORDER_USD
        _pred_min_order_val = float(os.environ.get("PREDICT_MIN_ORDER_USD", "0.9") or "0.9")
        _shares_for_value = float(leg.shares) if float(leg.shares) > 0 else 1.0
        _min_price_for_value = _pred_min_order_val / _shares_for_value
        if chosen_price < _min_price_for_value:
            # Round up to nearest tick
            import math as _math
            _ticks = _math.ceil(_min_price_for_value / tick_size)
            chosen_price = round(_ticks * tick_size, 6)
            q_meta["adjusted_for_min_value"] = True
            q_meta["min_price_for_value"] = round(chosen_price, 6)
            if chosen_price > max_bid:
                q_meta["decision"] = "skip_min_value_exceeds_max_bid"
                queue_pricing_meta.update(q_meta)
                queue_pricing_meta["action"] = "skip"
                print(
                    f"[PREDICT_LIMIT] queue_skip market_id={leg.market_id} "
                    f"min_price_for_value={chosen_price:.4f} > max_bid={max_bid:.4f} "
                    f"reason=skip_min_value_exceeds_max_bid"
                )
                return {
                    "chain_id": str(chain_id),
                    "market": {"id": market.get("id"), "title": market.get("title"), "feeRateBps": fee_rate_bps},
                    "token_id": token_id,
                    "request": None,
                    "response": {
                        "filled": False,
                        "orderId": None,
                        "orderHash": None,
                        "partial_fills": [],
                        "total_filled_wei": 0,
                        "total_filled_shares": 0.0,
                        "quote_meta": {
                            "cancel_reason": "queue_skip:skip_min_value_exceeds_max_bid",
                            "queue_pricing": queue_pricing_meta,
                        },
                    },
                }
        current_bid_price = chosen_price
        queue_pricing_meta["action"] = "quote"
        print(
            f"[PREDICT_LIMIT]{_trace} queue_price market_id={leg.market_id} "
            f"best_bid={live_best_bid:.4f} sz={live_best_bid_sz:.1f} "
            f"queue=${live_best_bid * live_best_bid_sz:.1f} "
            f"decision={q_meta.get('decision')} → bid={current_bid_price:.4f} max_bid={max_bid:.4f}"
        )
    else:
        # No live bids → bid at max_bid derived from Poly ask (not Predict ask price).
        # We become the best bid; first seller fills us at our profitable price.
        current_bid_price = round(max_bid, 6)
        queue_pricing_meta["decision"] = "bid_no_queue"
        queue_pricing_meta["action"] = "quote"
        print(
            f"[PREDICT_LIMIT]{_trace} bid_no_queue market_id={leg.market_id} "
            f"max_bid={max_bid:.4f} (no live bids on Predict)"
        )

    out, payload = _build_and_post(current_bid_price)
    create_data = out.get("data") if isinstance(out.get("data"), dict) else {}
    order_id = str(create_data.get("orderId") or "").strip() or None
    order_hash = str(create_data.get("orderHash") or "").strip() or None

    # Persist to disk immediately — so on crash/restart we know this order was in-flight
    if order_hash and leg.market_id is not None:
        _save_inflight_order(
            int(leg.market_id), order_hash, order_id, token_id,
            float(leg.shares or 0)
        )

    quote_post_ts = time.time()
    first_fill_ts: float | None = None
    partial_fills: list[dict[str, Any]] = []
    prev_filled_wei: int = 0
    replace_count: int = 0
    cancel_reason: str | None = None
    last_get: dict[str, Any] | None = None
    all_creates: list[dict[str, Any]] = [out]
    need_final_get_check: bool = False

    t_deadline = time.time() + max(0.0, fill_timeout_sec)
    filled = False

    if order_hash:
        while time.time() < t_deadline:
            # ── Poll fill status ──
            try:
                last_get = _predict_get_order_by_hash(session, order_hash)
            except Exception:
                time.sleep(poll_interval_sec)
                continue

            status = _get_status(last_get)
            current_filled_wei = _get_filled_wei(last_get)

            # Track partial fill deltas
            if current_filled_wei > prev_filled_wei:
                delta_wei = current_filled_wei - prev_filled_wei
                now_ts = time.time()
                if first_fill_ts is None:
                    first_fill_ts = now_ts
                partial_fills.append({
                    "ts": now_ts,
                    "delta_wei": delta_wei,
                    "cumulative_wei": current_filled_wei,
                    "delta_shares": delta_wei / 10**18,
                    "cumulative_shares": current_filled_wei / 10**18,
                })
                prev_filled_wei = current_filled_wei
                print(
                    f"[PREDICT_LIMIT]{_trace} partial_fill hash={order_hash} "
                    f"delta={delta_wei / 10**18:.4f} cumulative={current_filled_wei / 10**18:.4f}"
                )

            # Fully filled?
            if _predict_resp_is_filled(last_get):
                filled = True
                break

            # Terminal status with no fill
            if status in {"CANCELLED", "EXPIRED", "REJECTED"} and current_filled_wei <= 0:
                cancel_reason = f"terminal_status:{status}"
                need_final_get_check = True
                break

            # ── Live Poly hedge-viability check: cancel if hedge became unprofitable ──
            if poly_token_id and current_filled_wei <= 0:
                try:
                    _pb_live = _polymarket_book(poly_token_id)
                    _pa_live = _vwap_from_poly_book(_pb_live, float(leg.shares))
                    if _pa_live and 0 < _pa_live < 1:
                        _pdf_live = poly_fee_rate * _pa_live * (1.0 - _pa_live)
                        _pred_eff_live = current_bid_price * (1.0 + predict_fee_bps / 10_000)
                        _poly_eff_live = _pa_live + _pdf_live
                        _edge_live = 1.0 - _pred_eff_live - _poly_eff_live - safety_buffer_bps / 10_000
                        if _edge_live <= 0:
                            cancel_reason = f"poly_hedge_no_edge:{_edge_live:.4f}"
                            print(
                                f"[PREDICT_LIMIT]{_trace} cancel_poly_no_edge hash={order_hash} "
                                f"poly_ask={_pa_live:.4f} pred_bid={current_bid_price:.4f} "
                                f"edge={_edge_live:.4f}"
                            )
                            # Cancel + verify loop: retry cancel until status is no longer OPEN
                            # BSC можно подтвердить транзакцию до того как cancel API успел —
                            # поэтому проверяем что ордер реально отменился, иначе отменяем снова.
                            _CANCEL_RETRIES = 5
                            _CANCEL_VERIFY_SEC = 2.0
                            for _ci in range(_CANCEL_RETRIES):
                                try:
                                    if order_id:
                                        _predict_remove_orders(session, [order_id])
                                except Exception as _ce:
                                    print(f"[PREDICT_LIMIT]{_trace} cancel_attempt={_ci+1} err={_ce}")
                                time.sleep(_CANCEL_VERIFY_SEC)
                                try:
                                    _cv_get = _predict_get_order_by_hash(session, order_hash)
                                    _cv_status = _get_status(_cv_get)
                                    _cv_filled = _get_filled_wei(_cv_get)
                                    print(
                                        f"[PREDICT_LIMIT]{_trace} cancel_verify attempt={_ci+1}/{_CANCEL_RETRIES} "
                                        f"status={_cv_status} filled_wei={_cv_filled}"
                                    )
                                    if _cv_status in {"CANCELLED", "FILLED", "EXPIRED", "REJECTED"}:
                                        # Reached terminal state — update prev_filled_wei if filled
                                        if _cv_filled > prev_filled_wei:
                                            delta_wei = _cv_filled - prev_filled_wei
                                            now_ts = time.time()
                                            if first_fill_ts is None:
                                                first_fill_ts = now_ts
                                            partial_fills.append({
                                                "ts": now_ts,
                                                "delta_wei": delta_wei,
                                                "cumulative_wei": _cv_filled,
                                                "delta_shares": delta_wei / 10**18,
                                                "cumulative_shares": _cv_filled / 10**18,
                                            })
                                            prev_filled_wei = _cv_filled
                                            print(
                                                f"[PREDICT_LIMIT]{_trace} cancel_verify_fill_detected "
                                                f"hash={order_hash} filled={_cv_filled / 10**18:.4f}"
                                            )
                                        break  # terminal — stop retrying cancel
                                except Exception as _cve:
                                    print(f"[PREDICT_LIMIT]{_trace} cancel_verify_get_err attempt={_ci+1} err={_cve}")
                            need_final_get_check = True
                            break
                        # Also refresh max_bid so outbid check below uses latest
                        _live_dyn_fee2 = poly_fee_rate * _pa_live * (1.0 - _pa_live)
                        _live_max2 = (1.0 - _pa_live - _live_dyn_fee2 - safety_buffer_bps / 10_000) / _fee_mult if _fee_mult > 0 else 0.0
                        max_bid = min(_live_max2, _predict_max_bid_price)
                except Exception:
                    pass  # non-fatal: keep order open on API error

            # ── Outbid check: queue-aware replace when actually outbid ──
            quote_age_sec = time.time() - quote_post_ts
            if quote_age_sec >= quote_ttl_sec and replace_count < max_replaces and current_filled_wei <= 0:
                live_bb, live_bb_sz = _check_predict_best_bid()

                if live_bb is not None and live_bb > current_bid_price + 1e-6:
                    # max_bid already refreshed by hedge-viability check above; skip redundant fetch

                    # We've been outbid → apply queue-aware pricing
                    new_price, rq_meta = _queue_price(live_bb, live_bb_sz)
                    queue_pricing_meta[f"replace_{replace_count}"] = rq_meta

                    if new_price is None:
                        cancel_reason = f"replace_queue_skip:{rq_meta.get('decision')}"
                        print(
                            f"[PREDICT_LIMIT]{_trace} replace_skip hash={order_hash} "
                            f"best_bid={live_bb:.4f} sz={live_bb_sz:.1f} "
                            f"queue=${live_bb * live_bb_sz:.1f} max_bid={max_bid:.4f} "
                            f"reason={rq_meta.get('decision')}"
                        )
                        try:
                            if order_id:
                                _predict_remove_orders(session, [order_id])
                        except Exception:
                            pass
                        need_final_get_check = True
                        break

                    # Net-edge guard on the chosen price
                    pred_eff = new_price * (1.0 + predict_fee_bps / 10_000)
                    poly_eff = poly_hedge_ask + _poly_dynamic_fee
                    net_edge = 1.0 - pred_eff - poly_eff - safety_buffer_bps / 10_000
                    if net_edge <= 0:
                        cancel_reason = "replace_no_edge"
                        print(
                            f"[PREDICT_LIMIT]{_trace} no_edge_for_replace hash={order_hash} "
                            f"new_bid={new_price:.4f} net_edge={net_edge:.4f}"
                        )
                        try:
                            if order_id:
                                _predict_remove_orders(session, [order_id])
                        except Exception:
                            pass
                        need_final_get_check = True
                        break

                    print(
                        f"[PREDICT_LIMIT]{_trace} outbid hash={order_hash} "
                        f"our={current_bid_price:.4f} top={live_bb:.4f} sz={live_bb_sz:.1f} "
                        f"queue=${live_bb * live_bb_sz:.1f} "
                        f"decision={rq_meta.get('decision')} → new_bid={new_price:.4f} net_edge={net_edge:.4f}"
                    )

                    # Cancel old order
                    try:
                        if order_id:
                            _predict_remove_orders(session, [order_id])
                    except Exception as _ce:
                        print(f"[PREDICT_LIMIT]{_trace} cancel_failed hash={order_hash} err={_ce}")

                    # Post replacement order at queue-aware price
                    try:
                        current_bid_price = new_price
                        out_new, payload = _build_and_post(current_bid_price)
                        create_data_new = out_new.get("data") if isinstance(out_new.get("data"), dict) else {}
                        order_id = str(create_data_new.get("orderId") or "").strip() or None
                        order_hash = str(create_data_new.get("orderHash") or "").strip() or None
                        quote_post_ts = time.time()
                        replace_count += 1
                        all_creates.append(out_new)
                        prev_filled_wei = 0  # new order, reset
                        print(
                            f"[PREDICT_LIMIT]{_trace} replaced #{replace_count} "
                            f"new_hash={order_hash} price={current_bid_price:.4f}"
                        )
                    except Exception as _re:
                        cancel_reason = f"replace_failed:{_re}"
                        break
                    continue  # skip sleep, immediately poll new order

            time.sleep(max(0.05, poll_interval_sec))

    # If the poll loop exited via deadline (fill_timeout) without an explicit cancel,
    # treat it as a cancel so ghost_fill_watch + late_watch cover the BSC race window.
    if order_hash and not filled and cancel_reason is None:
        cancel_reason = "fill_timeout"
        need_final_get_check = True

    if order_hash and need_final_get_check and not filled:
        # ANY cancel: on-chain fill can arrive up to ~30s after cancel (BSC block lag).
        # ghost_fill_watch applies to all cancel_reasons — not just poly_hedge_no_edge.
        _FINAL_GET_RETRIES = 60
        _FINAL_GET_SLEEP_SEC = 1.0
        print(
            f"[PREDICT_LIMIT]{_trace} ghost_fill_watch hash={order_hash} "
            f"cancel_reason={cancel_reason} polling up to {_FINAL_GET_RETRIES}s"
        )
        for _attempt in range(_FINAL_GET_RETRIES):
            try:
                last_get = _predict_get_order_by_hash(_predict_monitor.get(), order_hash)
                _final_filled_wei = _get_filled_wei(last_get)
                if _final_filled_wei > prev_filled_wei:
                    delta_wei = _final_filled_wei - prev_filled_wei
                    now_ts = time.time()
                    if first_fill_ts is None:
                        first_fill_ts = now_ts
                    partial_fills.append({
                        "ts": now_ts,
                        "delta_wei": delta_wei,
                        "cumulative_wei": _final_filled_wei,
                        "delta_shares": delta_wei / 10**18,
                        "cumulative_shares": _final_filled_wei / 10**18,
                    })
                    prev_filled_wei = _final_filled_wei
                    print(
                        f"[PREDICT_LIMIT]{_trace} final_fill_check hash={order_hash} "
                        f"cumulative={_final_filled_wei / 10**18:.4f}"
                    )
                if _predict_resp_is_filled(last_get) or prev_filled_wei > 0:
                    filled = True
                    break
            except Exception:
                # Predict API down — fall back to direct BSC check every 5 attempts
                if _attempt % 5 == 0:
                    try:
                        _bsc_shares = _bsc_check_order_filled(order_hash, total_filled_shares if total_filled_shares else float(shares_requested or 0))
                        if _bsc_shares > 0:
                            now_ts = time.time()
                            if first_fill_ts is None:
                                first_fill_ts = now_ts
                            _bsc_wei = int(_bsc_shares * 10**18)
                            if _bsc_wei > prev_filled_wei:
                                partial_fills.append({
                                    "ts": now_ts,
                                    "delta_wei": _bsc_wei - prev_filled_wei,
                                    "cumulative_wei": _bsc_wei,
                                    "delta_shares": _bsc_shares - prev_filled_wei / 10**18,
                                    "cumulative_shares": _bsc_shares,
                                    "source": "bsc_direct",
                                })
                                prev_filled_wei = _bsc_wei
                            print(
                                f"[PREDICT_LIMIT]{_trace} ghost_fill_watch_bsc_direct "
                                f"hash={order_hash} bsc_shares={_bsc_shares:.4f} attempt={_attempt}"
                            )
                            filled = True
                            break
                    except Exception:
                        pass
            if _attempt < _FINAL_GET_RETRIES - 1:
                time.sleep(_FINAL_GET_SLEEP_SEC)

    # ── Final fill check (partial fills count as filled) ──
    if not filled and prev_filled_wei > 0:
        filled = True  # partial fill → still hedge what we got

    # ── Cleanup: cancel unfilled remainder ──
    remove_resp: dict[str, Any] | None = None
    if order_id:
        try:
            # Always try to cancel — if fully filled, API will just ignore it
            remove_resp = _predict_remove_orders(session, [order_id])
        except Exception as _re:
            remove_resp = {"success": False, "error": str(_re)}

    # ── Post-cancel fill sweep ──
    # BSC block confirmation lag: fills can land AFTER the cancel API call returns.
    # Poll until terminal state to capture ALL fills and size the hedge correctly.
    # Without this, partially-filled orders that cancel slowly leave unhedged positions.
    if order_hash:
        _PC_MAX_SEC = float(os.environ.get("PREDICT_POSTCANCEL_SWEEP_SEC", "5.0") or "5.0")
        _PC_POLL_SEC = 0.5
        _pc_deadline = time.time() + _PC_MAX_SEC
        while time.time() < _pc_deadline:
            time.sleep(_PC_POLL_SEC)
            try:
                _pc_get = _predict_get_order_by_hash(session, order_hash)
                _pc_wei = _get_filled_wei(_pc_get)
                if _pc_wei > prev_filled_wei:
                    _pc_delta = _pc_wei - prev_filled_wei
                    _pc_now = time.time()
                    if first_fill_ts is None:
                        first_fill_ts = _pc_now
                    partial_fills.append({
                        "ts": _pc_now,
                        "delta_wei": _pc_delta,
                        "cumulative_wei": _pc_wei,
                        "delta_shares": _pc_delta / 10**18,
                        "cumulative_shares": _pc_wei / 10**18,
                    })
                    prev_filled_wei = _pc_wei
                    print(
                        f"[PREDICT_LIMIT]{_trace} post_cancel_fill hash={order_hash} "
                        f"delta={_pc_delta / 10**18:.4f} cumulative={_pc_wei / 10**18:.4f}"
                    )
                _pc_st = _get_status(_pc_get)
                if _pc_st in {"CANCELLED", "FILLED", "EXPIRED", "REJECTED"}:
                    break
                # Order still OPEN — cancel may have failed; retry
                if _pc_st == "OPEN" and order_id:
                    try:
                        _predict_remove_orders(session, [order_id])
                    except Exception:
                        pass
            except Exception:
                pass

    total_filled_wei = prev_filled_wei
    if not filled and total_filled_wei > 0:
        filled = True  # post-cancel fill found

    quote_total_age_ms = (time.time() - (quote_post_ts - (quote_ttl_sec * replace_count if replace_count else 0))) * 1000.0

    response_obj: dict[str, Any] = {
        "create": all_creates[0] if all_creates else None,
        "all_creates": all_creates,
        "get": last_get,
        "remove": remove_resp,
        "filled": filled,
        "orderId": order_id,
        "orderHash": order_hash,
        "partial_fills": partial_fills,
        "total_filled_wei": total_filled_wei,
        "total_filled_shares": total_filled_wei / 10**18 if total_filled_wei > 0 else 0.0,
        "quote_meta": {
            "quote_age_ms": round((time.time() - quote_post_ts) * 1000.0, 1),
            "quote_total_age_ms": round(quote_total_age_ms, 1),
            "replace_count": replace_count,
            "cancel_reason": cancel_reason,
            "first_fill_ts": first_fill_ts,
            "time_to_first_fill_ms": round((first_fill_ts - quote_post_ts) * 1000.0, 1) if first_fill_ts else None,
            "final_bid_price": current_bid_price,
            "initial_bid_price": float(leg.ask),
            "partial_fill_count": len(partial_fills),
            "queue_pricing": queue_pricing_meta,
        },
    }
    return {
        "chain_id": str(chain_id),
        "market": {
            "id": market.get("id"),
            "title": market.get("title"),
            "feeRateBps": fee_rate_bps,
            "isNegRisk": is_neg_risk,
            "isYieldBearing": is_yield_bearing,
        },
        "token_id": token_id,
        "request": payload,
        "response": response_obj,
    }


def _place_predict_limit_sell(
    leg: OpportunityLeg,
    *,
    sell_qty: float,
    sell_price: float,
    fill_timeout_sec: float = 30.0,
    replace_interval_sec: float = 10.0,
    trace_id: int | None = None,
) -> dict[str, Any]:
    """Place a LIMIT SELL on Predict to unwind a partial position.

    Returns {"filled": bool, "filled_qty": float, "sell_price": float, "order_hash": str|None}.
    Always attempts to cancel remaining shares after timeout.
    """
    _trace = f"[{trace_id}]" if trace_id is not None else ""
    if leg.market_id is None:
        raise RuntimeError("predict_missing_market_id_for_sell")

    session, builder = _predict_client.get()
    market = _predict_client.get_market(int(leg.market_id))
    chain_id = _get_predict_chain_id()
    fee_rate_bps = int(market.get("feeRateBps") or 0)
    is_neg_risk = bool(market.get("isNegRisk"))
    is_yield_bearing = bool(market.get("isYieldBearing"))
    token_id = leg.token_id or _predict_token_id_for_side(market, leg.side)

    _tick = float(os.environ.get("PREDICT_TICK_SIZE", "0.01") or "0.01")
    sell_price = round(int(sell_price / _tick) * _tick, 6)
    price_per_share_wei = _wei_from_float(sell_price)
    quantity_wei = _wei_from_float(sell_qty)

    amounts = builder.get_limit_order_amounts(
        LimitHelperInput(
            side=Side.SELL,
            price_per_share_wei=price_per_share_wei,
            quantity_wei=quantity_wei,
        )
    )
    order = builder.build_order(
        "LIMIT",
        BuildOrderInput(
            side=Side.SELL,
            token_id=str(token_id),
            maker_amount=str(amounts.maker_amount),
            taker_amount=str(amounts.taker_amount),
            fee_rate_bps=fee_rate_bps,
        ),
    )
    typed_data = builder.build_typed_data(order, is_neg_risk=is_neg_risk, is_yield_bearing=is_yield_bearing)
    signed_order = builder.sign_typed_data_order(typed_data)
    signed_dump = _dump_obj(signed_order)
    if not isinstance(signed_dump, dict):
        raise RuntimeError("predict_sell_signed_order_bad")
    order_obj = signed_dump.get("order") if isinstance(signed_dump.get("order"), dict) else None
    signature = signed_dump.get("signature")
    if not order_obj or not signature:
        order_obj = {k: v for k, v in signed_dump.items() if k != "signature"}
        signature = signed_dump.get("signature")
    if not str(signature).startswith("0x"):
        signature = "0x" + str(signature)
    if "signature" not in order_obj:
        order_obj["signature"] = signature
    order_api = _predict_order_to_api(order_obj)

    payload = {
        "data": {
            "pricePerShare": str(amounts.price_per_share),
            "strategy": "LIMIT",
            "slippageBps": "0",
            "order": order_api,
        }
    }
    r = session.post(
        "https://api.predict.fun/v1/orders",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=float(CFG.timeout_sec),
    )
    if not r.ok:
        if r.status_code == 401:
            _predict_client.invalidate_jwt()
        raise RuntimeError(f"predict_sell_http_{r.status_code}: {r.text[:500]}")
    out = r.json()
    if not out.get("success"):
        raise RuntimeError(f"predict_sell_order_failed resp={out}")

    create_data = out.get("data") if isinstance(out.get("data"), dict) else {}
    order_id = str(create_data.get("orderId") or "").strip() or None
    order_hash = str(create_data.get("orderHash") or "").strip() or None

    print(
        f"[TRADER]{_trace} predict_limit_sell placed hash={order_hash} "
        f"qty={sell_qty:.4f} price={sell_price:.4f}"
    )

    # Poll for fill with periodic cancel+re-place one tick lower
    filled = False
    filled_wei = 0
    last_get: dict[str, Any] | None = None
    t_deadline = time.time() + fill_timeout_sec
    current_price = sell_price
    _active_order_id = order_id
    _active_order_hash = order_hash

    while _active_order_hash and time.time() < t_deadline:
        t_replace = time.time() + replace_interval_sec
        # Poll until filled or replace-interval elapsed
        while time.time() < min(t_replace, t_deadline):
            try:
                last_get = _predict_get_order_by_hash(session, _active_order_hash)
                _data = last_get.get("data") if isinstance(last_get, dict) else None
                if isinstance(_data, dict):
                    try:
                        filled_wei = max(0, int(str(_data.get("amountFilled") or "0")))
                    except Exception:
                        pass
                if _predict_resp_is_filled(last_get):
                    filled = True
                    break
            except Exception:
                pass
            time.sleep(0.5)

        if filled:
            break
        if time.time() >= t_deadline:
            break

        # Cancel current order and re-place one tick lower
        if _active_order_id:
            try:
                _predict_remove_orders(session, [_active_order_id])
            except Exception:
                pass
        current_price = max(_tick, round(current_price - _tick, 6))
        print(
            f"[TRADER]{_trace} predict_limit_sell replace → new_price={current_price:.4f} "
            f"remaining_sec={(t_deadline - time.time()):.1f}"
        )
        try:
            _rep_price_wei = _wei_from_float(current_price)
            _rep_amounts = builder.get_limit_order_amounts(
                LimitHelperInput(side=Side.SELL, price_per_share_wei=_rep_price_wei, quantity_wei=quantity_wei)
            )
            _rep_order = builder.build_order(
                "LIMIT",
                BuildOrderInput(
                    side=Side.SELL,
                    token_id=str(token_id),
                    maker_amount=str(_rep_amounts.maker_amount),
                    taker_amount=str(_rep_amounts.taker_amount),
                    fee_rate_bps=fee_rate_bps,
                ),
            )
            _rep_typed = builder.build_typed_data(_rep_order, is_neg_risk=is_neg_risk, is_yield_bearing=is_yield_bearing)
            _rep_signed = builder.sign_typed_data_order(_rep_typed)
            _rep_dump = _dump_obj(_rep_signed)
            if not isinstance(_rep_dump, dict):
                break
            _rep_obj = _rep_dump.get("order") if isinstance(_rep_dump.get("order"), dict) else None
            _rep_sig = _rep_dump.get("signature")
            if not _rep_obj or not _rep_sig:
                _rep_obj = {k: v for k, v in _rep_dump.items() if k != "signature"}
                _rep_sig = _rep_dump.get("signature")
            if not str(_rep_sig).startswith("0x"):
                _rep_sig = "0x" + str(_rep_sig)
            if "signature" not in _rep_obj:
                _rep_obj["signature"] = _rep_sig
            _rep_api = _predict_order_to_api(_rep_obj)
            _rep_payload = {
                "data": {
                    "pricePerShare": str(_rep_amounts.price_per_share),
                    "strategy": "LIMIT",
                    "slippageBps": "0",
                    "order": _rep_api,
                }
            }
            _rep_r = session.post(
                "https://api.predict.fun/v1/orders",
                headers={"Content-Type": "application/json"},
                data=json.dumps(_rep_payload),
                timeout=float(CFG.timeout_sec),
            )
            if _rep_r.ok:
                _rep_out = _rep_r.json()
                _rep_create = _rep_out.get("data") if isinstance(_rep_out.get("data"), dict) else {}
                _active_order_id = str(_rep_create.get("orderId") or "").strip() or None
                _active_order_hash = str(_rep_create.get("orderHash") or "").strip() or None
                print(f"[TRADER]{_trace} predict_limit_sell re-placed hash={_active_order_hash} price={current_price:.4f}")
            else:
                print(f"[TRADER]{_trace} predict_limit_sell re-place http_{_rep_r.status_code} — stop")
                break
        except Exception as _rep_e:
            print(f"[TRADER]{_trace} predict_limit_sell re-place err={_rep_e} — stop")
            break

    # Cancel remainder if still open
    if _active_order_id:
        try:
            _predict_remove_orders(session, [_active_order_id])
        except Exception:
            pass

    filled_qty = filled_wei / 10**18 if filled_wei > 0 else (sell_qty if filled else 0.0)
    return {
        "filled": filled,
        "filled_qty": filled_qty,
        "sell_price": current_price,
        "order_hash": _active_order_hash or order_hash,
        "get": last_get,
    }


def _place_predict_market_buy(leg: OpportunityLeg, timing: dict[str, Any] | None = None) -> dict[str, Any]:
    if leg.market_id is None:
        raise RuntimeError("predict_missing_market_id")

    # Используем персистентный клиент — JWT и builder уже готовы, новый HTTP round-trip не нужен
    session, builder = _predict_client.get()
    market = _predict_client.get_market(int(leg.market_id))

    chain_id = _get_predict_chain_id()
    fee_rate_bps = int(market.get("feeRateBps") or 0)
    is_neg_risk = bool(market.get("isNegRisk"))
    is_yield_bearing = bool(market.get("isYieldBearing"))
    token_id = leg.token_id or _predict_token_id_for_side(market, leg.side)

    # Строим Book с одним ask-уровнем из тика коллектора.
    # Формат: [[price_float, size_float]] — именно такой возвращает predict API.
    # Применяем slippage к цене в book: подписанный ордер будет стоить ask*(1+bps/10000),
    # что позволяет исполниться даже если реальный ask немного выше стейла.
    slippage_bps = int(os.environ.get("PREDICT_SLIPPAGE_BPS", "0") or "0")
    slipped_ask = min(float(leg.ask) * (1.0 + slippage_bps / 10000.0), 0.99)
    book_obj = Book(
        market_id=int(leg.market_id),
        update_timestamp_ms=0,
        bids=[],
        asks=[[slipped_ask, float(leg.ask_sz)]],
    )

    value_wei = _wei_from_float(float(leg.stake_usd))

    amounts = builder.get_market_order_amounts(
        MarketHelperValueInput(side=Side.BUY, value_wei=value_wei),
        book_obj,
    )

    try:
        maker_amt = int(amounts.maker_amount)
        taker_amt = int(amounts.taker_amount)
    except Exception as e:
        raise RuntimeError(f"predict_market_amounts_bad_amounts:{e}")
    if maker_amt <= 0 or taker_amt <= 0:
        raise RuntimeError("predict_market_amounts_zero")
    price_per_share_wei = (maker_amt * 10**18) // taker_amt

    order = builder.build_order(
        "MARKET",
        BuildOrderInput(
            side=Side.BUY,
            token_id=str(token_id),
            maker_amount=str(amounts.maker_amount),
            taker_amount=str(amounts.taker_amount),
            fee_rate_bps=fee_rate_bps,
        ),
    )

    typed_data = builder.build_typed_data(order, is_neg_risk=is_neg_risk, is_yield_bearing=is_yield_bearing)
    signed_order = builder.sign_typed_data_order(typed_data)

    signed_dump = _dump_obj(signed_order)
    if not isinstance(signed_dump, dict):
        raise RuntimeError("predict_signed_order_bad")

    order_obj = signed_dump.get("order") if isinstance(signed_dump.get("order"), dict) else None
    signature = signed_dump.get("signature")
    if not order_obj or not signature:
        order_obj = {k: v for k, v in signed_dump.items() if k != "signature"}
        signature = signed_dump.get("signature")
    if not signature:
        raise RuntimeError("predict_missing_signature")
    if not str(signature).startswith("0x"):
        signature = "0x" + str(signature)
    if "signature" not in order_obj:
        order_obj["signature"] = signature

    order_api = _predict_order_to_api(order_obj)

    payload = {
        "data": {
            "pricePerShare": str(int(price_per_share_wei)),
            "strategy": "MARKET",
            "slippageBps": str(slippage_bps),
            "isFillOrKill": True,
            "order": order_api,
        }
    }

    if timing is not None:
        timing["predict_create_request_ts"] = time.time()

    r = session.post(
        "https://api.predict.fun/v1/orders",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=float(os.environ.get("TRADER_TIMEOUT_SEC", "2.0")),
    )
    if not r.ok:
        # 401 может означать истёкший JWT — инвалидируем для следующего запроса
        if r.status_code == 401:
            _predict_client.invalidate_jwt()
        raise RuntimeError(f"predict_order_http_{r.status_code}: {r.text[:500]}")
    out = r.json()
    if not out.get("success"):
        raise RuntimeError(f"predict_create_order_failed resp={out}")

    create_data = out.get("data") if isinstance(out.get("data"), dict) else {}
    order_id = str(create_data.get("orderId") or "").strip() or None
    order_hash = str(create_data.get("orderHash") or "").strip() or None

    # create.success=True для FOK = принят и исполнен
    filled = out.get("success") is True

    last_get: dict[str, Any] | None = None
    remove_resp: dict[str, Any] | None = None

    fill_timeout_sec = float(os.environ.get("PREDICT_FILL_TIMEOUT_SEC", "2.0"))
    poll_interval_sec = float(os.environ.get("PREDICT_FILL_POLL_INTERVAL_SEC", "0.2"))

    if order_hash:
        if filled:
            # FOK create.success=True → ордер принят биржей.
            # Статус обновляется асинхронно: сразу после POST может быть OPEN/CANCELLED.
            # Делаем до 3 GET с паузой 300мс — этого обычно хватает для подтверждения.
            _FOK_RETRY_PAUSE = 0.3
            _FOK_MAX_RETRIES = 3
            for _attempt in range(_FOK_MAX_RETRIES):
                try:
                    last_get = _predict_get_order_by_hash(session, order_hash)
                except Exception:
                    break
                _data_tmp = (last_get or {}).get("data") or {}
                _af = str(_data_tmp.get("amountFilled") or "0")
                _st = str(_data_tmp.get("status") or "").upper()
                # Подтверждение через amountFilled > 0 или status=FILLED
                try:
                    _af_int = int(_af)
                except (ValueError, TypeError):
                    _af_int = 0
                if _af_int > 0 or _st in {"FILLED", "MATCHED", "COMPLETED", "SETTLED"}:
                    break
                if _attempt < _FOK_MAX_RETRIES - 1:
                    time.sleep(_FOK_RETRY_PAUSE)
            # Если после всех retry финальный статус CANCELLED/REJECTED и ничего не заполнено —
            # create.success был оптимистичным, реального исполнения нет.
            if filled and last_get is not None:
                _final_data = (last_get or {}).get("data") or {}
                _final_st = str(_final_data.get("status") or "").upper()
                _final_af = 0
                try:
                    _final_af = int(str(_final_data.get("amountFilled") or "0"))
                except (ValueError, TypeError):
                    pass
                if _final_st in {"CANCELLED", "EXPIRED", "REJECTED"} and _final_af <= 0:
                    filled = False
        else:
            # Ордер ещё не подтверждён — ждём polling (лимит-ордера и т.д.)
            t_deadline = time.time() + max(0.0, fill_timeout_sec)
            while time.time() < t_deadline:
                last_get = _predict_get_order_by_hash(session, order_hash)
                if _predict_resp_is_filled(last_get):
                    filled = True
                    break
                try:
                    data = (last_get or {}).get("data") or {}
                    status = str(data.get("status") or "").upper()
                    amount_filled = int(str(data.get("amountFilled") or "0"))
                except Exception:
                    status = ""
                    amount_filled = 0
                if status in {"CANCELLED", "EXPIRED", "REJECTED"} and amount_filled <= 0:
                    break
                time.sleep(poll_interval_sec)

            if not filled:
                try:
                    time.sleep(min(0.25, max(0.0, poll_interval_sec)))
                    last_get = _predict_get_order_by_hash(session, order_hash)
                    out["get_final"] = last_get
                    if _predict_resp_is_filled(last_get):
                        filled = True
                except Exception:
                    pass

    if not filled and order_id:
        try:
            remove_resp = _predict_remove_orders(session, [order_id])
        except Exception as e:
            remove_resp = {"success": False, "error": str(e)}

    response_obj: dict[str, Any] = {
        "create": out,
        "get": last_get,
        "remove": remove_resp,
        "filled": filled,
        "orderId": order_id,
        "orderHash": order_hash,
    }

    return {
        "chain_id": str(chain_id),
        "market": {
            "id": market.get("id"),
            "title": market.get("title"),
            "feeRateBps": fee_rate_bps,
            "isNegRisk": is_neg_risk,
            "isYieldBearing": is_yield_bearing,
        },
        "token_id": token_id,
        "taker_amount_wei": str(amounts.taker_amount),
        "request": payload,
        "response": response_obj,
    }


def _predict_auth_preflight() -> None:
    # Через _predict_client — заодно прогревает кеш JWT
    _predict_client.get()


def _predict_preflight_for_leg(leg: OpportunityLeg) -> dict[str, Any]:
    if leg.market_id is None:
        raise RuntimeError("predict_missing_market_id")

    session, builder = _predict_client.get()
    market = _predict_client.get_market(int(leg.market_id))
    token_id = leg.token_id or _predict_token_id_for_side(market, leg.side)
    if token_id is None:
        raise RuntimeError("predict_missing_token_id")

    chain_id = _get_predict_chain_id()
    predict_account = os.environ.get("PREDICT_ACCOUNT", "").strip() or None
    return {
        "market_id": leg.market_id,
        "market_title": market.get("title"),
        "side": leg.side,
        "token_id": token_id,
        "chain_id": str(chain_id),
        "predict_account": predict_account,
    }


@app.post("/test-predict")
def test_predict(opp: Opportunity) -> dict:
    """Выполняет только predict.fun ногу, polymarket — пропускается."""
    opp = _cap_opportunity(opp)
    pred_leg = next((l for l in opp.legs if l.source == "predict"), None)
    if pred_leg is None:
        raise HTTPException(status_code=400, detail="No predict leg found")
    if pred_leg.market_id is None:
        raise HTTPException(status_code=400, detail="predict leg missing market_id")

    print(
        f"[TEST-PREDICT] label={opp.label} shares={pred_leg.shares:.2f} "
        f"stake=${pred_leg.stake_usd:.2f} ask={pred_leg.ask} market_id={pred_leg.market_id}"
    )
    try:
        result = _place_predict_limit_buy(pred_leg)
        print(f"[TEST-PREDICT] OK response={result.get('response')}")
        return {"status": "ok", "predict": result}
    except Exception as e:
        print(f"[TEST-PREDICT] ERROR {e}")
        return {"status": "error", "error": str(e)}


@app.post("/opportunity")
def opportunity(opp: Opportunity) -> dict:
    _data_dir = Path(os.environ.get("TRADER_TRADES_FILE", "/data/trades.jsonl")).parent
    # Check halt flag written by balancer when total balance < BOT_STOP_TOTAL_USD
    _halt_path = _data_dir / "halt"
    if _halt_path.exists():
        print(f"[TRADER] HALTED — halt file exists at {_halt_path}, skipping opportunity")
        return {"status": "halted", "reason": "low_balance"}
    # Check halt flag written by VPN watchdog
    _halt_vpn_path = _data_dir / "halt_vpn"
    if _halt_vpn_path.exists():
        print(f"[TRADER] HALTED — VPN down, halt_vpn exists at {_halt_vpn_path}, skipping opportunity")
        return {"status": "halted", "reason": "vpn_down"}
    # Check halt flag written by API health watchdog
    _halt_api_path = _data_dir / "halt_api"
    if _halt_api_path.exists():
        _reason_text = _halt_api_path.read_text()[:80]
        print(f"[TRADER] HALTED — API down, halt_api exists reason={_reason_text}")
        return {"status": "halted", "reason": "api_down"}

    opp = _cap_opportunity(opp)

    trace_id = next(_TRACE_COUNTER)
    _t = f"[{trace_id}]"

    t0 = time.time()

    dry_run = bool(CFG.dry_run)
    trades_file = str(CFG.trades_file)
    success_trades_file = os.environ.get("TRADER_SUCCESS_TRADES_FILE", "/data/trades_success.jsonl")
    test_mode = _test_mode()

    # Minimal audit log for operator.
    print(
        f"[TRADER]{_t} recv "
        f"label={opp.label} shares={opp.shares:.2f} "
        f"cost=${opp.stake_usd:.2f} payout=${opp.payout_usd:.2f} profit=${opp.profit_usd:.2f} "
        f"sent_at={opp.sent_at} recv_at={datetime.utcnow().isoformat()}Z"
    )
    for i, leg in enumerate(opp.legs, start=1):
        print(
            f"[TRADER]{_t} leg{i} source={leg.source} side={leg.side} ask={leg.ask} "
            f"shares={leg.shares:.2f} stake_usd=${leg.stake_usd:.2f} "
            f"ask_sz={leg.ask_sz} pool_usd={leg.pool_usd:.2f} "
            f"market_id={leg.market_id} token_id={leg.token_id} ts={leg.ts}"
        )

    poly_leg = next((l for l in opp.legs if l.source == "polymarket"), None)
    pred_leg = next((l for l in opp.legs if l.source == "predict"), None)
    if pred_leg is None or poly_leg is None:
        raise HTTPException(status_code=400, detail="Expected both polymarket and predict legs")

    # Guard: не торговать если до закрытия рынка < 30 секунд
    if opp.end_date:
        try:
            _end_dt = datetime.fromisoformat(opp.end_date.rstrip("Z")).replace(tzinfo=timezone.utc)
            _secs_to_end = (_end_dt - datetime.now(timezone.utc)).total_seconds()
            if _secs_to_end < 30:
                print(
                    f"[TRADER]{_t}[SKIP] label={opp.label} "
                    f"reason=market_close_imminent secs_to_end={_secs_to_end:.1f}"
                )
                return {"status": "skipped", "reason": "market_close_imminent", "secs_to_end": round(_secs_to_end, 1)}
        except Exception:
            pass

    # Apply min(bank, pool) across BOTH legs while preserving equal payout (same shares on both).
    # We cap shares by each leg's available top-of-book size (ask_sz).
    sizing: dict[str, Any] = {
        "input": {
            "shares": float(opp.shares),
            "stake_usd": float(opp.stake_usd),
            "poly": {"ask": float(poly_leg.ask), "ask_sz": float(poly_leg.ask_sz), "stake_usd": float(poly_leg.stake_usd)},
            "pred": {"ask": float(pred_leg.ask), "ask_sz": float(pred_leg.ask_sz), "stake_usd": float(pred_leg.stake_usd)},
        }
    }
    q0 = float(opp.shares)
    q_caps = [q0]
    if poly_leg.ask_sz > 0:
        q_caps.append(float(poly_leg.ask_sz))
    if pred_leg.ask_sz > 0:
        q_caps.append(float(pred_leg.ask_sz))
    q = max(0.0, min(q_caps))
    if q + 1e-12 < q0:
        poly_leg = OpportunityLeg(**{**poly_leg.model_dump(), "shares": q, "stake_usd": q * float(poly_leg.ask)})
        pred_leg = OpportunityLeg(**{**pred_leg.model_dump(), "shares": q, "stake_usd": q * float(pred_leg.ask)})
        opp = Opportunity(
            **{
                **opp.model_dump(),
                "shares": q,
                "stake_usd": float(poly_leg.stake_usd) + float(pred_leg.stake_usd),
                "payout_usd": q,
                "profit_usd": q * (1.0 - (float(poly_leg.ask) + float(pred_leg.ask))),
                "legs": [poly_leg, pred_leg],
            }
        )
    sizing["output"] = {
        "shares": float(opp.shares),
        "stake_usd": float(opp.stake_usd),
        "poly_leg_stake_usd": float(poly_leg.stake_usd),
        "pred_leg_stake_usd": float(pred_leg.stake_usd),
    }

    row: dict[str, Any] = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "mode": "test" if test_mode else ("dry_run" if dry_run else "live"),
        "label": opp.label,
        "cap_max_trade_usd": _get_max_trade_usd(),
        "shares": opp.shares,
        "stake_usd": opp.stake_usd,
        "payout_usd": opp.payout_usd,
        "profit_usd": opp.profit_usd,
        "legs": [l.model_dump() for l in opp.legs],
        "sizing": sizing,
        "summary": {
            "label": opp.label,
            "mode": "test" if test_mode else ("dry_run" if dry_run else "live"),
            "shares": float(opp.shares),
            "stake_usd": float(opp.stake_usd),
            "profit_usd": float(opp.profit_usd),
            "poly": _leg_summary(poly_leg),
            "pred": _leg_summary(pred_leg),
        },
        "ok": False,
        "timing": {
            "t0": t0,
            "recv_at": datetime.utcnow().isoformat() + "Z",
            "sent_at": opp.sent_at,
            "poly_quote_ts": poly_leg.ts,
            "pred_quote_ts": pred_leg.ts,
        },
    }

    poly_dt = _parse_iso_dt(poly_leg.ts)
    pred_dt = _parse_iso_dt(pred_leg.ts)
    analyzer_calc_dt = _parse_iso_dt(getattr(opp, "analyzer_calc_at", None))
    analyzer_tick_max_dt = _parse_iso_dt(getattr(opp, "analyzer_tick_ts_max", None))
    now_dt = datetime.utcnow()
    if poly_dt is not None:
        row["timing"]["poly_quote_age_ms"] = (now_dt - poly_dt.replace(tzinfo=None)).total_seconds() * 1000.0
    if pred_dt is not None:
        row["timing"]["pred_quote_age_ms"] = (now_dt - pred_dt.replace(tzinfo=None)).total_seconds() * 1000.0

    poly_min = _get_poly_min_order_usd()
    if not test_mode and not dry_run:
        if poly_leg.stake_usd + 1e-9 < poly_min:
            row["skipped"] = True
            row["skip_reason"] = {
                "code": "poly_min_order_usd",
                "poly_leg_stake_usd": float(poly_leg.stake_usd),
                "poly_min_order_usd": float(poly_min),
            }
            row["summary"]["status"] = "skipped"
            row["summary"]["reason_code"] = "poly_min_order_usd"
            row["summary"]["reason"] = {
                "poly_leg_stake_usd": float(poly_leg.stake_usd),
                "poly_min_order_usd": float(poly_min),
            }
            print(
                f"[TRADER]{_t}[SKIP] "
                f"label={opp.label} reason=poly_min_order_usd "
                f"poly_stake={_fmt_usd(poly_leg.stake_usd)} poly_min={_fmt_usd(poly_min)} "
                f"pred_stake={_fmt_usd(pred_leg.stake_usd)} shares={opp.shares:.4f}"
            )
            _append_jsonl(trades_file, row)
            return {"status": "skipped", "reason": "poly_min_order_usd"}

        pred_min = _get_predict_min_order_usd()
        if pred_leg.stake_usd + 1e-9 < pred_min:
            row["skipped"] = True
            row["skip_reason"] = {
                "code": "predict_min_order_usd",
                "predict_leg_stake_usd": float(pred_leg.stake_usd),
                "predict_min_order_usd": float(pred_min),
            }
            row["summary"]["status"] = "skipped"
            row["summary"]["reason_code"] = "predict_min_order_usd"
            row["summary"]["reason"] = {
                "predict_leg_stake_usd": float(pred_leg.stake_usd),
                "predict_min_order_usd": float(pred_min),
            }
            print(
                f"[TRADER]{_t}[SKIP] "
                f"label={opp.label} reason=predict_min_order_usd "
                f"pred_stake={_fmt_usd(pred_leg.stake_usd)} pred_min={_fmt_usd(pred_min)} "
                f"poly_stake={_fmt_usd(poly_leg.stake_usd)} shares={opp.shares:.4f}"
            )
            _append_jsonl(trades_file, row)
            return {"status": "skipped", "reason": "predict_min_order_usd"}

    if test_mode:
        errs = _preflight_test(opp, poly_leg, pred_leg)
        if errs:
            row["preflight_errors"] = errs
            _append_jsonl(trades_file, row)
            return {"status": "error", "mode": "test", "preflight_errors": errs}

        try:
            session = requests.Session()
            api_key = os.environ.get("PREDICT_API_KEY", "").strip()
            if api_key:
                session.headers.update({"Accept": "application/json", "x-api-key": api_key})
            else:
                session.headers.update({"Accept": "application/json"})

            if CFG.predict_proxy_url:
                session.proxies.update({"http": CFG.predict_proxy_url, "https": CFG.predict_proxy_url})

            row["predict_market"] = _predict_market(session, int(pred_leg.market_id))
            row["polymarket_book"] = _polymarket_book(str(poly_leg.token_id))
            row["ok"] = True
            _append_jsonl(trades_file, row)
            return {"status": "ok", "mode": "test"}
        except Exception as e:
            row["error"] = str(e)
            _append_jsonl(trades_file, row)
            return {"status": "error", "mode": "test", "error": str(e)}

    errs = _preflight(opp, poly_leg, pred_leg)
    if errs:
        row["preflight_errors"] = errs
        _append_jsonl(trades_file, row)
        raise HTTPException(status_code=400, detail={"preflight_errors": errs})

    if dry_run:
        row["summary"]["status"] = "ok"
        row["summary"]["reason_code"] = "dry_run"
        print(
            "[TRADER][DRY_RUN] "
            f"label={opp.label} shares={opp.shares:.4f} stake={_fmt_usd(opp.stake_usd)} profit={_fmt_usd(opp.profit_usd)} "
            f"poly_stake={_fmt_usd(poly_leg.stake_usd)} pred_stake={_fmt_usd(pred_leg.stake_usd)}"
        )
        _append_jsonl(trades_file, row)
        return {"status": "ok", "mode": "dry_run"}

    _market_id_int: int | None = int(pred_leg.market_id) if pred_leg.market_id is not None else None

    try:
        # Cooldown: do not buy the same Predict market more than once per window.
        cooldown_sec = _get_predict_market_cooldown_sec()
        if pred_leg.market_id is not None and cooldown_sec > 0:
            last_ts = _predict_market_last_buy_ts.get(int(pred_leg.market_id))
            now_ts = time.time()
            if last_ts is not None and (now_ts - last_ts) < cooldown_sec:
                remaining = cooldown_sec - (now_ts - last_ts)
                row["skipped"] = True
                row["skip_reason"] = {
                    "code": "predict_market_cooldown",
                    "market_id": int(pred_leg.market_id),
                    "cooldown_sec": float(cooldown_sec),
                    "remaining_sec": float(remaining),
                }
                row["summary"]["status"] = "skipped"
                row["summary"]["reason_code"] = "predict_market_cooldown"
                row["summary"]["reason"] = row["skip_reason"]
                print(
                    f"[TRADER]{_t}[SKIP] "
                    f"label={opp.label} reason=predict_market_cooldown market_id={int(pred_leg.market_id)} "
                    f"remaining_sec={remaining:.1f}"
                )
                _append_jsonl(trades_file, row)
                return {"status": "skipped", "reason": "predict_market_cooldown"}

        # In-flight lock: если этот market_id уже исполняется в другом потоке — пропускаем.
        if _market_id_int is not None:
            with _predict_market_in_flight_lock:
                if _market_id_int in _predict_market_in_flight:
                    print(
                        f"[TRADER]{_t}[SKIP] "
                        f"label={opp.label} reason=predict_market_in_flight market_id={_market_id_int}"
                    )
                    row["skipped"] = True
                    row["skip_reason"] = {"code": "predict_market_in_flight", "market_id": _market_id_int}
                    row["summary"]["status"] = "skipped"
                    row["summary"]["reason_code"] = "predict_market_in_flight"
                    row["summary"]["reason"] = row["skip_reason"]
                    _append_jsonl(trades_file, row)
                    return {"status": "skipped", "reason": "predict_market_in_flight"}
                _predict_market_in_flight.add(_market_id_int)

        # ────────────────────────────────────────────────────
        # STATIC PRE-CHECK: hedge cost at stale poly ask.
        # Runs unconditionally (no live book needed) so it
        # always fires even when the live book fetch fails.
        # ────────────────────────────────────────────────────
        _poly_min_static = float(os.environ.get("POLY_MIN_ORDER_USD", "1.0") or "1.0")
        _static_hedge_cost = float(opp.shares) * float(poly_leg.ask)
        if _static_hedge_cost < _poly_min_static:
            row["skipped"] = True
            row["skip_reason"] = {
                "code": "poly_min_order_usd_static",
                "poly_ask": float(poly_leg.ask),
                "shares": float(opp.shares),
                "hedge_cost_usd": round(_static_hedge_cost, 4),
                "poly_min_order_usd": _poly_min_static,
            }
            row["summary"]["status"] = "skipped"
            row["summary"]["reason_code"] = "poly_min_order_usd_static"
            row["summary"]["reason"] = row["skip_reason"]
            print(
                f"[TRADER]{_t}[SKIP] "
                f"label={opp.label} reason=poly_min_order_usd_static "
                f"static_hedge=${_static_hedge_cost:.2f} min=${_poly_min_static:.2f}"
            )
            if _market_id_int is not None:
                with _predict_market_in_flight_lock:
                    _predict_market_in_flight.discard(_market_id_int)
            _append_jsonl(trades_file, row)
            return {"status": "skipped", "reason": "poly_min_order_usd_static"}

        # ────────────────────────────────────────────────────
        # LIVE POLY NET-EDGE CHECK — перед тем как коммитить
        # predict-капитал, проверяем что live net-edge > 0.
        # ────────────────────────────────────────────────────
        _pre_fee_rate = float(opp.poly_fee_rate or 0.072)
        _pre_pred_fee_bps = float(opp.predict_fee_bps or 0)
        _pre_safety_bps = float(opp.safety_buffer_bps or 0)
        try:
            live_book = _polymarket_book(str(poly_leg.token_id))
            _live_vwap_pre = _vwap_from_poly_book(live_book, float(opp.shares))
            if _live_vwap_pre is not None:
                _live_fee_pre = _pre_fee_rate * _live_vwap_pre * (1.0 - _live_vwap_pre)
                _pred_eff_pre = float(pred_leg.ask) * (1.0 + _pre_pred_fee_bps / 10_000)
                _poly_eff_pre = _live_vwap_pre + _live_fee_pre
                _live_net_edge_pre = 1.0 - _pred_eff_pre - _poly_eff_pre - _pre_safety_bps / 10_000
                row["live_poly_precheck"] = {
                    "stale_poly_ask": float(poly_leg.ask),
                    "live_poly_vwap": round(_live_vwap_pre, 6),
                    "live_poly_fee": round(_live_fee_pre, 6),
                    "pred_ask": float(pred_leg.ask),
                    "live_net_edge": round(_live_net_edge_pre, 6),
                    "live_net_edge_bps": round(_live_net_edge_pre * 10_000, 1),
                }
                print(
                    f"[TRADER]{_t} poly_live_precheck "
                    f"stale={poly_leg.ask} live_vwap={_live_vwap_pre:.4f} "
                    f"live_fee={_live_fee_pre:.4f} pred={pred_leg.ask} "
                    f"net_edge={_live_net_edge_pre:.4f} ({_live_net_edge_pre * 10_000:.1f}bps)"
                )
                if _live_net_edge_pre <= 0:
                    row["skipped"] = True
                    row["skip_reason"] = {
                        "code": "poly_live_no_edge",
                        "stale_poly_ask": float(poly_leg.ask),
                        "live_poly_vwap": round(_live_vwap_pre, 6),
                        "live_net_edge": round(_live_net_edge_pre, 6),
                    }
                    row["summary"]["status"] = "skipped"
                    row["summary"]["reason_code"] = "poly_live_no_edge"
                    row["summary"]["reason"] = row["skip_reason"]
                    print(
                        f"[TRADER]{_t}[SKIP] "
                        f"label={opp.label} reason=poly_live_no_edge "
                        f"live_vwap={_live_vwap_pre:.4f} net_edge={_live_net_edge_pre:.4f}"
                    )
                    _append_jsonl(trades_file, row)
                    return {"status": "skipped", "reason": "poly_live_no_edge"}
                # Hard cap: poly VWAP exceeds POLY_MAX_HEDGE_PRICE
                _pre_poly_max_hedge = float(os.environ.get("POLY_MAX_HEDGE_PRICE", "0.58") or "0.58")
                if _live_vwap_pre >= _pre_poly_max_hedge:
                    row["skipped"] = True
                    row["skip_reason"] = {
                        "code": "poly_live_hedge_price_cap",
                        "live_poly_vwap": round(_live_vwap_pre, 6),
                        "poly_max_hedge_price": _pre_poly_max_hedge,
                    }
                    row["summary"]["status"] = "skipped"
                    row["summary"]["reason_code"] = "poly_live_hedge_price_cap"
                    row["summary"]["reason"] = row["skip_reason"]
                    print(
                        f"[TRADER]{_t}[SKIP] "
                        f"label={opp.label} reason=poly_live_hedge_price_cap "
                        f"live_vwap={_live_vwap_pre:.4f} cap={_pre_poly_max_hedge:.4f}"
                    )
                    _append_jsonl(trades_file, row)
                    return {"status": "skipped", "reason": "poly_live_hedge_price_cap"}
                # Guard: ensure full fill hedge cost >= poly min order ($1).
                # Partial fills (≥1 share but < full qty) may produce a hedge amount below
                # Poly's $1 min. We check at full qty; partial fills are handled post-fill.
                _poly_min_pre = float(os.environ.get("POLY_MIN_ORDER_USD", "1.0") or "1.0")
                _hedge_cost_full = float(opp.shares) * _live_vwap_pre
                if _hedge_cost_full < _poly_min_pre:
                    row["skipped"] = True
                    row["skip_reason"] = {
                        "code": "poly_min_order_usd",
                        "live_poly_vwap": round(_live_vwap_pre, 6),
                        "hedge_cost_usd": round(_hedge_cost_full, 4),
                        "poly_min_order_usd": _poly_min_pre,
                    }
                    row["summary"]["status"] = "skipped"
                    row["summary"]["reason_code"] = "poly_min_order_usd"
                    row["summary"]["reason"] = row["skip_reason"]
                    print(
                        "[TRADER][SKIP] "
                        f"label={opp.label} reason=poly_min_order_usd "
                        f"hedge_cost=${_hedge_cost_full:.2f} min=${_poly_min_pre}"
                    )
                    _append_jsonl(trades_file, row)
                    return {"status": "skipped", "reason": "poly_min_order_usd"}
        except Exception as _e_poly_check:
            print(f"[TRADER]{_t} poly_live_precheck_failed (non-fatal): {_e_poly_check}")

        # ════════════════════════════════════════════════════════════════
        # ПАРАЛЛЕЛЬНАЯ отправка обеих ног через ThreadPoolExecutor.
        # Predict = FOK market buy, Poly = FOK market buy.
        # Обе ноги стартуют одновременно → submit gap ≈ 0.
        # Реальный fill gap зависит от бирж — измеряем через fill_analysis.
        # ════════════════════════════════════════════════════════════════
        incidents_file = os.environ.get("TRADER_INCIDENTS_FILE", "/data/incidents.jsonl")

        # book_age: сколько мс прошло от получения котировки до момента submit
        _submit_wall = time.time()
        _book_freshness: dict[str, Any] = {}
        if poly_dt is not None:
            _book_freshness["poly_book_age_at_submit_ms"] = (_submit_wall - poly_dt.replace(tzinfo=None).timestamp()) * 1000.0
        if pred_dt is not None:
            _book_freshness["pred_book_age_at_submit_ms"] = (_submit_wall - pred_dt.replace(tzinfo=None).timestamp()) * 1000.0
        row["book_freshness"] = _book_freshness

        # ════════════════════════════════════════════════════════════════
        # BID+ASK: Predict LIMIT BID (maker) → Poly FOK hedge (sequential).
        # Predict ставит пассивный лимит-бид; после фила хеджируем на Poly.
        # Cancel/replace при устаревании; partial fills хеджируются по delta.
        # ════════════════════════════════════════════════════════════════
        if opp.type == "bid_ask_arbitrage":
            _pred_timing_ba: dict[str, Any] = {}
            _poly_timing_ba: dict[str, Any] = {}

            # Step 1: LIMIT BID on Predict (blocking with cancel/replace + partial fill tracking)
            predict_result_ba: dict[str, Any] | None = None
            pred_exec_error_ba: Exception | None = None
            _pred_timing_ba["submit_ts"] = time.time()
            try:
                predict_result_ba = _place_predict_limit_buy(
                    pred_leg,
                    poly_hedge_ask=float(poly_leg.ask),
                    poly_token_id=poly_leg.token_id,
                    predict_fee_bps=float(opp.predict_fee_bps or 0),
                    poly_fee_rate=float(opp.poly_fee_rate or 0.072),
                    safety_buffer_bps=float(opp.safety_buffer_bps or 0),
                    trace_id=trace_id,
                )
                _pred_timing_ba["ack_ts"] = time.time()
            except Exception as _e_ba_pred:
                pred_exec_error_ba = _e_ba_pred
                _pred_timing_ba["fail_ts"] = time.time()
            _pred_timing_ba["end"] = time.time()

            _ba_pred_resp = (predict_result_ba.get("response") or {}) if predict_result_ba else {}
            pred_filled_ba = bool(_ba_pred_resp.get("filled", False))
            pred_hash_ba = _ba_pred_resp.get("orderHash")
            _ba_quote_meta = _ba_pred_resp.get("quote_meta") or {}
            _ba_partial_fills = _ba_pred_resp.get("partial_fills") or []
            _ba_total_filled_shares = float(_ba_pred_resp.get("total_filled_shares") or 0.0)

            row["predict"] = predict_result_ba or {}
            row["timing"]["predict_start"] = _pred_timing_ba.get("submit_ts")
            row["timing"]["predict_end"] = _pred_timing_ba.get("end")
            row["timing"]["predict_ack_ts"] = _pred_timing_ba.get("ack_ts")
            if _pred_timing_ba.get("submit_ts") and _pred_timing_ba.get("end"):
                row["timing"]["predict_ms"] = (_pred_timing_ba["end"] - _pred_timing_ba["submit_ts"]) * 1000.0
            # Rich timing from quote_meta
            row["timing"]["predict_quote_age_ms"] = _ba_quote_meta.get("quote_age_ms")
            row["timing"]["predict_time_to_first_fill_ms"] = _ba_quote_meta.get("time_to_first_fill_ms")
            row["timing"]["predict_replace_count"] = _ba_quote_meta.get("replace_count")
            row["timing"]["predict_cancel_reason"] = _ba_quote_meta.get("cancel_reason")

            if not pred_filled_ba:
                _skip_code_ba = "predict_limit_error" if pred_exec_error_ba else "predict_limit_not_filled"
                row["skipped"] = True
                row["skip_reason"] = {
                    "code": _skip_code_ba,
                    "error": str(pred_exec_error_ba) if pred_exec_error_ba else "timeout_or_cancel",
                    "order_hash": pred_hash_ba,
                    "cancel_reason": _ba_quote_meta.get("cancel_reason"),
                    "replace_count": _ba_quote_meta.get("replace_count"),
                    "quote_age_ms": _ba_quote_meta.get("quote_age_ms"),
                }
                row["summary"]["status"] = "skipped"
                row["summary"]["reason_code"] = _skip_code_ba
                print(
                    f"[TRADER]{_t}[SKIP] label={opp.label} reason={_skip_code_ba} "
                    f"hash={pred_hash_ba} cancel_reason={_ba_quote_meta.get('cancel_reason')} "
                    f"replaces={_ba_quote_meta.get('replace_count')} "
                    f"quote_age_ms={_ba_quote_meta.get('quote_age_ms')} err={pred_exec_error_ba}"
                )
                # Auto-cancel stale open orders to release locked collateral
                if pred_exec_error_ba and "insufficient_collateral" in str(pred_exec_error_ba):
                    try:
                        _session_ic, _ = _predict_client.get()
                        _freed = _predict_cancel_all_open_orders(_session_ic)
                        if _freed:
                            print(f"[TRADER] insufficient_collateral_freed_by_cancel freed={_freed}")
                    except Exception as _ic_e:
                        print(f"[TRADER] insufficient_collateral_cancel_err={_ic_e}")
                # Late-fill watch: ghost fill can arrive on BSC after the 60s ghost_fill_watch
                # window. Register ALL cancelled orders for background monitoring up to 30 min.
                # (Previously only poly_hedge_no_edge — but terminal_status:CANCELLED and
                # replace_* cancels can also race with on-chain fills.)
                _ba_cancel_rsn = _ba_quote_meta.get("cancel_reason") or ""
                if pred_hash_ba and _ba_cancel_rsn and pred_leg.market_id is not None:
                    _late_watch_save(str(pred_hash_ba), int(pred_leg.market_id), poly_leg.token_id if poly_leg else None, float(opp.shares))
                    print(f"[TRADER]{_t} late_watch_registered hash={str(pred_hash_ba)[:14]}... cancel_reason={_ba_cancel_rsn}")
                _append_jsonl(trades_file, row)
                return {"status": "skipped", "reason": _skip_code_ba}

            # Predict filled (full or partial) → record cooldown
            if pred_leg.market_id is not None:
                _predict_market_last_buy_ts[int(pred_leg.market_id)] = time.time()

            # Determine hedge quantity: use actual filled shares from predict, not requested
            _ba_hedge_qty = _ba_total_filled_shares if _ba_total_filled_shares > 0 else float(pred_leg.shares)

            # Net sell quantity for unwind: Predict SDK sets taker_amount=qty (full shares) on BUY,
            # meaning fee is charged in USDC (maker_amount), not deducted from shares.
            # The wallet receives exactly amountFilled shares, so sell the full amount.
            _ba_net_sell_qty = _ba_hedge_qty

            # Step 2: Live net-edge recheck before hedging (poly quote may be stale)
            # Fetch live poly orderbook, calculate VWAP at hedge qty, recompute full net-edge
            _ba_actual_pred_bid = float(_ba_quote_meta.get("final_bid_price") or pred_leg.ask)
            # Correct for tick-rounding: use actual on-chain order price (makerAmount/takerAmount)
            # which accounts for tick-snap applied inside _build_and_post.
            # E.g. final_bid_price=0.7272 → displayed as 0.73, but actual order placed at 0.72 tick.
            try:
                _ba_ord_data = (_ba_pred_resp.get("get") or {}).get("data") or {}
                _ba_ord = _ba_ord_data.get("order") or {}
                _ba_mk = int(_ba_ord.get("makerAmount") or 0)
                _ba_tk = int(_ba_ord.get("takerAmount") or 0)
                if _ba_mk > 0 and _ba_tk > 0:
                    _ba_actual_pred_bid = _ba_mk / _ba_tk
            except Exception:
                pass
            _ba_fee_rate = float(opp.poly_fee_rate or 0.072)
            _ba_pred_fee_bps = float(opp.predict_fee_bps or 0)
            _ba_safety_bps = float(opp.safety_buffer_bps or 0)
            _live_net_edge_ba: float | None = None
            _live_vwap_ba: float | None = None
            _live_poly_fee_ba: float | None = None

            try:
                _live_book_ba = _polymarket_book(str(poly_leg.token_id))
                _live_vwap_ba = _vwap_from_poly_book(_live_book_ba, _ba_hedge_qty)
            except Exception as _e_ba_live:
                print(f"[TRADER] bid_ask_hedge_live_check failed (non-fatal): {_e_ba_live}")

            if _live_vwap_ba is not None:
                _live_poly_fee_ba = _ba_fee_rate * _live_vwap_ba * (1.0 - _live_vwap_ba)
                _pred_eff_ba = _ba_actual_pred_bid * (1.0 + _ba_pred_fee_bps / 10_000)
                _poly_eff_ba = _live_vwap_ba + _live_poly_fee_ba
                _live_net_edge_ba = 1.0 - _pred_eff_ba - _poly_eff_ba - _ba_safety_bps / 10_000

                row["live_hedge_recheck"] = {
                    "pred_bid": _ba_actual_pred_bid,
                    "live_poly_vwap": round(_live_vwap_ba, 6),
                    "live_poly_fee": round(_live_poly_fee_ba, 6),
                    "live_net_edge": round(_live_net_edge_ba, 6),
                    "live_net_edge_bps": round(_live_net_edge_ba * 10_000, 1),
                    "hedge_qty": round(_ba_hedge_qty, 4),
                    "poly_fee_rate": _ba_fee_rate,
                }
                print(
                    f"[TRADER] bid_ask_hedge_live_recheck pred_bid={_ba_actual_pred_bid:.4f} "
                    f"live_vwap={_live_vwap_ba:.4f} live_fee={_live_poly_fee_ba:.4f} "
                    f"net_edge={_live_net_edge_ba:.4f} ({_live_net_edge_ba * 10_000:.1f}bps)"
                )
                if _live_net_edge_ba <= 0:
                    _ba_inc_pb = {
                        "ts": datetime.utcnow().isoformat() + "Z",
                        "type": "bid_ask_hedge_no_edge",
                        "label": opp.label,
                        "book_freshness": _book_freshness,
                        "live_poly_vwap": round(_live_vwap_ba, 6),
                        "live_poly_fee": round(_live_poly_fee_ba, 6),
                        "live_net_edge": round(_live_net_edge_ba, 6),
                        "pred_bid": _ba_actual_pred_bid,
                        "pred_filled_qty": float(_ba_hedge_qty),
                        "quote_meta": _ba_quote_meta,
                    }
                    _append_jsonl(incidents_file, _ba_inc_pb)
                    print(
                        f"[TRADER][INCIDENT] BID_ASK_HEDGE_NO_EDGE label={opp.label} "
                        f"net_edge={_live_net_edge_ba:.4f} pred_filled={_ba_hedge_qty:.4f} "
                        f"— forcing hedge to close unhedged predict position"
                    )
            # Hard cap: live poly VWAP exceeds POLY_MAX_HEDGE_PRICE
            _poly_max_hedge_price = float(os.environ.get("POLY_MAX_HEDGE_PRICE", "0.58") or "0.58")
            if _live_vwap_ba is not None and _live_vwap_ba >= _poly_max_hedge_price:
                # Unwind Predict position before declaring price_cap incident
                _pc_unwind_tick = float(os.environ.get("PREDICT_TICK_SIZE", "0.01") or "0.01")
                _pc_unwind_price = max(_pc_unwind_tick, _ba_actual_pred_bid - _pc_unwind_tick)
                _pc_unwind_result: dict[str, Any] = {}
                _pc_unwind_err: str | None = None
                try:
                    _pc_unwind_result = _place_predict_limit_sell(
                        pred_leg,
                        sell_qty=_ba_net_sell_qty,
                        sell_price=_pc_unwind_price,
                        fill_timeout_sec=30.0,
                        trace_id=trace_id,
                    )
                except Exception as _pc_uw_e:
                    _pc_unwind_err = str(_pc_uw_e)
                    print(f"[TRADER][UNWIND_ERROR] price_cap label={opp.label} err={_pc_uw_e}")
                _pc_uw_filled = _pc_unwind_result.get("filled", False)
                _pc_uw_qty = _pc_unwind_result.get("filled_qty", 0.0)

                _ba_inc_hp = {
                    "ts": datetime.utcnow().isoformat() + "Z",
                    "type": "bid_ask_hedge_price_cap",
                    "label": opp.label,
                    "live_poly_vwap": round(_live_vwap_ba, 6),
                    "poly_max_hedge_price": _poly_max_hedge_price,
                    "pred_bid": _ba_actual_pred_bid,
                    "pred_filled_qty": float(_ba_hedge_qty),
                    "quote_meta": _ba_quote_meta,
                    "unwind": _pc_unwind_result,
                    "unwind_error": _pc_unwind_err,
                }
                _append_jsonl(incidents_file, _ba_inc_hp)
                row["ok"] = False
                row["summary"]["status"] = "incident"
                row["summary"]["reason_code"] = "bid_ask_hedge_price_cap"
                print(
                    f"[TRADER][INCIDENT] BID_ASK_HEDGE_PRICE_CAP label={opp.label} "
                    f"live_vwap={_live_vwap_ba:.4f} cap={_poly_max_hedge_price:.4f} "
                    f"unwind_filled={_pc_uw_filled} unwind_qty={_pc_uw_qty:.4f}"
                )
                _pc_uw_status = "✅ продано" if _pc_uw_filled else "❌ не продано — ручная проверка!"
                notify(
                    f"🟡🟡🟡 <b>INCIDENT: HEDGE PRICE CAP → UNWIND</b>\n"
                    f"\n"
                    f"<b>{opp.label}</b>\n"
                    f"\n"
                    f"Poly цена {_live_vwap_ba:.2f} ≥ лимит {_poly_max_hedge_price:.2f} → хедж отменён\n"
                    f"Predict заполнил: <b>{_ba_hedge_qty:.2f} shares</b> @ {_ba_actual_pred_bid:.2f}\n"
                    f"\n"
                    f"Продажа обратно на Predict по {_pc_unwind_price:.2f}: {_pc_uw_status}\n"
                    + (f"Ошибка: {_pc_unwind_err}\n" if _pc_unwind_err else "")
                )
                _append_jsonl(trades_file, row)
                return {"status": "incident", "reason": "bid_ask_hedge_price_cap"}

            # Step 3: Exact-share limit BUY on Polymarket.
            # Use limit order (OrderArgs) with size=net_sell_qty so credited shares == predict net.
            # Price set to live_vwap × 1.02 (2% above) to guarantee immediate fill.
            # Unlike USD market orders, limit orders credit exactly `size` shares (no LP-fee deduction).
            _ba_hedge_vwap = _live_vwap_ba if _live_vwap_ba else float(poly_leg.ask)
            _ba_final_hedge_qty = _ba_net_sell_qty
            _ba_hedge_cost_usd = _ba_net_sell_qty * _ba_hedge_vwap
            _poly_min_hedge = float(os.environ.get("POLY_MIN_ORDER_USD", "1.0") or "1.0")
            # Zone $0.80-$1.00 → over-hedge up to $1.00; below $0.80 → unwind on Predict
            _poly_over_hedge_threshold = float(os.environ.get("POLY_OVER_HEDGE_MIN_USD", "0.80") or "0.80")
            if _ba_hedge_cost_usd < _poly_min_hedge:
                if _ba_hedge_cost_usd >= _poly_over_hedge_threshold:
                    # ── Over-hedge: buy slightly more shares to meet Poly $1 minimum ──
                    _ba_hedge_qty_orig = _ba_final_hedge_qty
                    _ba_final_hedge_qty = (_poly_min_hedge * 1.02) / _ba_hedge_vwap
                    _ba_hedge_cost_usd = _poly_min_hedge * 1.02
                    print(
                        f"[TRADER] BID_ASK_OVER_HEDGE label={opp.label} "
                        f"pred_filled={_ba_hedge_qty_orig:.4f} → boosted={_ba_final_hedge_qty:.4f} "
                        f"cost=${_ba_hedge_cost_usd:.2f}"
                    )
                    # fall through to Step 3 hedge below
                else:
                    # ── Unwind: sell back on Predict at current bid price ──
                    # Guard: if position is dust (< $0.01 value), skip unwind — Predict rejects tiny orders.
                    _min_unwind_usd = 0.01
                    if _ba_net_sell_qty * _ba_actual_pred_bid < _min_unwind_usd:
                        print(
                            f"[TRADER] BID_ASK_BELOW_MIN_DUST_SKIP label={opp.label} "
                            f"qty={_ba_net_sell_qty:.6f} value=${_ba_net_sell_qty * _ba_actual_pred_bid:.4f} "
                            f"— below unwind threshold, skipping"
                        )
                        row["ok"] = False
                        row["summary"]["status"] = "skipped"
                        row["summary"]["reason_code"] = "below_min_dust"
                        _append_jsonl(trades_file, row)
                        return {"status": "skipped", "reason": "below_min_dust"}
                    _unwind_price = max(
                        _poly_over_hedge_threshold / 10,  # floor sanity
                        _ba_actual_pred_bid - float(os.environ.get("PREDICT_TICK_SIZE", "0.01") or "0.01"),
                    )
                    _unwind_result: dict[str, Any] = {}
                    _unwind_err: str | None = None
                    try:
                        _unwind_result = _place_predict_limit_sell(
                            pred_leg,
                            sell_qty=_ba_net_sell_qty,
                            sell_price=_unwind_price,
                            fill_timeout_sec=30.0,
                            trace_id=trace_id,
                        )
                    except Exception as _uw_e:
                        _unwind_err = str(_uw_e)
                        print(f"[TRADER][UNWIND_ERROR] label={opp.label} err={_uw_e}")

                    _uw_filled = _unwind_result.get("filled", False)
                    _uw_qty = _unwind_result.get("filled_qty", 0.0)
                    _uw_loss = (_ba_actual_pred_bid - _unwind_price) * _ba_hedge_qty  # approximate loss

                    _ba_inc_min = {
                        "ts": datetime.utcnow().isoformat() + "Z",
                        "type": "bid_ask_hedge_below_min_unwind",
                        "label": opp.label,
                        "pred_filled_qty": float(_ba_hedge_qty),
                        "hedge_cost_usd": round(_ba_hedge_cost_usd, 4),
                        "poly_min_order_usd": _poly_min_hedge,
                        "unwind_price": _unwind_price,
                        "unwind_filled": _uw_filled,
                        "unwind_qty": _uw_qty,
                        "unwind_error": _unwind_err,
                    }
                    _append_jsonl(incidents_file, _ba_inc_min)
                    row["ok"] = False
                    row["summary"]["status"] = "incident"
                    row["summary"]["reason_code"] = "bid_ask_hedge_below_min_unwind"
                    print(
                        f"[TRADER][INCIDENT] BID_ASK_HEDGE_BELOW_MIN_UNWIND label={opp.label} "
                        f"pred_filled={_ba_hedge_qty:.4f} cost=${_ba_hedge_cost_usd:.2f} "
                        f"unwind_filled={_uw_filled} unwind_qty={_uw_qty:.4f}"
                    )
                    _uw_status = "✅ продано" if _uw_filled else "❌ не продано — ручная проверка!"
                    notify(
                        f"🟡🟡🟡 <b>INCIDENT: HEDGE BELOW MIN → UNWIND</b>\n"
                        f"\n"
                        f"<b>{opp.label}</b>\n"
                        f"\n"
                        f"Predict заполнил: <b>{_ba_hedge_qty:.2f} shares</b> (${_ba_hedge_cost_usd:.2f})\n"
                        f"Слишком мало для Poly (мин ${_poly_min_hedge:.2f})\n"
                        f"\n"
                        f"Продажа обратно на Predict по {_unwind_price:.2f}: {_uw_status}\n"
                        + (f"Убыток: ~${_uw_loss:.3f}\n" if _uw_filled else "")
                        + (f"Ошибка: {_unwind_err}\n" if _unwind_err else "")
                    )
                    _append_jsonl(trades_file, row)
                    return {"status": "incident", "reason": "bid_ask_hedge_below_min_unwind"}

            _ba_hedge_price = _ba_hedge_vwap
            # Limit price: 2% above VWAP, rounded UP to Poly's 0.001 tick to guarantee fill
            import math as _math
            _ba_limit_price = min(0.99, _math.ceil(_ba_hedge_vwap * 1.02 * 1000) / 1000)
            _ba_hedge_leg = OpportunityLeg(
                **{**poly_leg.model_dump(), "shares": _ba_final_hedge_qty, "stake_usd": _ba_final_hedge_qty * _ba_hedge_price}
            )
            polymarket_result_ba: dict[str, Any] | None = None
            poly_exec_error_ba: Exception | None = None
            _poly_timing_ba["submit_ts"] = time.time()
            _poly_pk = _normalize_hex_key(os.environ.get("POLY_PRIVATE_KEY", ""))
            _poly_funder = os.environ.get("POLY_FUNDER", "").strip()
            _poly_sig_type = int(os.environ.get("POLY_SIGNATURE_TYPE", "0").strip() or "0")
            _poly_api_key = os.environ.get("POLY_API_KEY", "").strip()
            _poly_secret = os.environ.get("POLY_SECRET", "").strip()
            _poly_passphrase = os.environ.get("POLY_PASSPHRASE", "").strip()
            # Retry poly hedge on network errors — predict is already filled so hedge is critical
            _POLY_HEDGE_RETRIES = 3
            for _poly_attempt in range(_POLY_HEDGE_RETRIES):
                try:
                    polymarket_result_ba = _place_polymarket_limit_buy_exact_shares(
                        str(poly_leg.token_id),
                        shares=_ba_final_hedge_qty,
                        price=_ba_limit_price,
                        private_key=_poly_pk,
                        funder=_poly_funder,
                        signature_type=_poly_sig_type,
                        poly_api_key=_poly_api_key,
                        poly_secret=_poly_secret,
                        poly_passphrase=_poly_passphrase,
                        fak_fallback=True,
                    )
                    _poly_timing_ba["ack_ts"] = time.time()
                    poly_exec_error_ba = None
                    break
                except Exception as _e_ba_poly:
                    poly_exec_error_ba = _e_ba_poly
                    _err_s = str(_e_ba_poly)
                    # Only retry on network/connection errors, not logical API rejections
                    _is_network_err = (
                        "Request exception" in _err_s
                        or "ConnectionError" in _err_s
                        or "Timeout" in _err_s
                        or "status_code=None" in _err_s
                    )
                    if _is_network_err and _poly_attempt < _POLY_HEDGE_RETRIES - 1:
                        print(
                            f"[TRADER][POLY][RETRY] attempt={_poly_attempt + 1}/{_POLY_HEDGE_RETRIES} "
                            f"err={_err_s[:80]}"
                        )
                        time.sleep(0.5)
                        continue
                    _poly_timing_ba["fail_ts"] = time.time()
                    break
            _poly_timing_ba["end"] = time.time()

            row["polymarket"] = polymarket_result_ba or {}
            row["timing"]["poly_start"] = _poly_timing_ba.get("submit_ts")
            row["timing"]["poly_end"] = _poly_timing_ba.get("end")
            row["timing"]["poly_ack_ts"] = _poly_timing_ba.get("ack_ts")
            row["timing"]["t_end"] = time.time()
            row["timing"]["total_ms"] = (row["timing"]["t_end"] - t0) * 1000.0
            if _poly_timing_ba.get("submit_ts") and _poly_timing_ba.get("end"):
                row["timing"]["poly_ms"] = (_poly_timing_ba["end"] - _poly_timing_ba["submit_ts"]) * 1000.0

            # predict_fill_to_poly_submit_ms: gap from first predict fill detection to poly submit
            _ba_first_fill_ts = _ba_quote_meta.get("first_fill_ts")
            if _ba_first_fill_ts and _poly_timing_ba.get("submit_ts"):
                row["timing"]["predict_fill_to_poly_submit_ms"] = (
                    _poly_timing_ba["submit_ts"] - _ba_first_fill_ts
                ) * 1000.0
            # poly_submit_to_fill_ms
            if _poly_timing_ba.get("submit_ts") and _poly_timing_ba.get("ack_ts"):
                row["timing"]["poly_submit_to_fill_ms"] = (
                    _poly_timing_ba["ack_ts"] - _poly_timing_ba["submit_ts"]
                ) * 1000.0
            # unhedged_ms = gap from predict fill ACK to poly ACK (total exposure time)
            if _ba_first_fill_ts and _poly_timing_ba.get("ack_ts"):
                row["timing"]["unhedged_ms"] = (
                    _poly_timing_ba["ack_ts"] - _ba_first_fill_ts
                ) * 1000.0
            elif _pred_timing_ba.get("ack_ts") and _poly_timing_ba.get("submit_ts"):
                row["timing"]["unhedged_ms"] = (
                    _poly_timing_ba["submit_ts"] - _pred_timing_ba["ack_ts"]
                ) * 1000.0

            _ba_poly_resp = (polymarket_result_ba.get("response") or {}) if polymarket_result_ba else {}
            _ba_poly_txhashes = _ba_poly_resp.get("transactionsHashes") or []
            poly_filled_ba = _ba_poly_resp.get("success") is True and bool(_ba_poly_txhashes)
            _ba_poly_qty: float = 0.0
            try:
                _ba_poly_qty = float(_ba_poly_resp.get("takingAmount") or 0)
            except (ValueError, TypeError):
                pass
            # Limit orders credit exactly `size` shares — no LP-fee deduction from shares.
            # takingAmount IS the actual credited amount for limit orders.
            _ba_poly_qty_actual = _ba_poly_qty
            _ba_residual = abs(_ba_hedge_qty - _ba_poly_qty_actual)

            row["fill_analysis"] = {
                "unhedged_ms": row["timing"].get("unhedged_ms"),
                "predict_fill_to_poly_submit_ms": row["timing"].get("predict_fill_to_poly_submit_ms"),
                "poly_submit_to_fill_ms": row["timing"].get("poly_submit_to_fill_ms"),
                "first_fill_venue": "predict",
                "first_fill_qty": round(_ba_hedge_qty, 6),
                "residual_unhedged_qty": round(_ba_residual, 6),
                "pred_filled_qty": round(_ba_hedge_qty, 6),
                "poly_filled_qty": round(_ba_poly_qty_actual, 6),
                "poly_filled_qty_gross": round(_ba_poly_qty, 6),
                "requested_qty": round(float(opp.shares), 6),
                "partial_fill_count": len(_ba_partial_fills),
                "replace_count": _ba_quote_meta.get("replace_count"),
                "predict_time_to_first_fill_ms": _ba_quote_meta.get("time_to_first_fill_ms"),
            }
            row["parallel_result"] = {
                "pred_filled": True,
                "poly_filled": poly_filled_ba,
                "pred_error": None,
                "poly_error": str(poly_exec_error_ba) if poly_exec_error_ba else None,
            }
            print(
                f"[TRADER] bid_ask_poly_done success={poly_filled_ba} "
                f"status={_ba_poly_resp.get('status')} making={_ba_poly_resp.get('makingAmount')} "
                f"taking={_ba_poly_resp.get('takingAmount')} taking_actual={_ba_poly_qty_actual:.4f} "
                f"txhashes={len(_ba_poly_txhashes)} "
                f"pred_filled={_ba_hedge_qty:.4f} residual={_ba_residual:.4f}"
            )

            if poly_filled_ba:
                row["ok"] = True
                row["summary"]["status"] = "ok"
                row["summary"]["reason_code"] = "ok"
                print(
                    f"[TRADER][OK] BID+ASK label={opp.label} shares={opp.shares:.4f} "
                    f"unhedged_ms={row['timing'].get('unhedged_ms', 'n/a')} "
                    f"pred_qty={_ba_hedge_qty:.4f} poly_qty={_ba_poly_qty:.4f} "
                    f"ttff_ms={_ba_quote_meta.get('time_to_first_fill_ms', 'n/a')} "
                    f"replaces={_ba_quote_meta.get('replace_count')}"
                )
                _ba_unhedged_sec = (row["timing"].get("unhedged_ms") or 0) / 1000
                _ba_total_sec = (row["timing"].get("total_ms") or 0) / 1000
                _ba_pred_price = _ba_actual_pred_bid
                _ba_poly_price = _live_vwap_ba if _live_vwap_ba else float(poly_leg.ask)
                # Override with actual fill price from Polymarket response (makingAmount/takingAmount)
                # makingAmount = USDC spent, takingAmount = shares received → actual avg price
                try:
                    _ba_poly_making = float(_ba_poly_resp.get("makingAmount") or 0)
                    _ba_poly_taking = float(_ba_poly_resp.get("takingAmount") or 0)
                    if _ba_poly_making > 0 and _ba_poly_taking > 0:
                        _ba_poly_price = _ba_poly_making / _ba_poly_taking
                except Exception:
                    pass
                _ba_poly_fee_paid = _ba_fee_rate * _ba_poly_price * (1.0 - _ba_poly_price) * _ba_hedge_qty
                _ba_pred_fee_paid = _ba_pred_fee_bps / 10_000 * _ba_pred_price * _ba_hedge_qty
                _ba_gross = _ba_hedge_qty * (1.0 - _ba_pred_price - _ba_poly_price)
                _ba_net_pnl = _ba_gross - _ba_poly_fee_paid - _ba_pred_fee_paid
                _trade_pnl_log.append((time.time(), _ba_net_pnl))
                # Store authoritative net_pnl so downstream code doesn't need to recalculate
                row["net_pnl"] = round(_ba_net_pnl, 6)
                # Update live_hedge_recheck with actual executed prices so stored PnL is accurate
                _actual_poly_fee = _ba_fee_rate * _ba_poly_price * (1.0 - _ba_poly_price)
                _actual_net_edge = (_ba_net_pnl / _ba_hedge_qty) if _ba_hedge_qty > 0 else 0.0
                if "live_hedge_recheck" in row:
                    row["live_hedge_recheck"]["live_poly_vwap"] = round(_ba_poly_price, 6)
                    row["live_hedge_recheck"]["live_poly_fee"] = round(_actual_poly_fee, 6)
                    # Recalculate net_edge from actual execution prices (overrides pre-execution estimate)
                    row["live_hedge_recheck"]["live_net_edge"] = round(_actual_net_edge, 6)
                    row["live_hedge_recheck"]["live_net_edge_bps"] = round(_actual_net_edge * 10_000, 1)
                else:
                    row["live_hedge_recheck"] = {
                        "pred_bid": round(_ba_pred_price, 6),
                        "live_poly_vwap": round(_ba_poly_price, 6),
                        "live_poly_fee": round(_actual_poly_fee, 6),
                        "live_net_edge": round(_actual_net_edge, 6),
                        "live_net_edge_bps": round(_actual_net_edge * 10_000, 1),
                        "hedge_qty": round(_ba_hedge_qty, 4),
                        "poly_fee_rate": _ba_fee_rate,
                    }

                # Log mismatch for diagnostics (no correction — extra Predict shares are a bonus,
                # not a risk: both legs pay out on resolution, just slightly asymmetric amounts).
                _ba_mismatch_shares = _ba_net_sell_qty - _ba_poly_qty_actual
                row["mismatch_correction"] = {
                    "pred_net_qty": round(_ba_net_sell_qty, 6),
                    "poly_filled_qty_actual": round(_ba_poly_qty_actual, 6),
                    "poly_filled_qty_gross": round(_ba_poly_qty, 6),
                    "mismatch_shares": round(_ba_mismatch_shares, 6),
                    "corrected": False,
                }

                _append_jsonl(trades_file, row)
                _append_jsonl(success_trades_file, row)

                _tkey = str(poly_leg.token_id)
                _prev = _ba_fill_state.get(_tkey)
                _GROUP_TTL = 1800  # 30 min window to group fills for same market
                _is_grouped = _prev is not None and (time.time() - _prev[3]) < _GROUP_TTL
                _reply_to_id = _prev[0] if _is_grouped else None
                _cum_pnl = (_prev[1] + _ba_net_pnl) if _is_grouped else _ba_net_pnl
                _fill_n = (_prev[2] + 1) if _is_grouped else 1

                # ROI relative to total stake
                _total_stake = _ba_poly_qty * _ba_poly_price + _ba_hedge_qty * _ba_pred_price
                _roi_pct = (_ba_net_pnl / _total_stake * 100) if _total_stake > 0 else 0.0

                # Market title from legs
                _mkt_title = ""
                for _leg in (row.get("legs") or []):
                    if _leg.get("title"):
                        _mkt_title = _leg["title"]
                        break

                # Fetch current total position from Polymarket (best-effort, non-blocking)
                _poly_pos_line = ""
                try:
                    _poly_pos = _fetch_poly_position(str(poly_leg.token_id), timeout=2.5)
                    if _poly_pos is not None:
                        _pos_shares, _pos_avg = _poly_pos
                        if _pos_shares > 0:
                            _poly_pos_line = (
                                f"<i>💼 Poly total position: {_pos_shares:.2f} shares"
                                f" @ avg {_pos_avg:.2f}</i>\n"
                            )
                except Exception:
                    pass

                _is_tyanuchka = (_ba_pred_price + _ba_poly_price) < 1.0
                if _ba_net_pnl >= 0:
                    _pnl_suffix = " — TYANUCHKA IS CANCELED"
                else:
                    _pnl_suffix = " — GG PROEBALI"
                _pnl_emoji = "📈" if _ba_net_pnl >= 0 else "📉"
                _cum_line = f"<i>total ×{_fill_n}: {_cum_pnl:+.2f}$</i>\n" if _fill_n > 1 else ""
                _title = f"🟢🟢🟢 <b>HEDGE FILLED ×{_fill_n}</b>" if _fill_n > 1 else "🟢🟢🟢 <b>HEDGE FILLED</b>"
                _h1_pnl, _h1_n = _pnl_last_hour()

                _msg_id = notify(
                    f"{_title}\n"
                    + (f"<i>{_mkt_title}</i>\n" if _mkt_title else "")
                    + f"\n"
                    f"<b>{opp.label}</b>\n"
                    f"\n"
                    f"<b>Polymarket</b>  {poly_leg.side.upper()}\n"
                    f"  {_ba_poly_qty:.3f} shares  @  <code>{_ba_poly_price:.2f}</code>  =  <b>${_ba_poly_qty * _ba_poly_price:.2f}</b>\n"
                    f"<b>Predict</b>  {pred_leg.side.upper()}\n"
                    f"  {_ba_hedge_qty:.3f} shares  @  <code>{_ba_pred_price:.2f}</code>  =  <b>${_ba_hedge_qty * _ba_pred_price:.2f}</b>\n"
                    + _poly_pos_line
                    + f"\n"
                    f"{_pnl_emoji} <b>{_ba_net_pnl:+.2f}$</b>  ({_roi_pct:+.2f}%){_pnl_suffix}\n"
                    + _cum_line
                    + f"\n"
                    f"<i>⏱ fill={_ba_quote_meta.get('time_to_first_fill_ms', 0)/1000:.1f}s  unhedged={_ba_unhedged_sec:.1f}s  total={_ba_total_sec:.1f}s</i>\n",
                    reply_to_message_id=_reply_to_id,
                )
                # Store state: use original msg_id for the whole group so all replies chain to first
                _stored_id = (_prev[0] if _is_grouped else _msg_id) if _msg_id is not None else (_reply_to_id or 0)
                if _stored_id:
                    _ba_fill_state[_tkey] = (_stored_id, _cum_pnl, _fill_n, time.time())
                    _save_ba_fill_state()
                return {"status": "ok"}
            else:
                # Predict filled, poly failed → try to unwind on Predict before declaring incident
                _unwind_tick = float(os.environ.get("PREDICT_TICK_SIZE", "0.01") or "0.01")
                _unwind_sell_price = max(_unwind_tick, _ba_actual_pred_bid - _unwind_tick)
                _uw2_result: dict[str, Any] = {}
                _uw2_err: str | None = None
                # Predict API can lag balance indexing after a fill — retry unwind up to 3x
                # with increasing delays so the balance has time to appear.
                _UW2_RETRIES = 3
                _UW2_DELAYS = [2.0, 5.0, 10.0]
                for _uw2_attempt in range(_UW2_RETRIES):
                    if _uw2_attempt > 0:
                        time.sleep(_UW2_DELAYS[_uw2_attempt - 1])
                    _uw2_result = {}
                    _uw2_err = None
                    try:
                        _uw2_result = _place_predict_limit_sell(
                            pred_leg,
                            sell_qty=_ba_net_sell_qty,
                            sell_price=_unwind_sell_price,
                            fill_timeout_sec=30.0,
                            trace_id=trace_id,
                        )
                        if _uw2_result.get("filled"):
                            break
                    except Exception as _uw2_e:
                        _uw2_err = str(_uw2_e)
                        print(
                            f"[TRADER][UNWIND_ERROR] unhedged_predict unwind "
                            f"attempt={_uw2_attempt+1}/{_UW2_RETRIES} err={_uw2_e}"
                        )
                        _is_balance_lag = "400" in _uw2_err or "insufficient" in _uw2_err.lower()
                        if not _is_balance_lag or _uw2_attempt == _UW2_RETRIES - 1:
                            break

                _uw2_filled = _uw2_result.get("filled", False)
                _uw2_qty = _uw2_result.get("filled_qty", 0.0)

                _ba_inc2 = {
                    "ts": datetime.utcnow().isoformat() + "Z",
                    "type": "bid_ask_unhedged_predict",
                    "label": opp.label,
                    "fill_analysis": row["fill_analysis"],
                    "book_freshness": _book_freshness,
                    "unhedged_leg": "predict",
                    "unhedged_side": pred_leg.side,
                    "unhedged_qty": float(_ba_hedge_qty),
                    "unhedged_stake_usd": float(pred_leg.stake_usd),
                    "poly_error": str(poly_exec_error_ba) if poly_exec_error_ba else None,
                    "unwind": _uw2_result,
                    "unwind_error": _uw2_err,
                    "quote_meta": _ba_quote_meta,
                }
                _append_jsonl(incidents_file, _ba_inc2)
                row["ok"] = False
                row["summary"]["status"] = "incident"
                row["summary"]["reason_code"] = "bid_ask_unhedged_predict"
                print(
                    f"[TRADER][INCIDENT] BID_ASK_UNHEDGED_PREDICT label={opp.label} "
                    f"pred_qty={_ba_hedge_qty:.6f} poly_err={poly_exec_error_ba} "
                    f"residual={_ba_residual:.6f} unwind_filled={_uw2_filled} unwind_qty={_uw2_qty:.4f}"
                )
                _uw2_status = f"✅ продано {_uw2_qty:.2f} шарес по {_unwind_sell_price:.2f}" if _uw2_filled else f"❌ не удалось — ручная проверка!{(' err: ' + _uw2_err[:200]) if _uw2_err else ''}"
                notify(
                    f"🔴🔴🔴 <b>INCIDENT: UNHEDGED PREDICT</b>\n"
                    f"\n"
                    f"<b>{opp.label}</b>\n"
                    f"\n"
                    f"Predict ({pred_leg.side.upper()} BID)\n"
                    f"price: {_ba_actual_pred_bid:.2f} - stake: ${_ba_hedge_qty * _ba_actual_pred_bid:.2f} - shares: {_ba_hedge_qty:.2f}\n"
                    f"Polymarket ({poly_leg.side.upper()} ASK) ❌\n"
                    f"price: {(_live_vwap_ba if _live_vwap_ba is not None else float(poly_leg.ask)):.2f} (est.) - err: {str(poly_exec_error_ba)[:200] if poly_exec_error_ba else 'unknown'}\n"
                    f"\n"
                    f"Unwind на Predict: {_uw2_status}\n"
                )
                _append_jsonl(trades_file, row)
                return {"status": "incident", "reason": "bid_ask_unhedged_predict"}



    except Exception as e:
        row["error"] = str(e)
        row["summary"]["status"] = "error"
        row["summary"]["reason_code"] = "exception"
        row["summary"]["reason"] = str(e)
        print(f"[TRADER][ERROR] label={opp.label} exception={e}")
        import traceback as _tb
        print(_tb.format_exc())
        _append_jsonl(trades_file, row)
        return {"status": "error", "error": str(e)}
    finally:
        if _market_id_int is not None:
            with _predict_market_in_flight_lock:
                _predict_market_in_flight.discard(_market_id_int)
            _remove_inflight_order(_market_id_int)
