from __future__ import annotations

from datetime import datetime, timezone
import itertools
import json
import math
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
from py_clob_client.order_builder.constants import BUY, SELL as POLY_SELL

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

    # Pre-populate _predict_market_in_flight SYNCHRONOUSLY from disk so new trade requests
    # are blocked immediately — before the background inflight-check thread runs.
    if _INFLIGHT_ORDERS_FILE.exists():
        try:
            _inflight_data = json.loads(_INFLIGHT_ORDERS_FILE.read_text())
            _cutoff = time.time() - 600
            _preload_ids = [
                int(k)
                for k, v in _inflight_data.items()
                if isinstance(v, dict) and float(v.get("ts", 0)) > _cutoff
            ]
            if _preload_ids:
                with _predict_market_in_flight_lock:
                    _predict_market_in_flight.update(_preload_ids)
                print(f"[TRADER] inflight markets pre-blocked={_preload_ids}")
        except Exception as _pre_e:
            print(f"[TRADER] inflight preload error={_pre_e}")

    # Check for Predict orders that were in-flight when the container last stopped.
    # Runs in a background thread so it doesn't block server startup.
    threading.Thread(target=_check_inflight_on_startup, daemon=True, name="inflight_startup_check").start()

    # Background late-fill watcher: detects ghost fills that arrive after the 60s
    # ghost_fill_watch window has expired (real BSC confirmation lag edge case).
    threading.Thread(target=_late_fill_watcher, daemon=True, name="late_fill_watcher").start()

    # BSC WebSocket newHeads — wakes Predict poll / ghost_fill_watch on each block (~3s)
    _start_bsc_ws_thread()
    # Polygon newHeads — same idea for Polymarket CTF / reconcile vs REST indexer lag
    _start_polygon_ws_thread()

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
        # Debounce: require N consecutive failures before DOWN, N consecutive successes before RESTORED.
        # Prevents SSL handshake timeouts (transient VPN hiccups) from triggering flapping alerts.
        _FAIL_THRESHOLD = int(os.environ.get("API_DOWN_FAIL_THRESHOLD", "3") or "3")
        _OK_THRESHOLD = int(os.environ.get("API_UP_OK_THRESHOLD", "2") or "2")
        _fail_streak = 0
        _ok_streak = 0
        # Inner retry: on first failure, wait this long then retry once before counting as failed.
        # Handles transient SSL handshake drops without bumping the fail_streak.
        _INNER_RETRY_DELAY = float(os.environ.get("API_HEALTH_RETRY_DELAY_SEC", "4") or "4")
        _INNER_RETRIES = int(os.environ.get("API_HEALTH_INNER_RETRIES", "2") or "2")

        def _check_url(url: str, kw: dict) -> tuple[bool, str]:
            """Try up to _INNER_RETRIES times; return (ok, last_err)."""
            _last_err = ""
            for _attempt in range(_INNER_RETRIES):
                if _attempt > 0:
                    time.sleep(_INNER_RETRY_DELAY)
                try:
                    _resp = httpx.get(url, timeout=httpx.Timeout(8.0, connect=4.0), **kw)
                    if _resp.status_code < 500:
                        return True, ""
                    _last_err = f"status {_resp.status_code}"
                except Exception as _ex:
                    _last_err = str(_ex)
            return False, _last_err

        while True:
            time.sleep(_api_check_interval)
            _predict_ok = False
            _poly_ok = False
            _reason = ""
            _predict_ok, _predict_err = _check_url(_PREDICT_HEALTH_URL, {})
            if not _predict_ok:
                _reason = f"predict: {_predict_err}"
                print(f"[TRADER][API_WATCHDOG] predict_health_failed err={_predict_err}")
            _kw: dict = {}
            if _proxy_url:
                _kw["proxy"] = _proxy_url
            _poly_ok, _poly_err = _check_url(_POLY_HEALTH_URL, _kw)
            if not _poly_ok:
                if not _reason:
                    _reason = f"poly: {_poly_err}"
                print(f"[TRADER][API_WATCHDOG] poly_health_failed err={_poly_err}")

            _all_ok = _predict_ok and _poly_ok
            if not _all_ok:
                _fail_streak += 1
                _ok_streak = 0
                print(
                    f"[TRADER][API_WATCHDOG] fail_streak={_fail_streak}/{_FAIL_THRESHOLD} "
                    f"predict_ok={_predict_ok} poly_ok={_poly_ok}"
                )
            else:
                _ok_streak += 1
                _fail_streak = 0
                if _was_down:
                    print(f"[TRADER][API_WATCHDOG] ok_streak={_ok_streak}/{_OK_THRESHOLD}")

            if not _all_ok and _fail_streak >= _FAIL_THRESHOLD and not _was_down:
                _was_down = True
                _ok_streak = 0
                _down_reason = _reason or f"predict_ok={_predict_ok} poly_ok={_poly_ok}"
                _halt_api_path.write_text(_down_reason)
                print(
                    f"[TRADER][API_WATCHDOG] API DOWN after {_fail_streak} consecutive failures"
                    f" — halt_api created reason={_down_reason}"
                )
                notify("🔴 <b>API DOWN</b>\n")
            elif _all_ok and _was_down and _ok_streak >= _OK_THRESHOLD:
                _was_down = False
                _fail_streak = 0
                _halt_api_path.unlink(missing_ok=True)
                print(f"[TRADER][API_WATCHDOG] API RESTORED after {_ok_streak} consecutive OK — halt_api removed")
                notify("🟢 <b>API RESTORED</b>\n")

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


def _predict_maker_taker_wei_from_get_payload(payload: dict[str, Any] | None) -> tuple[int, int] | None:
    """Read maker/taker (wei) from GET/POST /v1/orders `data` payload (on-chain order sizes)."""
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    order = data.get("order")
    if not isinstance(order, dict):
        return None
    try:
        mk = int(str(order.get("makerAmount") or "0"))
        tk = int(str(order.get("takerAmount") or "0"))
    except (ValueError, TypeError):
        return None
    if mk <= 0 or tk <= 0:
        return None
    return (mk, tk)


def _bsc_get_order_status(
    order_hash_hex: str,
) -> tuple[bool, int, str] | None:
    """eth_call CTF/NegRisk getOrderStatus — returns (isFilledOrCancelled, remaining, contract_tag) or None.

    `remaining` is the unfilled *maker* amount (CTF Exchange v3); see _bsc_taker_filled_shares.
    Tries *both* exchanges on the first RPC: a hash is valid on only one; the other is (false,0).
    Partial or full must not be lost by stopping after the first empty-looking decode.
    """
    global _ORDER_STATUS_SELECTOR
    try:
        if _ORDER_STATUS_SELECTOR is None:
            from eth_utils import keccak as eth_keccak

            _ORDER_STATUS_SELECTOR = "0x" + eth_keccak(text="getOrderStatus(bytes32)")[:4].hex()

        if not order_hash_hex.startswith("0x"):
            order_hash_hex = "0x" + order_hash_hex
        call_data = _ORDER_STATUS_SELECTOR + order_hash_hex[2:].zfill(64)

        def _decode_st(_rpc: str, to_addr: str) -> tuple[bool, int] | None:
            try:
                resp = requests.post(
                    _rpc,
                    json={
                        "jsonrpc": "2.0",
                        "method": "eth_call",
                        "params": [{"to": to_addr, "data": call_data}, "latest"],
                        "id": 1,
                    },
                    timeout=5,
                )
                result = resp.json()
                if "error" in result:
                    return None
                raw = result.get("result", "")
                if len(raw) < 2 + 128:
                    return None
                raw_bytes = bytes.fromhex(raw[2:])
                is_fc = bool(int.from_bytes(raw_bytes[0:32], "big"))
                rem = int.from_bytes(raw_bytes[32:64], "big")
                return (is_fc, rem)
            except Exception:
                return None

        for _rpc in _BSC_RPCS:
            ctf = _decode_st(_rpc, _BSC_CTF_EXCHANGE)
            neg = _decode_st(_rpc, _BSC_NEG_RISK_CTF_EXCHANGE)
            # Definitive full fill on either contract
            for st, cname in ((ctf, "CTF"), (neg, "NEG_RISK")):
                if st and st[0] and st[1] == 0:
                    return (st[0], st[1], cname)
            # In-flight or partial / on-chain cancel with maker remainder
            for st, cname in ((ctf, "CTF"), (neg, "NEG_RISK")):
                if st and st[1] > 0:
                    return (st[0], st[1], cname)
            # Only (false,0) “empty / unknown on this book”
            if ctf and ctf == (False, 0) and neg and neg == (False, 0):
                return (False, 0, "CTF")
            if ctf:
                return (ctf[0], ctf[1], "CTF")
            if neg:
                return (neg[0], neg[1], "NEG_RISK")
            # Both ctf and neg are None — this RPC failed entirely; try the next one.
            continue
    except Exception as _e:
        print(f"[TRADER][BSC_CHECK] err={_e}")
    return None


def _bsc_taker_filled_shares(
    is_fc: bool,
    remaining: int,
    maker_amt: int | None,
    taker_amt: int | None,
    leg_shares: float,
) -> float:
    """Convert BSC getOrderStatus + on-chain order sizes to outcome shares filled (BUY taker side).

    CTF: remaining is *maker* not yet filled. Filled taker (wei) = (maker_amt - remaining) * taker_amt // maker_amt
    (same as CalculatorHelper.calculateTakingAmount for the cumulative fill).

    Without maker/taker, only a *full* fill can be represented (is_fc and remaining==0) — same as the old
    _bsc_check_order_filled; partials need maker/taker from API or late_watch.
    """
    if not is_fc and remaining == 0:
        return 0.0
    if (
        maker_amt is not None
        and taker_amt is not None
        and maker_amt > 0
        and taker_amt > 0
    ):
        filled_maker = maker_amt - remaining
        if filled_maker <= 0:
            return 0.0
        if filled_maker > maker_amt:
            filled_maker = maker_amt
        taker_filled_wei = (filled_maker * taker_amt) // maker_amt
        out = taker_filled_wei / 10**18
        if leg_shares and leg_shares > 0:
            return min(float(leg_shares), out)
        return out
    if is_fc and remaining == 0:
        return max(0.0, float(leg_shares))
    return 0.0

# ── BSC WebSocket (newHeads) — wake poll loops + optional early getOrderStatus ──
# Public HTTP RPCs often disable eth_getLogs; we still use eth_call for fills.
# WS gives a proactive ~block-time signal instead of relying only on late_fill + fixed sleeps.
_bsc_head_cv = _threading.Condition()
_bsc_head_gen: int = 0
_bsc_ws_stop = _threading.Event()
_bsc_ws_thread: _threading.Thread | None = None


def _bsc_head_gen_snapshot() -> int:
    with _bsc_head_cv:
        return _bsc_head_gen


def _bsc_signal_new_head() -> None:
    global _bsc_head_gen
    with _bsc_head_cv:
        _bsc_head_gen += 1
        _bsc_head_cv.notify_all()


def _bsc_wait_new_head_or_timeout(prev_gen: int, timeout: float) -> int:
    """Block up to timeout, or return sooner when a BSC newHead arrives (if WS active)."""
    if timeout <= 0:
        return _bsc_head_gen_snapshot()
    with _bsc_head_cv:
        end = time.monotonic() + timeout
        while _bsc_head_gen == prev_gen:
            remaining = end - time.monotonic()
            if remaining <= 0:
                break
            _bsc_head_cv.wait(timeout=remaining)
        return _bsc_head_gen


def _bsc_ws_urls_resolved() -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for u in (os.environ.get("BSC_WS_URL", "").strip(),):
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    for part in os.environ.get("BSC_WS_URLS", "").split(","):
        p = part.strip()
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    if out:
        return out
    # Public HTTP seeds (_BSC_RPCS) usually do not serve eth_subscribe on wss://same-host → 404.
    # Try dedicated WSS endpoints first; keep https→wss as last resort.
    for w in (
        "wss://bsc.publicnode.com",
        "wss://bsc-rpc.publicnode.com",
        "wss://bsc.drpc.org",
    ):
        if w not in seen:
            seen.add(w)
            out.append(w)
    for h in _BSC_RPCS:
        h = (h or "").strip()
        if h.startswith("https://"):
            cand = "wss://" + h[8:].rstrip("/")
            if cand not in seen:
                seen.add(cand)
                out.append(cand)
        elif h.startswith("http://"):
            cand = "ws://" + h[7:].rstrip("/")
            if cand not in seen:
                seen.add(cand)
                out.append(cand)
    return out


def _bsc_ws_should_run() -> bool:
    if os.environ.get("BSC_WS_ENABLE", "1").strip().lower() in ("0", "false", "no", "off"):
        return False
    try:
        import websocket  # noqa: F401
    except ImportError:
        print("[TRADER][BSC_WS] websocket-client not installed — pip install websocket-client")
        return False
    return True


def _bsc_ws_newheads_loop() -> None:
    import json

    try:
        import websocket
    except ImportError:
        return
    backoff = 1.0
    while not _bsc_ws_stop.is_set():
        urls = _bsc_ws_urls_resolved()
        if not urls:
            time.sleep(30.0)
            continue
        connected = False
        for wurl in urls:
            if _bsc_ws_stop.is_set():
                break
            ws = None
            try:
                ws = websocket.create_connection(wurl, timeout=20, enable_multithread=True)
                ws.settimeout(120)
                ws.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_subscribe",
                            "params": ["newHeads"],
                        }
                    )
                )
                sub_raw = ws.recv()
                sub_msg = json.loads(sub_raw)
                if sub_msg.get("error") or not sub_msg.get("result"):
                    raise RuntimeError(sub_msg.get("error") or "no subscription id")
                print(f"[TRADER][BSC_WS] subscribed newHeads url={wurl[:56]}...")
                backoff = 1.0
                connected = True
                while not _bsc_ws_stop.is_set():
                    try:
                        raw = ws.recv()
                    except Exception:
                        break
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    if msg.get("method") == "eth_subscription":
                        _bsc_signal_new_head()
            except Exception as _wse:
                print(f"[TRADER][BSC_WS] session_error url={wurl[:40]}... err={_wse}")
            finally:
                if ws:
                    try:
                        ws.close()
                    except Exception:
                        pass
        if not connected:
            time.sleep(min(backoff, 45.0))
            backoff = min(backoff * 1.4, 60.0)


def _start_bsc_ws_thread() -> None:
    global _bsc_ws_thread
    if not _bsc_ws_should_run():
        print("[TRADER][BSC_WS] disabled or unavailable")
        return
    if _bsc_ws_thread is not None and _bsc_ws_thread.is_alive():
        return
    _bsc_ws_thread = _threading.Thread(
        target=_bsc_ws_newheads_loop,
        daemon=True,
        name="bsc_ws_newheads",
    )
    _bsc_ws_thread.start()
    print("[TRADER][BSC_WS] background thread started")


# ── Polygon (Polymarket CTF) — on-chain balance + WS newHeads (mirrors BSC+Predict pattern) ──
_POLY_CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_POLY_OUTCOME_SHARE_SCALE = 1_000_000.0  # CTF ERC1155 outcome amounts (same as CLOB takingAmount/1e6)
_POLY_CTF_BALANCE_ABI = [
    {
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]
# Default public Polygon HTTP RPCs (tried in order; proxy via PROXY_URL when set on Provider)
_DEFAULT_POLYGON_RPCS = [
    "https://polygon-bor.publicnode.com",
    "https://polygon.drpc.org",
    "https://1rpc.io/matic",
    "https://polygon-rpc.com",
]
_poly_head_cv = _threading.Condition()
_poly_head_gen: int = 0
_poly_ws_stop = _threading.Event()
_poly_ws_thread: _threading.Thread | None = None


def _polygon_http_rpcs() -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for u in (os.environ.get("POLYGON_RPC_URL", "").strip(),):
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    for part in (os.environ.get("POLYGON_RPC_URLS", "") or "").split(","):
        p = part.strip()
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    for d in _DEFAULT_POLYGON_RPCS:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


def _get_web3_polygon() -> Any:
    from web3 import Web3

    proxy_url = os.environ.get("PROXY_URL", "").strip() or None
    req_kwargs: dict[str, Any] = {"timeout": 20}
    if proxy_url:
        req_kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}
    for url in _polygon_http_rpcs():
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs=req_kwargs))
            try:
                from web3.middleware import ExtraDataToPOAMiddleware
                w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            except ImportError:
                try:
                    from web3.middleware import geth_poa_middleware
                    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
                except ImportError:
                    pass
            if w3.is_connected():
                return w3
        except Exception:
            continue
    raise RuntimeError("polygon_rpc_not_connected")


def _poly_ctf_shares_onchain(funder: str, token_id: str) -> float | None:
    """CTF balanceOf(account, id) on Polygon; returns human shares, or None on failure."""
    funder = (funder or "").strip()
    if not funder or not str(token_id).strip():
        return None
    try:
        from web3 import Web3
    except ImportError:
        return None
    try:
        tid = int(str(token_id).strip())
    except Exception:
        return None
    try:
        w3 = _get_web3_polygon()
        ctf = w3.eth.contract(
            address=Web3.to_checksum_address(_POLY_CTF_ADDRESS),
            abi=_POLY_CTF_BALANCE_ABI,
        )
        raw = int(
            ctf.functions.balanceOf(Web3.to_checksum_address(funder), tid).call()
        )
        return float(raw) / _POLY_OUTCOME_SHARE_SCALE
    except Exception as _e:
        print(f"[TRADER][POLY][CTF] balance err token={str(token_id)[:20]}... err={_e}")
        return None


def _poly_head_gen_snapshot() -> int:
    with _poly_head_cv:
        return _poly_head_gen


def _poly_signal_new_head() -> None:
    global _poly_head_gen
    with _poly_head_cv:
        _poly_head_gen += 1
        _poly_head_cv.notify_all()


def _poly_wait_new_head_or_timeout(prev_gen: int, timeout: float) -> int:
    """Block up to `timeout` or until a Polygon newHead (eth_subscribe) fires."""
    if timeout <= 0:
        return _poly_head_gen_snapshot()
    with _poly_head_cv:
        end = time.monotonic() + timeout
        while _poly_head_gen == prev_gen:
            remaining = end - time.monotonic()
            if remaining <= 0:
                break
            _poly_head_cv.wait(timeout=remaining)
        return _poly_head_gen


def _polygon_ws_urls_resolved() -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for u in (os.environ.get("POLYGON_WS_URL", "").strip(),):
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    for part in (os.environ.get("POLYGON_WS_URLS", "") or "").split(","):
        p = part.strip()
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    if out:
        return out
    for w in (
        "wss://polygon-bor.publicnode.com",
        "wss://polygon.drpc.org",
    ):
        if w not in seen:
            seen.add(w)
            out.append(w)
    for h in _polygon_http_rpcs():
        h = (h or "").strip()
        if h.startswith("https://"):
            cand = "wss://" + h[8:].rstrip("/")
            if cand not in seen:
                seen.add(cand)
                out.append(cand)
        elif h.startswith("http://"):
            cand = "ws://" + h[7:].rstrip("/")
            if cand not in seen:
                seen.add(cand)
                out.append(cand)
    return out


def _polygon_ws_should_run() -> bool:
    if os.environ.get("POLYGON_WS_ENABLE", "1").strip().lower() in ("0", "false", "no", "off"):
        return False
    try:
        import websocket  # noqa: F401
    except ImportError:
        print(
            "[TRADER][POLYGON_WS] websocket-client not installed — pip install websocket-client"
        )
        return False
    return True


def _polygon_ws_newheads_loop() -> None:
    import json

    try:
        import websocket
    except ImportError:
        return
    backoff = 1.0
    while not _poly_ws_stop.is_set():
        urls = _polygon_ws_urls_resolved()
        if not urls:
            time.sleep(30.0)
            continue
        connected = False
        for wurl in urls:
            if _poly_ws_stop.is_set():
                break
            ws = None
            try:
                ws = websocket.create_connection(wurl, timeout=20, enable_multithread=True)
                ws.settimeout(120)
                ws.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_subscribe",
                            "params": ["newHeads"],
                        }
                    )
                )
                sub_raw = ws.recv()
                sub_msg = json.loads(sub_raw)
                if sub_msg.get("error") or not sub_msg.get("result"):
                    raise RuntimeError(sub_msg.get("error") or "no subscription id")
                print(f"[TRADER][POLYGON_WS] subscribed newHeads url={wurl[:56]}...")
                backoff = 1.0
                connected = True
                while not _poly_ws_stop.is_set():
                    try:
                        raw = ws.recv()
                    except Exception:
                        break
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    if msg.get("method") == "eth_subscription":
                        _poly_signal_new_head()
            except Exception as _wse:
                print(f"[TRADER][POLYGON_WS] session_error url={wurl[:40]}... err={_wse}")
            finally:
                if ws:
                    try:
                        ws.close()
                    except Exception:
                        pass
        if not connected:
            time.sleep(min(backoff, 45.0))
            backoff = min(backoff * 1.4, 60.0)


def _start_polygon_ws_thread() -> None:
    global _poly_ws_thread
    if not _polygon_ws_should_run():
        print("[TRADER][POLYGON_WS] disabled or unavailable")
        return
    if _poly_ws_thread is not None and _poly_ws_thread.is_alive():
        return
    _poly_ws_thread = _threading.Thread(
        target=_polygon_ws_newheads_loop,
        daemon=True,
        name="polygon_ws_newheads",
    )
    _poly_ws_thread.start()
    print("[TRADER][POLYGON_WS] background thread started")


def _poly_ba_reconcile_shares(
    token_id: str,
    funder: str,
) -> tuple[float, float | None, dict[str, float | None]]:
    """Polymarket hedge visibility: data-api (REST) + CTF on Polygon. Max = conservative estimate.

    CLOB may be wrong after GTC/cancel; chain CTF and indexer can lag differently — we take max.
    """
    funder = (funder or "").strip()
    d_sh = 0.0
    d_avg: float | None = None
    p = _fetch_poly_position(str(token_id), timeout=5.0)
    if p:
        d_sh = float(p[0])
        if len(p) > 1 and float(p[1] or 0) > 0:
            d_avg = float(p[1])
    ctf = _poly_ctf_shares_onchain(funder, str(token_id)) if funder else None
    ctf_f = 0.0 if ctf is None else ctf
    m = max(d_sh, ctf_f)
    meta: dict[str, float | None] = {
        "data_api": d_sh,
        "polygon_ctf": ctf,
    }
    return m, d_avg, meta
# State for grouping repeated HEDGE FILLED notifications in the same market
# key: poly token_id  value: (message_id, cumulative_pnl, fill_count, timestamp)
_BA_FILL_STATE_FILE = Path("/data/ba_fill_state.json")
_ba_fill_state: dict[str, tuple[int, float, int, float]] = {}

# ── Rolling positions-indexer latency store ───────────────────────────────────
# Tracks observed chain_fill → positions_visible lag (seconds) for the last N samples.
# Used to compute a dynamic expiry safety buffer: p99 + unwind_time_budget.
import collections as _collections
_POSITIONS_LAG_STORE: _collections.deque[float] = _collections.deque(maxlen=200)
_positions_lag_store_lock = _threading.Lock()


def _record_positions_lag(wait_sec: float) -> None:
    """Record one observed positions-indexer latency sample (only successful finds)."""
    if wait_sec > 0:
        with _positions_lag_store_lock:
            _POSITIONS_LAG_STORE.append(wait_sec)


def _positions_lag_p99() -> float | None:
    """Return p99 of observed positions-visible latency, or None if < 10 samples."""
    with _positions_lag_store_lock:
        data = list(_POSITIONS_LAG_STORE)
    if len(data) < 10:
        return None
    data.sort()
    idx = int(len(data) * 0.99)
    return data[min(idx, len(data) - 1)]


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
                f"- Market: <code>{mkt}</code>  Hash: <code>{oh}...</code>\n"
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

        # Release the pre-blocked market IDs so normal trading can resume
        _preblocked = {int(k) for k in fresh}
        with _predict_market_in_flight_lock:
            _predict_market_in_flight.difference_update(_preblocked)
        print(f"[TRADER][STARTUP] inflight markets unblocked={list(_preblocked)}")

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


def _late_watch_save(
    order_hash: str,
    market_id: int,
    token_id: str | None,
    shares: float,
    *,
    maker_amount: int | None = None,
    taker_amount: int | None = None,
) -> None:
    """Register a cancelled Predict order for background late-fill monitoring.

    maker_amount / taker_amount (wei) enable BSC partial-fill → taker hedge sizing
    (CTF stores remaining in *maker* units).
    """
    try:
        with _late_watch_file_lock:
            data: dict = {}
            if _LATE_WATCH_FILE.exists():
                try:
                    data = json.loads(_LATE_WATCH_FILE.read_text())
                except Exception:
                    data = {}
            row: dict[str, Any] = {
                "order_hash": order_hash,
                "market_id": market_id,
                "token_id": token_id,
                "shares": shares,
                "ts": time.time(),
            }
            if maker_amount is not None and maker_amount > 0:
                row["makerAmount"] = int(maker_amount)
            if taker_amount is not None and taker_amount > 0:
                row["takerAmount"] = int(taker_amount)
            data[order_hash] = row
            _LATE_WATCH_FILE.write_text(json.dumps(data))
    except Exception as _e:
        print(f"[TRADER] late_watch_save error={_e}")


def _late_watch_maker_taker_from_entry(entry: dict[str, Any]) -> tuple[int | None, int | None]:
    """Integer maker/taker from persisted late_watch row (may be missing on old files)."""
    try:
        mk = entry.get("makerAmount")
        tk = entry.get("takerAmount")
        if mk is None and tk is None:
            return (None, None)
        m = int(str(mk or "0"))
        t = int(str(tk or "0"))
        if m > 0 and t > 0:
            return (m, t)
    except (ValueError, TypeError):
        pass
    return (None, None)


def _incident_tg_emoji(self_resolved: bool) -> str:
    return "🟡🟡🟡" if self_resolved else "🔴🔴🔴"


def _auto_hedge_late_fill_succeeded(hedge_status: str) -> bool:
    """True if _auto_hedge_late_fill placed a hedge (status starts with 'ok:')."""
    return bool(hedge_status) and hedge_status.startswith("ok:")


def _late_ghost_fill_incident(
    incidents_file: str,
    *,
    detection: Literal["predict_api_lag", "bsc_onchain", "bsc_watcher_expiry"],
    order_hash: str,
    market_id: object,
    shares: float,
    hedge_st: str,
) -> None:
    """Log late ghost fill to incidents.jsonl and send one Telegram with full outcome."""
    _resolved = _auto_hedge_late_fill_succeeded(hedge_st)
    _em = _incident_tg_emoji(_resolved)
    _sub = "RESOLVED" if _resolved else "ACTION REQUIRED"
    _det = {
        "predict_api_lag": "Predict API reported a fill after cancel (late / indexer lag).",
        "bsc_onchain": "BSC: on-chain fill; Predict API had not yet indexed 0 (API vs chain lag).",
        "bsc_watcher_expiry": "After 30 min watcher still had API=0; fill confirmed on BSC (on-chain).",
    }[detection]
    _row: dict[str, Any] = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "type": "late_ghost_fill",
        "detection": detection,
        "order_hash": order_hash,
        "market_id": market_id,
        "shares": round(float(shares), 6),
        "poly_hedge_result": hedge_st,
        "self_resolved": _resolved,
    }
    _append_jsonl(incidents_file, _row)
    _outcome = (
        "<i>Outcome: emergency Polymarket hedge (FOK) succeeded — flat.</i>\n"
        if _resolved
        else "<i>Outcome: auto-hedge failed — check Predict + Polymarket manually.</i>\n"
    )
    notify(
        f"{_em} <b>INCIDENT: LATE GHOST FILL — {_sub}</b>\n"
        f"\n"
        f"<i>{_det}</i>\n"
        f"\n"
        f"Order: <code>{str(order_hash)[:20]}…</code>\n"
        f"Market: <code>{market_id}</code>\n"
        f"Shares: <b>{shares:.3f}</b>\n"
        f"\n"
        f"<b>Poly auto-hedge</b> <code>{hedge_st}</code>\n"
        f"\n"
        f"{_outcome}"
    )


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
    """Legacy: full fill only (no maker/taker) — same as _bsc_taker_filled_shares with mk/tk=None."""
    st = _bsc_get_order_status(order_hash_hex)
    if st is None:
        return 0.0
    is_fc, rem, cname = st
    sh = _bsc_taker_filled_shares(is_fc, rem, None, None, float(entry_shares))
    if sh > 0 and is_fc and rem == 0:
        print(f"[TRADER][BSC_CHECK] FILLED hash={order_hash_hex[:18]}... contract={cname}")
    return sh


def _bsc_filled_taker_shares_for_hedge(
    order_hash_hex: str,
    leg_shares: float,
    maker_amt: int | None,
    taker_amt: int | None,
    *,
    log: bool = True,
) -> float:
    """BSC ground-truth taker (outcome) shares: full or partial. maker/taker from GET /orders if available."""
    st = _bsc_get_order_status(order_hash_hex)
    if st is None:
        return 0.0
    is_fc, rem, cname = st
    sh = _bsc_taker_filled_shares(is_fc, rem, maker_amt, taker_amt, float(leg_shares))
    if sh > 0 and log:
        _p = "partial" if rem > 0 else "full"
        print(
            f"[TRADER][BSC_CHECK] {_p} taker_sh={sh:.6f} hash={order_hash_hex[:18]}... "
            f"contract={cname} is_fc={is_fc} rem_maker={rem}"
        )
    return sh


def _late_fill_watcher() -> None:
    """Background thread: checks cancelled Predict order hashes for late fills.

    When ghost_fill_watch (60s) times out without finding a fill, the order hash
    is saved to _LATE_WATCH_FILE. This thread keeps polling for up to 30 min.
    On late fill: emergency Poly FOK hedge, one INCIDENT: LATE GHOST FILL message
    (🟡 if hedge ok, 🔴 if not) + line in incidents.jsonl.
    """
    _POLL_INTERVAL = 15.0
    _MAX_WATCH_SEC = 1800  # 30 minutes — Predict API can lag BSC by several minutes
    _inc_path = os.environ.get("TRADER_INCIDENTS_FILE", "/data/incidents.jsonl")
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
                    #   - partial on-chain (remaining>0 in maker units) + maker/taker → size hedge
                    #   - clean cancel (never seen on-chain) → mild alert, position likely not open
                    _mk, _tk = _late_watch_resolve_maker_taker(session, oh, entry)
                    bsc_shares = _bsc_filled_taker_shares_for_hedge(
                        oh, float(entry.get("shares", 0)), _mk, _tk
                    )
                    if bsc_shares > 0:
                        _hedge_st = _auto_hedge_late_fill(entry.get("token_id"), bsc_shares, int(mkt_id) if str(mkt_id).isdigit() else 0)
                        print(
                            f"[TRADER][LATE_WATCH] BSC_FILL_CONFIRMED_ON_EXPIRY "
                            f"hash={oh[:14]}... market_id={mkt_id} "
                            f"bsc_shares={bsc_shares:.4f} age={age:.0f}s hedge={_hedge_st}"
                        )
                        _late_ghost_fill_incident(
                            _inc_path,
                            detection="bsc_watcher_expiry",
                            order_hash=oh,
                            market_id=mkt_id,
                            shares=bsc_shares,
                            hedge_st=_hedge_st,
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
                            f"[TRADER][LATE_WATCH] LATE_GHOST_FILL_API "
                            f"hash={oh[:14]}... market_id={mkt_id} "
                            f"shares={shares:.4f} age={age:.0f}s hedge={_hedge_st}"
                        )
                        _late_ghost_fill_incident(
                            _inc_path,
                            detection="predict_api_lag",
                            order_hash=oh,
                            market_id=mkt_id,
                            shares=shares,
                            hedge_st=_hedge_st,
                        )
                        to_remove.append(oh)
                    else:
                        # Predict API still shows 0 — check BSC directly as fallback.
                        # The API indexer can lag on-chain confirms by several minutes.
                        _mk2, _tk2 = _late_watch_resolve_maker_taker(session, oh, entry)
                        bsc_shares = _bsc_filled_taker_shares_for_hedge(
                            oh, float(entry.get("shares", 0)), _mk2, _tk2
                        )
                        if bsc_shares > 0:
                            _hedge_st = _auto_hedge_late_fill(entry.get("token_id"), bsc_shares, int(mkt_id) if str(mkt_id).isdigit() else 0)
                            print(
                                f"[TRADER][LATE_WATCH] BSC_FILL_API_STILL_0 "
                                f"hash={oh[:14]}... market_id={mkt_id} "
                                f"bsc_shares={bsc_shares:.4f} age={age:.0f}s hedge={_hedge_st}"
                            )
                            _late_ghost_fill_incident(
                                _inc_path,
                                detection="bsc_onchain",
                                order_hash=oh,
                                market_id=mkt_id,
                                shares=bsc_shares,
                                hedge_st=_hedge_st,
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


def _predict_orderbook(session: requests.Session | None, market_id: int) -> dict[str, Any]:
    if session is None:
        session = _predict_monitor.get()
    r = session.get(f"https://api.predict.fun/v1/markets/{market_id}/orderbook", timeout=5)
    r.raise_for_status()
    j = r.json()
    if not isinstance(j, dict) or not j.get("success"):
        raise RuntimeError(f"predict_get_orderbook_failed market_id={market_id}")
    data = j.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(f"predict_get_orderbook_bad_response market_id={market_id}")
    return data


def _predict_live_sell_price(
    session: requests.Session | None,
    market_id: int,
    side: str,
    fallback: float,
    tick: float = 0.01,
) -> float:
    """Return the best live bid price on Predict for the token we want to sell.

    For UP side: best bid from bids[] (highest bid = most willing buyer).
    For DOWN side: complement of the lowest ask from asks[] (DOWN bid = 1 - UP ask).

    Starting the unwind sell at the live bid means it matches immediately instead of
    sitting at a stale/floor price. Falls back to fallback-tick if book is empty or
    the API call fails.
    """
    try:
        ob = _predict_orderbook(session, market_id)
        bids_flat = ob.get("bids") or []
        asks_flat = ob.get("asks") or []
        live_bid: float | None = None
        if side == "up":
            best = max(bids_flat, key=lambda b: float(b[0])) if bids_flat else None
            live_bid = float(best[0]) if best else None
        else:
            # DOWN bid = 1 - lowest UP ask
            best_ask = min(asks_flat, key=lambda a: float(a[0])) if asks_flat else None
            live_bid = round(1.0 - float(best_ask[0]), 6) if best_ask else None
        if live_bid is not None and live_bid > 0:
            # Snap down to tick grid so the SDK doesn't reject the price
            return max(tick, round(int(live_bid / tick) * tick, 6))
    except Exception:
        pass
    return max(tick, round(int((fallback - tick) / tick) * tick, 6))


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


def _late_watch_resolve_maker_taker(
    session: requests.Session, order_hash: str, entry: dict[str, Any]
) -> tuple[int | None, int | None]:
    """On-chain BSC fill math needs maker/taker (wei). Prefer JSON row; else GET /v1/orders/{hash}."""
    mk, tk = _late_watch_maker_taker_from_entry(entry)
    if mk is not None and tk is not None:
        return (mk, tk)
    try:
        g = _predict_get_order_by_hash(session, order_hash)
        p = _predict_maker_taker_wei_from_get_payload(g)
        if p:
            return p
    except Exception:
        pass
    return (None, None)


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


def _predict_position_row_token_id(pos: dict[str, Any]) -> str | None:
    """Best-effort outcome token id from a GET /v1/positions row (schema may vary)."""
    o = pos.get("outcome") if isinstance(pos.get("outcome"), dict) else {}
    tok = pos.get("token")
    if isinstance(tok, dict):
        for key in ("tokenId", "token_id", "id", "address"):
            v = tok.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
    for src in (pos, o):
        for key in ("tokenId", "token_id", "erc1155TokenId", "id"):
            v = src.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
    return None


def _parse_predict_position_amount_shares(pos: dict[str, Any]) -> float:
    """Parse position `amount` to human shares (Predict uses wei in amount)."""
    raw = pos.get("amount")
    if raw is None:
        return 0.0
    try:
        if isinstance(raw, bool):
            return 0.0
        if isinstance(raw, int):
            return raw / 1e18
        if isinstance(raw, float):
            return int(raw) / 1e18
        s = str(raw).strip()
        if not s:
            return 0.0
        if "." in s or "e" in s.lower():
            return float(s)
        return int(s, 10) / 1e18
    except Exception:
        return 0.0


def _predict_max_shares_for_market(session: requests.Session | None, market_id: int) -> float:
    """Largest position size on this market (any outcome) — fallback when token match fails."""
    if session is None:
        session = _predict_monitor.get()
    best = 0.0
    try:
        r = session.get(
            "https://api.predict.fun/v1/positions",
            params={"limit": 500},
            timeout=8,
        )
        if not r.ok:
            return 0.0
        for pos in (r.json().get("data") or []):
            if not isinstance(pos, dict):
                continue
            mkt = pos.get("market") or {}
            if str(mkt.get("id")) != str(market_id):
                continue
            sh = _parse_predict_position_amount_shares(pos)
            if sh > best:
                best = sh
    except Exception:
        return 0.0
    return best


def _predict_wait_for_balance(
    session: requests.Session,
    market_id: int,
    min_shares: float,
    timeout_sec: float = 90.0,
    poll_sec: float = 1.5,
    token_id: str | None = None,
) -> tuple[float, float]:
    """Poll GET /v1/positions until sellable balance >= min_shares appears in the API.

    Predict indexes BSC settlement into REST with lag.  SELL uses that balance;
    without waiting, create_order_insufficient_shares_balance is common right
    after a BUY fill.  BSC confirms the hedge leg faster; this waits only for
    the Predict leg that must go through their API.

    If token_id is set, only the matching outcome row counts (same market can have
    YES/NO rows).

    Returns (shares_found, wait_sec). shares_found=0.0 on timeout.
    The wait_sec is recorded into the rolling p99 latency store on success.
    """
    t_start = time.time()
    deadline = t_start + timeout_sec
    # Tiny fills: 95% of 2.32 leaves little slack vs on-chain rounding / API dust.
    if min_shares < 10.0:
        _threshold = max(0.0, min_shares * 0.99 - 1e-6)
    else:
        _threshold = min_shares * 0.95
    _want_tid = (token_id or "").strip() or None
    while time.time() < deadline:
        try:
            r = session.get(
                "https://api.predict.fun/v1/positions",
                params={"limit": 500},
                timeout=8,
            )
            if r.ok:
                for pos in (r.json().get("data") or []):
                    if not isinstance(pos, dict):
                        continue
                    mkt = pos.get("market") or {}
                    if str(mkt.get("id")) != str(market_id):
                        continue
                    if _want_tid:
                        row_tid = _predict_position_row_token_id(pos)
                        if row_tid and row_tid.lower() != _want_tid.lower():
                            continue
                    shares = _parse_predict_position_amount_shares(pos)
                    if shares >= _threshold:
                        wait_sec = time.time() - t_start
                        _record_positions_lag(wait_sec)
                        return shares, wait_sec
        except Exception:
            pass
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        time.sleep(min(poll_sec, remaining))
    return 0.0, time.time() - t_start


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
    result = _vwap_and_worst_from_poly_book(book, shares)
    return result[0] if result else None


def _vwap_and_worst_from_poly_book(book: dict[str, Any], shares: float) -> tuple[float, float] | None:
    """Calculate VWAP and worst (highest) ask price touched for a given number of shares.
    Returns (vwap, worst_price) or None if insufficient liquidity (< 99% of requested shares).
    worst_price is the highest ask level needed to fill the full quantity —
    use it as the Poly limit price to guarantee a complete fill."""
    asks_raw = book.get("asks") or []
    if not asks_raw or shares <= 0:
        return None
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
    worst_price = 0.0
    for p, sz in levels:
        if remaining <= 0:
            break
        take = min(remaining, sz)
        cost += take * p
        got += take
        remaining -= take
        worst_price = p
    if got <= 0:
        return None
    # Reject if book depth covers less than 99% of requested qty — trade would be mismatched
    if got < shares * 0.99:
        return None
    return cost / got, worst_price


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


def _place_poly_limit_sell(token_id: str, qty: float, price: float) -> dict[str, Any]:
    """Place a GTC limit SELL on Polymarket at the given price.

    Returns {"filled_qty": float, "price": float, "order_id": str, "status": str}.
    Does not poll for fill — caller decides whether to wait.
    """
    import math as _m

    private_key = _normalize_hex_key(os.environ.get("POLY_PRIVATE_KEY", ""))
    funder = os.environ.get("POLY_FUNDER", "").strip()
    poly_api_key = os.environ.get("POLY_API_KEY", "").strip()
    poly_secret = os.environ.get("POLY_SECRET", "").strip()
    poly_passphrase = os.environ.get("POLY_PASSPHRASE", "").strip()
    try:
        signature_type = int(os.environ.get("POLY_SIGNATURE_TYPE", "0").strip() or "0")
    except ValueError:
        signature_type = 0

    if not private_key or not funder:
        return {"filled_qty": 0.0, "price": price, "order_id": "", "status": "skip:no_creds"}

    price_ticked = max(0.01, _m.floor(price * 1000) / 1000)  # snap to Poly 0.001 tick grid

    cli = ClobClient("https://clob.polymarket.com", chain_id=137,
                     key=private_key, signature_type=signature_type, funder=funder)
    if poly_api_key and poly_secret and poly_passphrase:
        cli.set_api_creds(ApiCreds(api_key=poly_api_key, api_secret=poly_secret, api_passphrase=poly_passphrase))
    else:
        cli.set_api_creds(cli.create_or_derive_api_creds())

    signed = cli.create_order(OrderArgs(
        token_id=token_id,
        price=price_ticked,
        size=round(qty, 4),
        side=POLY_SELL,
    ))
    resp = cli.post_order(signed, OrderType.GTC)
    status = (resp.get("status") or "").lower()
    filled_qty = qty if (status in ("matched", "filled") or resp.get("transactionsHashes")) else 0.0
    return {
        "filled_qty": filled_qty,
        "price": price_ticked,
        "order_id": resp.get("orderID", ""),
        "status": status,
    }


def _place_poly_temp_hedge(poly_token_id: str, shares: float) -> dict[str, Any]:
    """Emergency: open a temporary FAK market BUY on Poly to neutralize Predict long exposure.

    State: CHAIN_FILLED_UNINDEXED → TEMP_POLY_HEDGED.
    Fetches live orderbook VWAP, then calls _place_polymarket_fok_market_buy with fak_fallback.

    Returns {"filled_qty": float, "cost_usd": float, "vwap": float, "status": str}.
    """
    if not poly_token_id or shares < 0.01:
        return {"filled_qty": 0.0, "cost_usd": 0.0, "vwap": 0.0, "status": "skip:invalid_input"}
    try:
        book = _polymarket_book(poly_token_id)
        vwap = _vwap_from_poly_book(book, shares)
        if vwap is None or vwap <= 0.0:
            return {"filled_qty": 0.0, "cost_usd": 0.0, "vwap": 0.0, "status": "skip:no_liquidity"}
        stake_usd = shares * vwap * 1.02  # 2% slippage buffer
        leg = OpportunityLeg(
            source="emergency_temp_hedge",
            side="BUY",
            ts=datetime.utcnow().isoformat(),
            ask=vwap,
            ask_sz=shares,
            pool_usd=0.0,
            shares=shares,
            stake_usd=stake_usd,
            token_id=poly_token_id,
            market_id=None,
        )
        result = _place_polymarket_fok_market_buy(leg, fak_fallback=True)
        resp = result.get("response") or {}
        resp_status = (resp.get("status") or "").lower()
        filled_qty: float = 0.0
        if resp_status in ("matched", "filled") or resp.get("transactionsHashes"):
            try:
                _taking = int(resp.get("takingAmount") or 0)
                filled_qty = _taking / 1_000_000 if _taking > 0 else shares
            except (ValueError, TypeError):
                filled_qty = shares
        return {
            "filled_qty": filled_qty,
            "cost_usd": stake_usd,
            "vwap": vwap,
            "status": resp_status or "placed",
            "response": resp,
        }
    except Exception as _e:
        return {"filled_qty": 0.0, "cost_usd": 0.0, "vwap": 0.0, "status": f"error:{_e}"}


def _close_poly_temp_hedge(poly_token_id: str, qty: float) -> dict[str, Any]:
    """Close a temporary Poly hedge by selling qty.

    Strategy (aggressive — must close, not optional):
    1. GTC at floor(best_bid * 1000)/1000  → immediate taker fill
    2. If order goes live (resting), poll 5s then cancel
    3. Retry at bid - 0.01 tick (one level below best bid)
    4. Last resort: FAK market sell at any available price
    Returns {"filled_qty": float, "price": float, "status": str, "attempts": int}.
    """
    import math as _cm

    if not poly_token_id or qty < 0.01:
        return {"filled_qty": 0.0, "price": 0.0, "status": "skip:invalid_input", "attempts": 0}

    private_key = _normalize_hex_key(os.environ.get("POLY_PRIVATE_KEY", ""))
    funder = os.environ.get("POLY_FUNDER", "").strip()
    poly_api_key = os.environ.get("POLY_API_KEY", "").strip()
    poly_secret = os.environ.get("POLY_SECRET", "").strip()
    poly_passphrase = os.environ.get("POLY_PASSPHRASE", "").strip()
    try:
        sig_type = int(os.environ.get("POLY_SIGNATURE_TYPE", "0").strip() or "0")
    except ValueError:
        sig_type = 0

    if not private_key or not funder:
        return {"filled_qty": 0.0, "price": 0.0, "status": "skip:no_creds", "attempts": 0}

    def _build_cli() -> ClobClient:
        c = ClobClient("https://clob.polymarket.com", chain_id=137,
                       key=private_key, signature_type=sig_type, funder=funder)
        if poly_api_key and poly_secret and poly_passphrase:
            c.set_api_creds(ApiCreds(api_key=poly_api_key, api_secret=poly_secret,
                                     api_passphrase=poly_passphrase))
        else:
            c.set_api_creds(c.create_or_derive_api_creds())
        return c

    def _fetch_best_bid() -> float:
        try:
            book = _polymarket_book(poly_token_id)
            bids = book.get("bids") or []
            return float(bids[0]["price"]) if bids else 0.0
        except Exception:
            return 0.0

    total_filled = 0.0
    last_price = 0.0

    # ── Attempts 1 & 2: GTC at bid, then bid-0.01 ─────────────────────────
    for _attempt in range(2):
        best_bid = _fetch_best_bid()
        if best_bid < 0.02:
            break
        # Attempt 0: floor to tick; attempt 1: one tick lower to cross the spread
        tick_offset = 0.001 * _attempt
        sell_price = max(0.01, _cm.floor((best_bid - tick_offset) * 1000) / 1000)
        remaining = max(0.01, qty - total_filled)

        try:
            cli = _build_cli()
            signed = cli.create_order(OrderArgs(
                token_id=poly_token_id,
                price=sell_price,
                size=round(remaining, 4),
                side=POLY_SELL,
            ))
            resp = cli.post_order(signed, OrderType.GTC)
            resp_status = (resp.get("status") or "").lower()
            order_id = resp.get("orderID", "")

            if resp_status in ("matched", "filled") or resp.get("transactionsHashes"):
                total_filled += remaining
                last_price = sell_price
                return {
                    "filled_qty": total_filled, "price": last_price,
                    "status": "matched", "attempts": _attempt + 1,
                }

            if resp_status == "live" and order_id:
                # Poll up to 5s for fill
                _poll_deadline = time.time() + 5.0
                _filled_on_poll = False
                while time.time() < _poll_deadline:
                    time.sleep(1.0)
                    try:
                        _ord = cli.get_order(order_id)
                        _s = (_ord.get("status") or "").lower()
                        _sm = float(_ord.get("size_matched") or 0)
                        if _s in ("matched", "filled") or _sm > 0:
                            total_filled += _sm if _sm > 0 else remaining
                            last_price = sell_price
                            _filled_on_poll = True
                            break
                    except Exception:
                        pass
                if _filled_on_poll:
                    return {
                        "filled_qty": total_filled, "price": last_price,
                        "status": "matched_via_poll", "attempts": _attempt + 1,
                    }
                # Still not filled — cancel and try next attempt
                try:
                    cli.cancel(order_id)
                except Exception:
                    pass
                time.sleep(0.5)

        except Exception as _e:
            print(f"[TRADER][CLOSE_TEMP_HEDGE] attempt={_attempt+1} err={_e}")

    # ── Last resort: FAK market sell ──────────────────────────────────────
    remaining = max(0.01, qty - total_filled)
    if remaining >= 0.01:
        try:
            best_bid = _fetch_best_bid()
            if best_bid >= 0.02:
                stake_for_sell = remaining * best_bid * 0.98  # ~2% below mid
                cli_fak = _build_cli()
                from py_clob_client.clob_types import MarketOrderArgs as _MOA
                mo = _MOA(
                    token_id=poly_token_id,
                    amount=stake_for_sell,
                    side=POLY_SELL,
                    order_type=OrderType.FAK,
                )
                signed_fak = cli_fak.create_market_order(mo)
                resp_fak = cli_fak.post_order(signed_fak, OrderType.FAK)
                fak_status = (resp_fak.get("status") or "").lower()
                if fak_status in ("matched", "filled") or resp_fak.get("transactionsHashes"):
                    total_filled += remaining
                    last_price = best_bid
                    return {
                        "filled_qty": total_filled, "price": last_price,
                        "status": "fak_matched", "attempts": 3,
                    }
                return {
                    "filled_qty": total_filled, "price": last_price,
                    "status": f"fak_failed:{fak_status}", "attempts": 3,
                }
        except Exception as _fak_e:
            return {
                "filled_qty": total_filled, "price": last_price,
                "status": f"fak_error:{_fak_e}", "attempts": 3,
            }

    return {"filled_qty": total_filled, "price": last_price, "status": "close_exhausted", "attempts": 3}


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
    gtc_fill_timeout_sec: float | None = None,
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
    _GTC_FILL_TIMEOUT = float(
        gtc_fill_timeout_sec
        if gtc_fill_timeout_sec is not None
        else (os.environ.get("POLY_GTC_FILL_TIMEOUT_SEC", "10") or "10")
    )
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
                "filled": True,
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
                            "filled": True,
                        }
                except Exception:
                    pass
            # Timed out — cancel the resting order, then do a definitive fill check.
            # Race condition: the order may fill concurrently with or just before cancel.
            # We MUST verify before declaring failure to avoid phantom unwinds.
            if order_id:
                try:
                    client.cancel(order_id)
                    print(f"[TRADER][POLY][GTC_CANCEL] cancelled live order order_id={order_id}")
                except Exception as _ce:
                    print(f"[TRADER][POLY][GTC_CANCEL_ERR] order_id={order_id} err={_ce}")
                # Wait for exchange to process cancel and any in-flight fill settlement
                time.sleep(2.0)
                # 1) Authenticated order status check (more reliable than public endpoint)
                try:
                    _auth_order = client.get_order(order_id)
                    _auth_status = (_auth_order.get("status") or "").lower()
                    print(
                        f"[TRADER][POLY][GTC_CANCEL_VERIFY] order_id={order_id} "
                        f"auth_status={_auth_status} "
                        f"size_matched={_auth_order.get('size_matched')} "
                        f"size_filled={_auth_order.get('size_filled')}"
                    )
                    _size_matched = float(_auth_order.get("size_matched") or 0)
                    if _auth_status in ("matched", "filled") or _size_matched > 0:
                        print(
                            f"[TRADER][POLY][GTC_FILLED_AFTER_CANCEL] "
                            f"order_id={order_id} matched={_size_matched:.4f} — treating as success"
                        )
                        return {
                            "token_id": token_id,
                            "shares_requested": shares,
                            "price": price,
                            "response": _auth_order,
                            "order_type": "GTC",
                            "filled": True,
                        }
                except Exception as _goe:
                    print(f"[TRADER][POLY][GTC_GET_ORDER_ERR] order_id={order_id} err={_goe}")
                # 2) Fallback: check trade history for any fill from this order
                try:
                    from py_clob_client.clob_types import TradeParams as _TradeParams
                    _recent_trades = client.get_trades(
                        _TradeParams(id=order_id, asset_id=token_id), next_cursor="MA=="
                    )
                    if _recent_trades:
                        _fill_qty = sum(float(t.get("size") or 0) for t in _recent_trades)
                        print(
                            f"[TRADER][POLY][GTC_TRADE_HISTORY_FILL] order_id={order_id} "
                            f"trades={len(_recent_trades)} fill_qty={_fill_qty:.4f} — treating as success"
                        )
                        # Reconstruct a minimal response for the caller
                        _synth_resp = {
                            "success": True,
                            "status": "matched",
                            "orderID": order_id,
                            "takingAmount": str(_fill_qty),
                            "transactionsHashes": [t.get("transaction_hash", "") for t in _recent_trades if t.get("transaction_hash")],
                        }
                        return {
                            "token_id": token_id,
                            "shares_requested": shares,
                            "price": price,
                            "response": _synth_resp,
                            "order_type": "GTC",
                            "filled": True,
                        }
                except Exception as _gte:
                    print(f"[TRADER][POLY][GTC_TRADE_HISTORY_ERR] order_id={order_id} err={_gte}")
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
    hard_max_queue_usd = float(os.environ.get("PREDICT_HARD_MAX_QUEUE_USD", "30.0") or "30.0")

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
    replaced_order_hashes: list[str] = []  # hashes that were replaced (not just cancelled)
    cancel_reason: str | None = None
    last_get: dict[str, Any] | None = None
    all_creates: list[dict[str, Any]] = [out]
    need_final_get_check: bool = False

    def _limit_buy_mktk() -> tuple[int | None, int | None]:
        """On-chain taker-amount-from-BSC for BUY needs maker/taker in wei; prefer latest GET, else last create body."""
        p = _predict_maker_taker_wei_from_get_payload(last_get) if last_get else None
        if p:
            return p
        return _predict_maker_taker_wei_from_get_payload(all_creates[-1] if all_creates else None)

    t_deadline = time.time() + max(0.0, fill_timeout_sec)
    filled = False

    if order_hash:
        while time.time() < t_deadline:
            # ── Poll fill status ──
            try:
                last_get = _predict_get_order_by_hash(session, order_hash)
            except Exception:
                _e_prev = _bsc_head_gen_snapshot()
                _bsc_wait_new_head_or_timeout(_e_prev, poll_interval_sec)
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

            # ── Max quote age: if no fill at all within N seconds → cancel and skip ──
            # Prevents holding a stale bid for 60–80s while Poly moves against us.
            # Only applies before any fill — once partially filled we must continue or unwind.
            _max_quote_age_sec = float(os.environ.get("PREDICT_MAX_QUOTE_AGE_SEC", "0") or "0")
            if _max_quote_age_sec > 0 and first_fill_ts is None and current_filled_wei <= 0:
                _quote_age = time.time() - quote_post_ts
                if _quote_age >= _max_quote_age_sec:
                    cancel_reason = f"max_quote_age:{_quote_age:.1f}s"
                    print(
                        f"[PREDICT_LIMIT]{_trace} cancel_max_quote_age hash={order_hash} "
                        f"age={_quote_age:.1f}s limit={_max_quote_age_sec:.0f}s"
                    )
                    try:
                        if order_id:
                            _predict_remove_orders(session, [order_id])
                    except Exception as _mqa_e:
                        print(f"[PREDICT_LIMIT]{_trace} cancel_max_quote_age_err err={_mqa_e}")
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
                        # Track old hash BEFORE overwriting — it may have filled on BSC
                        if order_hash:
                            replaced_order_hashes.append(order_hash)
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

            _poll_prev = _bsc_head_gen_snapshot()
            _poll_new = _bsc_wait_new_head_or_timeout(_poll_prev, max(0.05, poll_interval_sec))
            if order_hash and not filled and _poll_new > _poll_prev:
                try:
                    _pm, _pt = _limit_buy_mktk()
                    _poll_bsc = _bsc_filled_taker_shares_for_hedge(
                        order_hash, float(leg.shares or 0), _pm, _pt, log=False
                    )
                    if _poll_bsc > 0:
                        filled = True
                        _pb_wei = int(_poll_bsc * 10**18)
                        if _pb_wei > prev_filled_wei:
                            _pb_delta = _pb_wei - prev_filled_wei
                            _pb_now = time.time()
                            if first_fill_ts is None:
                                first_fill_ts = _pb_now
                            partial_fills.append({
                                "ts": _pb_now,
                                "delta_wei": _pb_delta,
                                "cumulative_wei": _pb_wei,
                                "delta_shares": _pb_delta / 10**18,
                                "cumulative_shares": _poll_bsc,
                                "source": "bsc_ws_head",
                            })
                            prev_filled_wei = _pb_wei
                        print(
                            f"[PREDICT_LIMIT]{_trace} bsc_fill_on_new_head hash={order_hash} "
                            f"shares={_poll_bsc:.4f}"
                        )
                        break
                except Exception:
                    pass

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
            _api_ok = False
            try:
                last_get = _predict_get_order_by_hash(_predict_monitor.get(), order_hash)
                _api_ok = True
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
                pass  # API down — fall through to BSC check below

            # BSC is the ground truth for hedging: check every 3 attempts regardless of
            # whether the API is up or just lagging (returning 0 for a real on-chain fill).
            # Previously this only ran inside `except` (API down); now it also fires when
            # the API responds OK but hasn't indexed the fill yet.
            if not filled and _attempt % 3 == 0:
                try:
                    _bm, _bt = _limit_buy_mktk()
                    _bsc_shares = _bsc_filled_taker_shares_for_hedge(
                        order_hash, float(leg.shares or 0), _bm, _bt, log=False
                    )
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
                            f"[PREDICT_LIMIT]{_trace} ghost_fill_watch_bsc "
                            f"api_ok={_api_ok} hash={order_hash} "
                            f"bsc_shares={_bsc_shares:.4f} attempt={_attempt}"
                        )
                        filled = True
                        break
                except Exception:
                    pass

            if _attempt < _FINAL_GET_RETRIES - 1:
                _gf_prev = _bsc_head_gen_snapshot()
                _gf_new = _bsc_wait_new_head_or_timeout(_gf_prev, _FINAL_GET_SLEEP_SEC)
                if not filled and order_hash and _gf_new > _gf_prev and (_attempt % 3 != 0):
                    try:
                        _gm, _gt = _limit_buy_mktk()
                        _gf_bsc = _bsc_filled_taker_shares_for_hedge(
                            order_hash, float(leg.shares or 0), _gm, _gt, log=False
                        )
                        if _gf_bsc > 0:
                            now_ts = time.time()
                            if first_fill_ts is None:
                                first_fill_ts = now_ts
                            _gf_wei = int(_gf_bsc * 10**18)
                            if _gf_wei > prev_filled_wei:
                                partial_fills.append({
                                    "ts": now_ts,
                                    "delta_wei": _gf_wei - prev_filled_wei,
                                    "cumulative_wei": _gf_wei,
                                    "delta_shares": _gf_bsc - prev_filled_wei / 10**18,
                                    "cumulative_shares": _gf_bsc,
                                    "source": "bsc_direct",
                                })
                                prev_filled_wei = _gf_wei
                            print(
                                f"[PREDICT_LIMIT]{_trace} ghost_fill_watch_bsc_on_head "
                                f"hash={order_hash} bsc_shares={_gf_bsc:.4f}"
                            )
                            filled = True
                            break
                    except Exception:
                        pass

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

    # Replaced outbid orders: we reset prev_filled_wei to 0 for the new hash, but the old hash
    # may have partial/late amountFilled on-chain. If we only count the final order, Poly hedge
    # matches too few shares (Predict activity shows two buys; Poly one).
    replaced_filled_wei_total = 0
    for _rh in replaced_order_hashes:
        if not _rh:
            continue
        _rh_best = 0
        for _ri in range(6):
            try:
                _rh_get = _predict_get_order_by_hash(session, str(_rh))
                _rh_w = _get_filled_wei(_rh_get)
                if _rh_w > _rh_best:
                    _rh_best = _rh_w
            except Exception:
                pass
            if _ri < 5:
                time.sleep(0.25)
        replaced_filled_wei_total += _rh_best
        if _rh_best > 0:
            print(
                f"[PREDICT_LIMIT]{_trace} replaced_hash_filled "
                f"hash={str(_rh)[:16]}... sh={_rh_best / 10**18:.4f}"
            )

    total_filled_wei = prev_filled_wei + replaced_filled_wei_total
    if replaced_filled_wei_total > 0:
        print(
            f"[PREDICT_LIMIT]{_trace} total_filled merge final={prev_filled_wei / 10**18:.4f} sh "
            f"+ replaced={replaced_filled_wei_total / 10**18:.4f} sh "
            f"(n_replaced={len(replaced_order_hashes)})"
        )
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
        "replaced_order_hashes": replaced_order_hashes,
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
            "replaced_filled_shares": round(replaced_filled_wei_total / 10**18, 6) if replaced_filled_wei_total else 0.0,
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
    replace_interval_sec: float = 3.0,
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

    # Wait for Predict REST to index post-buy balance before SELL (all call sites).
    _idx_raw = os.environ.get("PREDICT_SELL_INDEX_WAIT_SEC", "").strip()
    if not _idx_raw:
        _idx_raw = os.environ.get("PREDICT_UNWIND_BAL_TIMEOUT_SEC", "").strip()
    _idx_wait = float(_idx_raw or "90")
    _positions_seen_sec: float | None = None
    if _idx_wait > 0 and sell_qty >= 0.01:
        _bal, _pos_wait = _predict_wait_for_balance(
            session,
            int(leg.market_id),
            sell_qty,
            timeout_sec=_idx_wait,
            token_id=str(token_id) if token_id else None,
        )
        _positions_seen_sec = _pos_wait
        if _bal <= 0 and token_id:
            # Token id in /positions may not match SDK string; use largest row on this market.
            # Tight band vs requested size avoids picking the wrong outcome when user holds both.
            _bal_fb = _predict_max_shares_for_market(session, int(leg.market_id))
            if sell_qty * 0.85 <= _bal_fb <= sell_qty * 1.02:
                _bal = _bal_fb
                print(
                    f"[TRADER]{_trace} predict_sell_index_wait_fallback market_id={leg.market_id} "
                    f"max_market_shares={_bal_fb:.6f}"
                )
        print(
            f"[TRADER]{_trace} predict_sell_index_wait market_id={leg.market_id} "
            f"token={str(token_id)[:18]}... need={sell_qty:.4f} found={_bal:.4f} "
            f"timeout={_idx_wait:.0f}s"
        )
        if _bal > 0:
            # Never ask to sell more than REST reports (fixes insufficient_shares from wei rounding).
            _cap = min(sell_qty, _bal * (1.0 - 1e-9))
            sell_qty = max(0.01, math.floor(_cap * 1_000_000) / 1_000_000)

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

        # Cancel current order and re-place one tick lower.
        # ── BSC pre-replace guard: if the order already filled on-chain (API lag), do NOT replace. ──
        # BSC block = 3s; replace_interval_sec = 3s → fills happen before API indexes them.
        # Without this check, the cancelled (ignored) filled order + new replacement BOTH execute,
        # causing a double-sell from the shared position balance.
        if _active_order_hash:
            try:
                _bsc_pre = _bsc_check_order_filled(
                    _active_order_hash,
                    max(0.01, sell_qty - (filled_wei / 10**18 if filled_wei > 0 else 0.0)),
                )
                if _bsc_pre > 0:
                    filled = True
                    _bsc_wei = int(_bsc_pre * 10**18)
                    if _bsc_wei > filled_wei:
                        filled_wei = _bsc_wei
                    print(
                        f"[TRADER]{_trace} sell_bsc_pre_replace_fill hash={_active_order_hash} "
                        f"qty={_bsc_pre:.4f} — skipping replace"
                    )
                    break
            except Exception:
                pass
        if _active_order_id:
            try:
                _predict_remove_orders(session, [_active_order_id])
            except Exception:
                pass
        # ── BSC post-cancel guard ──
        # A fill can land on-chain in the window between the pre-cancel BSC check and
        # the actual cancel execution (BSC block = ~3s, replace_interval = 3s).
        # Re-check BSC after cancel; if the old order now shows filled, skip replacement.
        if _active_order_hash:
            try:
                _bsc_post = _bsc_check_order_filled(
                    _active_order_hash,
                    max(0.01, sell_qty - (filled_wei / 10**18 if filled_wei > 0 else 0.0)),
                )
                if _bsc_post > 0:
                    filled = True
                    _bsc_post_wei = int(_bsc_post * 10**18)
                    if _bsc_post_wei > filled_wei:
                        filled_wei = _bsc_post_wei
                    print(
                        f"[TRADER]{_trace} sell_bsc_post_cancel_fill hash={_active_order_hash} "
                        f"qty={_bsc_post:.4f} — skipping replacement"
                    )
                    break
            except Exception:
                pass
        current_price = max(_tick, round(current_price - _tick, 6))
        # Compute remaining unfilled quantity — avoid re-placing for already-filled shares
        _filled_so_far = filled_wei / 10**18 if filled_wei > 0 else 0.0
        _rem_qty = max(0.01, sell_qty - _filled_so_far)
        _rem_qty_wei = _wei_from_float(_rem_qty)
        print(
            f"[TRADER]{_trace} predict_limit_sell replace → new_price={current_price:.4f} "
            f"filled_so_far={_filled_so_far:.4f} remaining={_rem_qty:.4f} "
            f"remaining_sec={(t_deadline - time.time()):.1f}"
        )
        try:
            _rep_price_wei = _wei_from_float(current_price)
            _rep_amounts = builder.get_limit_order_amounts(
                LimitHelperInput(side=Side.SELL, price_per_share_wei=_rep_price_wei, quantity_wei=_rem_qty_wei)
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
        "positions_seen_sec": _positions_seen_sec,
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


@app.get("/health")
def health() -> dict[str, str]:
    """Liveness for reverse-proxy / monitoring; does not check venue APIs."""
    return {"status": "ok"}


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

    # Guard: не торговать если до закрытия рынка недостаточно времени.
    # Минимальный буфер = PREDICT_SELL_INDEX_WAIT_SEC (90s, индексер) +
    # fill_timeout_sec unwind (60s) × 3 retry + задержки (~300s итого).
    # Самый опасный сценарий: Predict-fill уже произошёл, а продать обратно
    # некуда — рынок закрылся, позиция висит без хеджа до экспайри.
    if opp.end_date:
        try:
            _end_dt = datetime.fromisoformat(opp.end_date.rstrip("Z")).replace(tzinfo=timezone.utc)
            _secs_to_end = (_end_dt - datetime.now(timezone.utc)).total_seconds()
            _static_buf = float(os.environ.get("PREDICT_MIN_EXPIRY_BUFFER_SEC", "300") or "300")
            _p99_lag = _positions_lag_p99()
            # unwind budget: fill_timeout_sec=60 × up to 5 retries + 60s safety margin
            _unwind_budget_sec = 60.0 * 5 + 60.0
            _dynamic_buf = (_p99_lag * 1.5 + _unwind_budget_sec) if _p99_lag is not None else None
            _min_expiry_buf = max(_static_buf, _dynamic_buf) if _dynamic_buf is not None else _static_buf
            if _secs_to_end < _min_expiry_buf:
                print(
                    f"[TRADER]{_t}[SKIP] label={opp.label} "
                    f"reason=market_close_imminent secs_to_end={_secs_to_end:.1f} "
                    f"min_buffer={_min_expiry_buf:.0f}s "
                    f"(static={_static_buf:.0f}s dynamic={f'{_dynamic_buf:.0f}s' if _dynamic_buf is not None else 'n/a'} p99_lag={f'{_p99_lag:.1f}s' if _p99_lag is not None else 'n/a'})"
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
            _vwap_worst_pre = _vwap_and_worst_from_poly_book(live_book, float(opp.shares))
            if _vwap_worst_pre is None:
                # Poly book doesn't have enough depth for the full qty — skip before Predict
                _poly_min_pre2 = float(os.environ.get("POLY_MIN_ORDER_USD", "1.0") or "1.0")
                row["skipped"] = True
                row["skip_reason"] = {"code": "poly_insufficient_depth", "shares": float(opp.shares)}
                row["summary"]["status"] = "skipped"
                row["summary"]["reason_code"] = "poly_insufficient_depth"
                row["summary"]["reason"] = row["skip_reason"]
                print(f"[TRADER]{_t}[SKIP] label={opp.label} reason=poly_insufficient_depth shares={opp.shares:.4f}")
                if _market_id_int is not None:
                    with _predict_market_in_flight_lock:
                        _predict_market_in_flight.discard(_market_id_int)
                _append_jsonl(trades_file, row)
                return {"status": "skipped", "reason": "poly_insufficient_depth"}
            _live_vwap_pre = _vwap_worst_pre[0]
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

        # ── Book freshness guard: skip if Poly data is too stale ──────────
        # Stale Poly quotes mean we're pricing the arb off old information.
        # By the time we get a Predict fill, Poly may have moved far away.
        # Default 0 = disabled (set via BA_MAX_POLY_BOOK_AGE_MS env var).
        _max_poly_book_age_ms = float(os.environ.get("BA_MAX_POLY_BOOK_AGE_MS", "0") or "0")
        _max_pred_book_age_ms = float(os.environ.get("BA_MAX_PRED_BOOK_AGE_MS", "0") or "0")
        if _max_poly_book_age_ms > 0 and _book_freshness.get("poly_book_age_at_submit_ms", 0) > _max_poly_book_age_ms:
            _poly_age_ms = _book_freshness["poly_book_age_at_submit_ms"]
            print(
                f"[TRADER]{_t}[SKIP] label={opp.label} reason=poly_book_stale "
                f"age={_poly_age_ms:.0f}ms limit={_max_poly_book_age_ms:.0f}ms"
            )
            row["ok"] = False
            row["skipped"] = True
            row["summary"]["status"] = "skipped"
            row["summary"]["reason_code"] = "poly_book_stale"
            row["summary"]["reason"] = {"code": "poly_book_stale", "poly_book_age_ms": _poly_age_ms, "limit_ms": _max_poly_book_age_ms}
            _append_jsonl(trades_file, row)
            return {"status": "skipped", "reason": "poly_book_stale"}
        if _max_pred_book_age_ms > 0 and _book_freshness.get("pred_book_age_at_submit_ms", 0) > _max_pred_book_age_ms:
            _pred_age_ms = _book_freshness["pred_book_age_at_submit_ms"]
            print(
                f"[TRADER]{_t}[SKIP] label={opp.label} reason=pred_book_stale "
                f"age={_pred_age_ms:.0f}ms limit={_max_pred_book_age_ms:.0f}ms"
            )
            row["ok"] = False
            row["skipped"] = True
            row["summary"]["status"] = "skipped"
            row["summary"]["reason_code"] = "pred_book_stale"
            row["summary"]["reason"] = {"code": "pred_book_stale", "pred_book_age_ms": _pred_age_ms, "limit_ms": _max_pred_book_age_ms}
            _append_jsonl(trades_file, row)
            return {"status": "skipped", "reason": "pred_book_stale"}

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
                _ba_mktk = _predict_maker_taker_wei_from_get_payload(
                    _ba_pred_resp.get("get") if isinstance(_ba_pred_resp.get("get"), dict) else None
                ) or _predict_maker_taker_wei_from_get_payload(
                    _ba_pred_resp.get("create") if isinstance(_ba_pred_resp.get("create"), dict) else None
                )
                _ba_mk, _ba_tk = _ba_mktk if _ba_mktk else (None, None)
                if pred_hash_ba and _ba_cancel_rsn and pred_leg.market_id is not None:
                    _late_watch_save(
                        str(pred_hash_ba),
                        int(pred_leg.market_id),
                        poly_leg.token_id if poly_leg else None,
                        float(opp.shares),
                        maker_amount=_ba_mk,
                        taker_amount=_ba_tk,
                    )
                    print(f"[TRADER]{_t} late_watch_registered hash={str(pred_hash_ba)[:14]}... cancel_reason={_ba_cancel_rsn}")
                    # Also register every hash that was replaced during the order lifetime.
                    # When a replace happens, the OLD hash is broadcast to BSC before replacement
                    # and may fill on-chain even after the API consider it cancelled.
                    _ba_replaced_hashes = _ba_pred_resp.get("replaced_order_hashes") or []
                    for _old_h in _ba_replaced_hashes:
                        if _old_h and _old_h != str(pred_hash_ba):
                            # Do not use current order's maker/taker for a *previous* order hash
                            # (replaced at a new price) — late_watch resolves m/t via GET by hash.
                            _late_watch_save(
                                str(_old_h),
                                int(pred_leg.market_id),
                                poly_leg.token_id if poly_leg else None,
                                float(opp.shares),
                            )
                            print(f"[TRADER]{_t} late_watch_registered_replaced hash={str(_old_h)[:14]}... (was replaced by {str(pred_hash_ba)[:14]}...)")
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
            # For the live recheck AFTER Predict fill, safety buffer must be 0:
            # we already own the position, the only question is whether hedging is
            # better than unwinding. Applying the pre-trade safety margin here causes
            # profitable hedges (sum < 1.0) to be mistakenly treated as no_edge.
            _ba_live_recheck_safety_bps = float(os.environ.get("BA_LIVE_RECHECK_SAFETY_BPS", "0") or "0")
            _live_net_edge_ba: float | None = None
            _live_vwap_ba: float | None = None
            _live_worst_ba: float | None = None
            _live_poly_fee_ba: float | None = None

            try:
                _live_book_ba = _polymarket_book(str(poly_leg.token_id))
                _vwap_worst = _vwap_and_worst_from_poly_book(_live_book_ba, _ba_hedge_qty)
                if _vwap_worst:
                    _live_vwap_ba, _live_worst_ba = _vwap_worst
            except Exception as _e_ba_live:
                print(f"[TRADER] bid_ask_hedge_live_check failed (non-fatal): {_e_ba_live}")

            if _live_vwap_ba is not None:
                _live_poly_fee_ba = _ba_fee_rate * _live_vwap_ba * (1.0 - _live_vwap_ba)
                _pred_eff_ba = _ba_actual_pred_bid * (1.0 + _ba_pred_fee_bps / 10_000)
                _poly_eff_ba = _live_vwap_ba + _live_poly_fee_ba
                _live_net_edge_ba = 1.0 - _pred_eff_ba - _poly_eff_ba - _ba_live_recheck_safety_bps / 10_000

                row["live_hedge_recheck"] = {
                    "pred_bid": _ba_actual_pred_bid,
                    "live_poly_vwap": round(_live_vwap_ba, 6),
                    "live_poly_fee": round(_live_poly_fee_ba, 6),
                    "live_net_edge": round(_live_net_edge_ba, 6),
                    "live_net_edge_bps": round(_live_net_edge_ba * 10_000, 1),
                    "live_recheck_safety_bps": _ba_live_recheck_safety_bps,
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
                        f"— loss-based exit: unwind on Predict vs hedge on Polymarket (exactly one)"
                    )
                    # Try to unwind on Predict, OR (if cheaper in $) skip straight to Poly hedge.
                    _ne_unwind_tick = float(os.environ.get("PREDICT_TICK_SIZE", "0.01") or "0.01")
                    # Pick the exit with *lower* estimated USDC loss at live quotes; execute only
                    # that path first (fallback to the other if the primary action fails to fill).
                    _ba_no_edge_chose_hedge = False
                    # Approximate: Poly hedge below this USD notional cannot use a normal hedge
                    # (must unwind on Predict) — same threshold as the below-min-hedge branch.
                    _ne_poly_ov_thr = float(
                        os.environ.get("POLY_OVER_HEDGE_MIN_USD", "0.80") or "0.80"
                    )
                    _ne_pred_eff_loss = _ba_actual_pred_bid * (1.0 + _ba_pred_fee_bps / 10_000)
                    _ne_poly_eff_loss = _live_vwap_ba + _live_poly_fee_ba
                    _L_hedge_usd = max(
                        0.0, (_ne_pred_eff_loss + _ne_poly_eff_loss - 1.0) * _ba_hedge_qty
                    )
                    _lsell_cmp = _predict_live_sell_price(
                        None,
                        int(pred_leg.market_id),
                        pred_leg.side,
                        fallback=_ba_actual_pred_bid,
                        tick=_ne_unwind_tick,
                    )
                    _Puw_est = max(
                        _ne_unwind_tick, round(_lsell_cmp - _ne_unwind_tick, 6)
                    )
                    _L_unwind_usd = max(0.0, (_ne_pred_eff_loss - _Puw_est) * _ba_hedge_qty)
                    _hedge_size_feasible = _ba_hedge_qty * float(_live_vwap_ba) >= _ne_poly_ov_thr
                    _ne_tie = (os.environ.get("BA_NO_EDGE_TIE", "unwind") or "unwind").lower()

                    # ── Emergency Poly temp-hedge ──────────────────────────────────────────
                    # If Predict positions are not yet indexed, we can't sell immediately.
                    # Place a temporary opposite-side position on Poly to neutralize delta
                    # exposure during the indexer lag window.
                    # State path: CHAIN_FILLED_UNINDEXED → TEMP_POLY_HEDGED → PREDICT_UNWOUND
                    #             → TEMP_HEDGE_RELEASED (or TEMP_HEDGE_RETAINED on Predict fail)
                    _ne_temp_hedge_result: dict[str, Any] = {}
                    _ne_temp_hedge_qty: float = 0.0
                    _ne_temp_hedge_state: str = "none"
                    _ne_use_temp_hedge = (
                        os.environ.get("PREDICT_EMERGENCY_POLY_HEDGE", "1") not in ("0", "false", "no")
                        and poly_leg.token_id is not None
                    )
                    if _ne_use_temp_hedge:
                        # Quick balance check (no blocking wait) — 0 means positions not indexed
                        _ne_quick_bal = _predict_max_shares_for_market(None, int(pred_leg.market_id))
                        if _ne_quick_bal < _ba_net_sell_qty * 0.5:
                            print(
                                f"[TRADER][EMERGENCY_HEDGE] positions_not_indexed label={opp.label} "
                                f"quick_bal={_ne_quick_bal:.4f} target={_ba_net_sell_qty:.4f} "
                                f"— placing temp Poly hedge"
                            )
                            _ne_temp_hedge_result = _place_poly_temp_hedge(
                                str(poly_leg.token_id), _ba_net_sell_qty
                            )
                            _ne_temp_hedge_qty = _ne_temp_hedge_result.get("filled_qty", 0.0)
                            if _ne_temp_hedge_qty > 0:
                                _ne_temp_hedge_state = "placed"
                                notify(
                                    f"🟡 <b>TEMP POLY HEDGE PLACED</b>\n"
                                    f"\n"
                                    f"<b>{opp.label}</b>\n"
                                    f"\n"
                                    f"Predict long {_ba_net_sell_qty:.2f} sh — position indexer lag\n"
                                    f"Poly: bought {_ne_temp_hedge_qty:.2f} sh (opposite leg)\n"
                                    f"VWAP: {_ne_temp_hedge_result.get('vwap', 0):.3f} | cost ${_ne_temp_hedge_result.get('cost_usd', 0):.2f}\n"
                                    f"\n"
                                    f"Waiting for Predict indexer…"
                                )
                            else:
                                _ne_temp_hedge_state = "placement_failed"
                                print(
                                    f"[TRADER][EMERGENCY_HEDGE] placement_failed label={opp.label} "
                                    f"status={_ne_temp_hedge_result.get('status')} "
                                    f"— proceeding without temp hedge"
                                )
                    # ──────────────────────────────────────────────────────────────────────
                    if _ne_temp_hedge_qty > 0 and _ne_temp_hedge_state == "placed":
                        _ba_no_edge_chose_hedge = False
                    elif not _hedge_size_feasible:
                        _ba_no_edge_chose_hedge = False
                    else:
                        if _L_hedge_usd < _L_unwind_usd - 1e-9:
                            _ba_no_edge_chose_hedge = True
                        elif (
                            abs(_L_hedge_usd - _L_unwind_usd)
                            <= 1e-6 * max(1.0, _ba_hedge_qty * _Puw_est)
                        ):
                            _ba_no_edge_chose_hedge = _ne_tie in (
                                "hedge", "h", "poly", "polymarket", "p"
                            )
                        else:
                            _ba_no_edge_chose_hedge = False
                    row["no_edge_loss_pick"] = {
                        "L_hedge_usd": round(_L_hedge_usd, 6),
                        "L_unwind_usd": round(_L_unwind_usd, 6),
                        "P_unwind_est": round(_Puw_est, 6),
                        "pred_eff": round(_ne_pred_eff_loss, 6),
                        "poly_eff": round(_ne_poly_eff_loss, 6),
                        "hedge_size_feasible": _hedge_size_feasible,
                        "chose_hedge_first": _ba_no_edge_chose_hedge,
                    }
                    print(
                        f"[TRADER] no_edge_loss_cmp label={opp.label} "
                        f"L_hedge=${_L_hedge_usd:.4f} L_unwind=${_L_unwind_usd:.4f} "
                        f"tie_pref={_ne_tie} → {'HEDGE_POLY_FIRST' if _ba_no_edge_chose_hedge else 'UNWIND_FIRST'}"
                    )

                    _ne_unwind_result: dict[str, Any] = {}
                    _ne_unwind_err: str | None = None
                    _ne_unwind_price = _ne_unwind_tick
                    _NE_UW_RETRIES = int(
                        float(os.environ.get("PREDICT_NO_EDGE_UNWIND_RETRIES", "5") or "5")
                    )
                    _NE_UW_DELAYS = [2.0, 4.0, 6.0, 8.0, 10.0]
                    _ne_fill_sec = float(
                        os.environ.get("PREDICT_NO_EDGE_UNWIND_FILL_SEC", "90") or "90"
                    )
                    _ne_uw_sold = 0.0
                    if not _ba_no_edge_chose_hedge:
                        for _ne_attempt in range(_NE_UW_RETRIES):
                            if _ne_attempt > 0:
                                time.sleep(
                                    _NE_UW_DELAYS[min(_ne_attempt - 1, len(_NE_UW_DELAYS) - 1)]
                                )
                            _ne_remaining = max(0.01, _ba_net_sell_qty - _ne_uw_sold)
                            if _ne_remaining < 0.01:
                                break
                            _live_sell = _predict_live_sell_price(
                                None,
                                int(pred_leg.market_id),
                                pred_leg.side,
                                fallback=_ba_actual_pred_bid,
                                tick=_ne_unwind_tick,
                            )
                            # One tick more aggressive than top-of-book so we take priority over
                            # a deep queue; still a limit order (not a market).
                            _ne_unwind_price = max(
                                _ne_unwind_tick, round(_live_sell - _ne_unwind_tick, 6)
                            )
                            print(
                                f"[TRADER] no_edge_unwind_pricing n={_ne_attempt + 1}/{_NE_UW_RETRIES} "
                                f"label={opp.label} live_sell={_live_sell:.4f} limit={_ne_unwind_price:.4f}"
                            )
                            _ne_unwind_result = {}
                            _ne_unwind_err = None
                            try:
                                _ne_unwind_result = _place_predict_limit_sell(
                                    pred_leg,
                                    sell_qty=_ne_remaining,
                                    sell_price=_ne_unwind_price,
                                    fill_timeout_sec=_ne_fill_sec,
                                    trace_id=trace_id,
                                )
                                _ne_uw_sold += _ne_unwind_result.get("filled_qty", 0.0)
                                if _ne_unwind_result.get("positions_seen_sec") is not None:
                                    row["timing"]["positions_seen_sec"] = _ne_unwind_result[
                                        "positions_seen_sec"
                                    ]
                                if _ne_uw_sold >= _ba_net_sell_qty * 0.99:
                                    break
                            except Exception as _ne_uw_e:
                                _ne_unwind_err = str(_ne_uw_e)
                                print(
                                    f"[TRADER][UNWIND_ERROR] no_edge attempt={_ne_attempt+1}/"
                                    f"{_NE_UW_RETRIES} label={opp.label} err={_ne_uw_e}"
                                )
                                if "insufficient" in _ne_unwind_err.lower():
                                    break
                                _ne_is_lag = "400" in _ne_unwind_err
                                if not _ne_is_lag or _ne_attempt == _NE_UW_RETRIES - 1:
                                    break
                        if _ne_uw_sold < _ba_net_sell_qty * 0.99 and not _ne_unwind_err:
                            _ne_unwind_err = (
                                f"predict_limit_0_filled: sold={_ne_uw_sold:.4f}/"
                                f"{_ba_net_sell_qty:.4f} last_limit={_ne_unwind_price:.4f} "
                                f"retries={_NE_UW_RETRIES} fill_timeout_sec={_ne_fill_sec}"
                            )
                    elif _ba_no_edge_chose_hedge:
                        print(
                            f"[TRADER] no_edge_skip_unwind label={opp.label} "
                            f"— loss_cmp chose POLY first (HEDGE_POLY_FIRST), skipping Predict unwind"
                        )
                    _ne_uw_filled = _ne_uw_sold >= _ba_net_sell_qty * 0.99
                    _ne_uw_qty = _ne_uw_sold
                    if _ne_uw_filled or _ne_uw_qty > 0:
                        # Unwind succeeded (fully or partially) → declare incident, do NOT hedge

                        # Close temp Poly hedge proportional to what we sold on Predict.
                        # MUST close — retry until sold or all attempts exhausted.
                        _ne_close_result: dict[str, Any] = {}
                        if _ne_temp_hedge_qty > 0 and _ne_temp_hedge_state == "placed":
                            _close_ratio = _ne_uw_qty / _ba_net_sell_qty if _ba_net_sell_qty > 0 else 1.0
                            _ne_close_qty = round(
                                min(_ne_temp_hedge_qty, _ne_temp_hedge_qty * _close_ratio), 4
                            )
                            if _ne_close_qty >= 0.01:
                                print(
                                    f"[TRADER][EMERGENCY_HEDGE] closing_temp_hedge label={opp.label} "
                                    f"hedge_qty={_ne_temp_hedge_qty:.4f} close_qty={_ne_close_qty:.4f}"
                                )
                                # _close_poly_temp_hedge already retries internally (GTC×2 + FAK).
                                # If it still fails, retry the whole closer up to 2 more times
                                # before declaring stuck.
                                _CLOSE_OUTER_RETRIES = 3
                                for _close_outer in range(_CLOSE_OUTER_RETRIES):
                                    if _close_outer > 0:
                                        time.sleep(3.0)
                                    _ne_close_result = _close_poly_temp_hedge(
                                        str(poly_leg.token_id), _ne_close_qty
                                    )
                                    if _ne_close_result.get("filled_qty", 0) > 0:
                                        break
                                    print(
                                        f"[TRADER][EMERGENCY_HEDGE] close_outer_retry "
                                        f"attempt={_close_outer+1}/{_CLOSE_OUTER_RETRIES} "
                                        f"label={opp.label} status={_ne_close_result.get('status')}"
                                    )

                                _ne_temp_hedge_state = (
                                    "released" if _ne_close_result.get("filled_qty", 0) > 0
                                    else "stuck"  # exhausted all attempts
                                )
                                print(
                                    f"[TRADER][EMERGENCY_HEDGE] close_final label={opp.label} "
                                    f"state={_ne_temp_hedge_state} "
                                    f"filled={_ne_close_result.get('filled_qty', 0):.4f} "
                                    f"price={_ne_close_result.get('price', 0):.4f} "
                                    f"status={_ne_close_result.get('status')}"
                                )
                                if _ne_temp_hedge_state == "stuck":
                                    notify(
                                        f"🚨🚨🚨 <b>STUCK TEMP HEDGE — CLOSE MANUALLY</b>\n"
                                        f"\n"
                                        f"<b>{opp.label}</b>\n"
                                        f"\n"
                                        f"Predict unwind: ✅ sold {_ne_uw_qty:.2f} sh\n"
                                        f"Poly temp hedge: {_ne_close_qty:.2f} sh — close failed\n"
                                        f"Token: <code>{str(poly_leg.token_id)[:32]}</code>\n"
                                        f"\n"
                                        f"Sell the Poly leg manually on Polymarket.\n"
                                    )
                            else:
                                _ne_temp_hedge_state = "skip_close_qty_too_small"
                        row["temp_hedge"] = {
                            "state": _ne_temp_hedge_state,
                            "placed_qty": _ne_temp_hedge_qty,
                            "placed_cost_usd": _ne_temp_hedge_result.get("cost_usd", 0.0),
                            "placed_vwap": _ne_temp_hedge_result.get("vwap", 0.0),
                            "close_result": _ne_close_result,
                        }

                        row["ok"] = False
                        row["summary"]["status"] = "incident"
                        row["summary"]["reason_code"] = "bid_ask_hedge_no_edge"
                        row["no_edge_unwind"] = {
                            "unwind_price": _ne_unwind_price,
                            "unwind_qty": _ne_uw_qty,
                            "unwind_filled": _ne_uw_filled,
                            "unwind_error": _ne_unwind_err,
                        }
                        print(
                            f"[TRADER][INCIDENT] BID_ASK_HEDGE_NO_EDGE unwind_ok label={opp.label} "
                            f"sold={_ne_uw_qty:.4f}/{_ba_net_sell_qty:.4f} price={_ne_unwind_price:.4f}"
                        )
                        if _ne_uw_filled:
                            _ne_uw_status = f"✅ sold {_ne_uw_qty:.2f} sh @ {_ne_unwind_price:.2f}"
                        else:
                            _ne_uw_rem = _ba_net_sell_qty - _ne_uw_qty
                            _ne_uw_status = (
                                f"⚠️ PARTIAL {_ne_uw_qty:.2f}/{_ba_net_sell_qty:.2f} sh — "
                                f"left {_ne_uw_rem:.2f} sh — check manually"
                            )
                        # Build temp hedge line for notification
                        _ne_th_line = ""
                        if _ne_temp_hedge_state == "released":
                            _ne_th_line = (
                                f"\n<i>🛡 Temp hedge closed: sold {_ne_close_result.get('filled_qty', 0):.2f} sh Poly "
                                f"@ {_ne_close_result.get('price', 0):.3f}</i>"
                            )
                        elif _ne_temp_hedge_state == "placed":
                            _ne_th_line = (
                                f"\n<i>⚠️ Temp hedge not closed: {_ne_temp_hedge_qty:.2f} sh Poly — check manually</i>"
                            )
                        elif _ne_temp_hedge_state == "close_failed":
                            _ne_th_line = (
                                f"\n<i>🔴 Temp hedge close failed: {_ne_temp_hedge_qty:.2f} sh Poly — manual</i>"
                            )
                        _ne_tg_resolved = _ne_uw_filled and _ne_temp_hedge_state not in (
                            "placed",
                            "stuck",
                            "close_failed",
                        )
                        _em_ne = _incident_tg_emoji(_ne_tg_resolved)
                        notify(
                            f"{_em_ne} <b>INCIDENT: HEDGE NO EDGE → UNWIND</b>\n"
                            f"\n"
                            f"<b>{opp.label}</b>\n"
                            f"\n"
                            f"Predict fill: <b>{_ba_hedge_qty:.2f} sh</b> @ {_ba_actual_pred_bid:.2f}\n"
                            f"Poly moved: VWAP {_live_vwap_ba:.2f} → edge {_live_net_edge_ba * 10_000:.0f} bps\n"
                            f"\n"
                            f"Predict sell @ {_ne_unwind_price:.2f}: {_ne_uw_status}\n"
                            f"{_ne_th_line}\n"
                        )
                        _append_jsonl(trades_file, row)
                        return {"status": "incident", "reason": "bid_ask_hedge_no_edge"}
                    else:
                        # Unwind failed (we tried) → temp hedge + notify, then fall through to Poly
                        if not _ba_no_edge_chose_hedge:
                            if _ne_temp_hedge_qty > 0 and _ne_temp_hedge_state == "placed":
                                _ne_temp_hedge_state = "retained"
                                notify(
                                    f"🔴 <b>TEMP POLY HEDGE RETAINED</b>\n"
                                    f"\n"
                                    f"<b>{opp.label}</b>\n"
                                    f"\n"
                                    f"Predict unwind failed — temp Poly hedge left on as cover\n"
                                    f"Poly: {_ne_temp_hedge_qty:.2f} sh @ "
                                    f"{_ne_temp_hedge_result.get('vwap', 0):.3f}\n"
                                    f"Next: manually unwind Predict and close the Poly temp hedge\n"
                                )
                                print(
                                    f"[TRADER][EMERGENCY_HEDGE] retained label={opp.label} "
                                    f"hedge_qty={_ne_temp_hedge_qty:.4f} — predict_unwind_failed"
                                )
                            row["temp_hedge"] = {
                                "state": _ne_temp_hedge_state,
                                "placed_qty": _ne_temp_hedge_qty,
                                "placed_cost_usd": _ne_temp_hedge_result.get("cost_usd", 0.0),
                                "placed_vwap": _ne_temp_hedge_result.get("vwap", 0.0),
                                "close_result": {},
                            }
                            print(
                                f"[TRADER] no_edge_unwind_failed label={opp.label} "
                                f"err={_ne_unwind_err} — falling through to forced hedge"
                            )
                            notify(
                                f"🔴🔴🔴 <b>INCIDENT: HEDGE NO EDGE — UNWIND FAILED → FORCED HEDGE</b>\n"
                                f"\n"
                                f"<b>{opp.label}</b>\n"
                                f"\n"
                                f"Predict fill: <b>{_ba_hedge_qty:.2f} sh</b> @ {_ba_actual_pred_bid:.2f}\n"
                                f"Poly moved: {_live_vwap_ba:.2f} → "
                                f"edge {_live_net_edge_ba * 10_000:.0f} bps\n"
                                f"Unwind failed: {(_ne_unwind_err or 'no fill')[:200]}\n"
                                f"\n"
                                f"Hedging on Polymarket to close exposure.\n"
                            )
                        else:
                            row["temp_hedge"] = {
                                "state": _ne_temp_hedge_state,
                                "placed_qty": _ne_temp_hedge_qty,
                                "placed_cost_usd": _ne_temp_hedge_result.get("cost_usd", 0.0),
                                "placed_vwap": _ne_temp_hedge_result.get("vwap", 0.0),
                                "close_result": {},
                            }
            # Hard cap: live poly VWAP exceeds POLY_MAX_HEDGE_PRICE
            _poly_max_hedge_price = float(os.environ.get("POLY_MAX_HEDGE_PRICE", "0.58") or "0.58")
            if _live_vwap_ba is not None and _live_vwap_ba >= _poly_max_hedge_price:
                # Unwind Predict position before declaring price_cap incident
                _pc_unwind_tick = float(os.environ.get("PREDICT_TICK_SIZE", "0.01") or "0.01")
                _pc_unwind_price = _predict_live_sell_price(
                    None, int(pred_leg.market_id), pred_leg.side,
                    fallback=_ba_actual_pred_bid, tick=_pc_unwind_tick,
                )
                _pc_unwind_result: dict[str, Any] = {}
                _pc_unwind_err: str | None = None
                _PC_UW_RETRIES = 3
                _PC_UW_DELAYS = [2.0, 5.0, 10.0]
                _pc_uw_total_sold = 0.0
                for _pc_attempt in range(_PC_UW_RETRIES):
                    if _pc_attempt > 0:
                        time.sleep(_PC_UW_DELAYS[_pc_attempt - 1])
                    _pc_uw_remaining = max(0.01, _ba_net_sell_qty - _pc_uw_total_sold)
                    if _pc_uw_remaining < 0.01:
                        break
                    _pc_unwind_result = {}
                    _pc_unwind_err = None
                    try:
                        _pc_unwind_result = _place_predict_limit_sell(
                            pred_leg,
                            sell_qty=_pc_uw_remaining,
                            sell_price=_pc_unwind_price,
                            fill_timeout_sec=60.0,
                            trace_id=trace_id,
                        )
                        _pc_uw_total_sold += _pc_unwind_result.get("filled_qty", 0.0)
                        if _pc_unwind_result.get("positions_seen_sec") is not None:
                            row["timing"]["positions_seen_sec"] = _pc_unwind_result["positions_seen_sec"]
                        if _pc_uw_total_sold >= _ba_net_sell_qty * 0.99:
                            break
                    except Exception as _pc_uw_e:
                        _pc_unwind_err = str(_pc_uw_e)
                        print(f"[TRADER][UNWIND_ERROR] price_cap attempt={_pc_attempt+1}/{_PC_UW_RETRIES} label={opp.label} err={_pc_uw_e}")
                        # insufficient_shares_balance = shares already gone → terminal, don't retry
                        if "insufficient" in _pc_unwind_err.lower():
                            break
                        _pc_is_lag = "400" in _pc_unwind_err
                        if not _pc_is_lag or _pc_attempt == _PC_UW_RETRIES - 1:
                            break
                _pc_uw_filled = _pc_uw_total_sold >= _ba_net_sell_qty * 0.99
                _pc_uw_qty = _pc_uw_total_sold

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
                if _pc_uw_filled:
                    _pc_uw_status = f"✅ sold {_pc_uw_qty:.2f} sh @ {_pc_unwind_price:.2f}"
                elif _pc_uw_qty > 0:
                    _pc_uw_status = (
                        f"⚠️ PARTIAL {_pc_uw_qty:.2f}/{_ba_hedge_qty:.2f} sh — "
                        f"left {_ba_hedge_qty - _pc_uw_qty:.2f} — check manually"
                    )
                else:
                    _pc_uw_status = "❌ not sold — check manually" + (f" err: {_pc_unwind_err}" if _pc_unwind_err else "")
                _em_pc = _incident_tg_emoji(_pc_uw_filled)
                notify(
                    f"{_em_pc} <b>INCIDENT: HEDGE PRICE CAP → UNWIND</b>\n"
                    f"\n"
                    f"<b>{opp.label}</b>\n"
                    f"\n"
                    f"Poly VWAP {_live_vwap_ba:.2f} ≥ cap {_poly_max_hedge_price:.2f} → hedge skipped\n"
                    f"Predict fill: <b>{_ba_hedge_qty:.2f} sh</b> @ {_ba_actual_pred_bid:.2f}\n"
                    f"\n"
                    f"Predict sell @ {_pc_unwind_price:.2f}: {_pc_uw_status}\n"
                    + (f"Error: {_pc_unwind_err}\n" if _pc_unwind_err and _pc_uw_qty == 0 else "")
                )
                _append_jsonl(trades_file, row)
                return {"status": "incident", "reason": "bid_ask_hedge_price_cap"}

            # Step 3: Exact-share limit BUY on Polymarket.
            # Use limit order (OrderArgs) with size=net_sell_qty so credited shares == predict net.
            # Price set to live_vwap × 1.02 (2% above) to guarantee immediate fill.
            # Unlike USD market orders, limit orders credit exactly `size` shares (no LP-fee deduction).

            # Pre-check: if Predict fill value < $1.00, unwind immediately — too small to hedge.
            _pred_fill_usd = _ba_net_sell_qty * _ba_actual_pred_bid
            _pred_min_fill_usd = float(os.environ.get("PREDICT_MIN_FILL_USD", "1.0") or "1.0")
            if _pred_fill_usd < _pred_min_fill_usd:
                _min_unwind_usd = 0.01
                _unwind_tick_pre = float(os.environ.get("PREDICT_TICK_SIZE", "0.01") or "0.01")
                _unwind_price_pre = _predict_live_sell_price(
                    None, int(pred_leg.market_id), pred_leg.side,
                    fallback=_ba_actual_pred_bid, tick=_unwind_tick_pre,
                )
                _unwind_pre_filled = False
                _unwind_pre_err: str | None = None
                _unwind_pre_qty = 0.0
                if _pred_fill_usd >= _min_unwind_usd:
                    _PRE_UW_RETRIES = 3
                    _PRE_UW_DELAYS = [2.0, 5.0, 10.0]
                    for _pre_attempt in range(_PRE_UW_RETRIES):
                        if _pre_attempt > 0:
                            time.sleep(_PRE_UW_DELAYS[_pre_attempt - 1])
                        _pre_uw_remaining = max(0.01, _ba_net_sell_qty - _unwind_pre_qty)
                        if _pre_uw_remaining < 0.01:
                            break
                        _unwind_pre_err = None
                        try:
                            _unwind_pre_result = _place_predict_limit_sell(
                                pred_leg,
                                sell_qty=_pre_uw_remaining,
                                sell_price=_unwind_price_pre,
                                fill_timeout_sec=60.0,
                                trace_id=trace_id,
                            )
                            _unwind_pre_qty += _unwind_pre_result.get("filled_qty", 0.0)
                            if _unwind_pre_result.get("positions_seen_sec") is not None:
                                row["timing"]["positions_seen_sec"] = _unwind_pre_result["positions_seen_sec"]
                            if _unwind_pre_qty >= _ba_net_sell_qty * 0.99:
                                break
                        except Exception as _upre_e:
                            _unwind_pre_err = str(_upre_e)
                            print(f"[TRADER][UNWIND_ERROR] fill_too_small attempt={_pre_attempt+1}/{_PRE_UW_RETRIES} label={opp.label} err={_upre_e}")
                            # insufficient_shares_balance = shares already gone → terminal, don't retry
                            if "insufficient" in _unwind_pre_err.lower():
                                break
                            _pre_is_lag = "400" in _unwind_pre_err
                            if not _pre_is_lag or _pre_attempt == _PRE_UW_RETRIES - 1:
                                break
                    _unwind_pre_filled = _unwind_pre_qty >= _ba_net_sell_qty * 0.99
                if _unwind_pre_filled:
                    _unwind_pre_status = f"✅ sold {_unwind_pre_qty:.2f} sh"
                elif _unwind_pre_qty > 0:
                    _unwind_pre_status = (
                        f"⚠️ PARTIAL {_unwind_pre_qty:.2f}/{_ba_net_sell_qty:.2f} sh — "
                        f"left {_ba_net_sell_qty - _unwind_pre_qty:.2f} — check manually"
                    )
                else:
                    _unwind_pre_status = f"❌ failed{(' — ' + _unwind_pre_err[:80]) if _unwind_pre_err else ''}"
                _unwind_pre_loss = (_unwind_price_pre - _ba_actual_pred_bid) * _ba_net_sell_qty
                _fts_resolved = bool(_unwind_pre_filled)
                _em_fts = _incident_tg_emoji(_fts_resolved)
                _inc_fts: dict[str, Any] = {
                    "ts": datetime.utcnow().isoformat() + "Z",
                    "type": "predict_fill_too_small",
                    "label": opp.label,
                    "pred_filled_shares": round(float(_ba_net_sell_qty), 6),
                    "pred_fill_usd": round(_pred_fill_usd, 4),
                    "pred_min_fill_usd": _pred_min_fill_usd,
                    "unwind_filled": _unwind_pre_filled,
                    "unwind_qty": round(_unwind_pre_qty, 6),
                    "unwind_error": _unwind_pre_err,
                    "approx_loss_usd": round(abs(_unwind_pre_loss), 6) if _fts_resolved else None,
                    "self_resolved": _fts_resolved,
                }
                _append_jsonl(incidents_file, _inc_fts)
                notify(
                    f"{_em_fts} <b>INCIDENT: PREDICT FILL TOO SMALL → UNWIND</b>\n"
                    f"\n"
                    f"<b>{opp.label}</b>\n"
                    f"\n"
                    f"Predict fill: <b>{_ba_net_sell_qty:.2f} sh</b> (${_pred_fill_usd:.2f})\n"
                    f"Below min hedge notional (min ${_pred_min_fill_usd:.2f})\n"
                    f"\n"
                    f"Predict sell @ {_unwind_price_pre:.2f}: {_unwind_pre_status}\n"
                    f"Loss ~${abs(_unwind_pre_loss):.3f}\n"
                )
                row["ok"] = False
                row["incident"] = {
                    "type": "predict_fill_too_small",
                    "pred_fill_usd": round(_pred_fill_usd, 4),
                    "pred_min_fill_usd": _pred_min_fill_usd,
                    "unwind_filled": _unwind_pre_filled,
                    "unwind_error": _unwind_pre_err,
                }
                row["summary"]["status"] = "incident"
                row["summary"]["reason_code"] = "predict_fill_too_small"
                print(
                    f"[TRADER][INCIDENT] PREDICT_FILL_TOO_SMALL label={opp.label} "
                    f"pred_filled={_ba_net_sell_qty:.4f} fill_usd=${_pred_fill_usd:.2f} "
                    f"min=${_pred_min_fill_usd:.2f} unwind={_unwind_pre_filled}"
                )
                if _market_id_int is not None:
                    with _predict_market_in_flight_lock:
                        _predict_market_in_flight.discard(_market_id_int)
                # Refresh cooldown from NOW (end of unwind), not from the fill detection time.
                # Without this, the 30s cooldown expires before unwind finishes → 2nd ghost fill.
                if pred_leg.market_id is not None:
                    _predict_market_last_buy_ts[int(pred_leg.market_id)] = time.time()
                _append_jsonl(trades_file, row)
                return {"status": "incident", "reason": "predict_fill_too_small"}

            _ba_hedge_vwap = _live_vwap_ba if _live_vwap_ba else float(poly_leg.ask)
            _ba_final_hedge_qty = _ba_net_sell_qty
            # Polymarket charges fee IN SHARES (taker asset), not in USDC.
            # takingAmount in API response = gross shares before fee deduction.
            # To receive exactly pred_fill net shares on Poly, we must order gross = net / (1 - fee_factor).
            # Actual Poly fee formula: feeRate * (1 - price) per share
            # (verified: at p=0.345 → 4.71%, at p=0.90 → 0.72%).
            # Note: p*(1-p) proxy used in edge-check is correct for USDC cost per net share;
            # (1-p) is the correct formula for SHARE deduction rate.
            _ba_taker_fee_factor = _ba_fee_rate * (1.0 - _ba_hedge_vwap)
            _ba_final_hedge_qty_gross = _ba_final_hedge_qty / max(1e-6, 1.0 - _ba_taker_fee_factor)
            _ba_hedge_cost_usd = _ba_final_hedge_qty_gross * _ba_hedge_vwap
            _poly_min_hedge = float(os.environ.get("POLY_MIN_ORDER_USD", "1.0") or "1.0")
            # Zone $0.80-$1.00 → over-hedge up to $1.00; below $0.80 → unwind on Predict
            _poly_over_hedge_threshold = float(os.environ.get("POLY_OVER_HEDGE_MIN_USD", "0.80") or "0.80")
            if _ba_hedge_cost_usd < _poly_min_hedge:
                if _ba_hedge_cost_usd >= _poly_over_hedge_threshold:
                    # ── Over-hedge: buy slightly more shares to meet Poly $1 minimum ──
                    _ba_hedge_qty_orig = _ba_final_hedge_qty_gross
                    _ba_final_hedge_qty = (_poly_min_hedge * 1.02) / _ba_hedge_vwap
                    # _ba_final_hedge_qty is already the gross target (USD / price); update gross var.
                    _ba_final_hedge_qty_gross = _ba_final_hedge_qty
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
                    _unwind_price = _predict_live_sell_price(
                        None, int(pred_leg.market_id), pred_leg.side,
                        fallback=_ba_actual_pred_bid,
                        tick=float(os.environ.get("PREDICT_TICK_SIZE", "0.01") or "0.01"),
                    )
                    _unwind_result: dict[str, Any] = {}
                    _unwind_err: str | None = None
                    _UW_RETRIES = 3
                    _UW_DELAYS = [2.0, 5.0, 10.0]
                    _uw_total_sold = 0.0
                    for _uw_attempt in range(_UW_RETRIES):
                        if _uw_attempt > 0:
                            time.sleep(_UW_DELAYS[_uw_attempt - 1])
                        _uw_remaining = max(0.01, _ba_net_sell_qty - _uw_total_sold)
                        if _uw_remaining < 0.01:
                            break
                        _unwind_result = {}
                        _unwind_err = None
                        try:
                            _unwind_result = _place_predict_limit_sell(
                                pred_leg,
                                sell_qty=_uw_remaining,
                                sell_price=_unwind_price,
                                fill_timeout_sec=60.0,
                                trace_id=trace_id,
                            )
                            _uw_total_sold += _unwind_result.get("filled_qty", 0.0)
                            if _unwind_result.get("positions_seen_sec") is not None:
                                row["timing"]["positions_seen_sec"] = _unwind_result["positions_seen_sec"]
                            if _uw_total_sold >= _ba_net_sell_qty * 0.99:
                                break
                        except Exception as _uw_e:
                            _unwind_err = str(_uw_e)
                            print(f"[TRADER][UNWIND_ERROR] below_min attempt={_uw_attempt+1}/{_UW_RETRIES} label={opp.label} err={_uw_e}")
                            # insufficient_shares_balance = shares already gone → terminal, don't retry
                            if "insufficient" in _unwind_err.lower():
                                break
                            _uw_is_lag = "400" in _unwind_err
                            if not _uw_is_lag or _uw_attempt == _UW_RETRIES - 1:
                                break

                    _uw_filled = _uw_total_sold >= _ba_net_sell_qty * 0.99
                    _uw_qty = _uw_total_sold
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
                    if _uw_filled:
                        _uw_status = f"✅ sold {_uw_qty:.2f} sh @ {_unwind_price:.2f}"
                    elif _uw_qty > 0:
                        _uw_status = (
                            f"⚠️ PARTIAL {_uw_qty:.2f}/{_ba_net_sell_qty:.2f} sh — "
                            f"left {_ba_net_sell_qty - _uw_qty:.2f} — check manually"
                        )
                    else:
                        _uw_status = "❌ not sold — check manually" + (f" err: {_unwind_err}" if _unwind_err else "")
                    _em_bm = _incident_tg_emoji(_uw_filled)
                    notify(
                        f"{_em_bm} <b>INCIDENT: HEDGE BELOW MIN → UNWIND</b>\n"
                        f"\n"
                        f"<b>{opp.label}</b>\n"
                        f"\n"
                        f"Predict fill: <b>{_ba_hedge_qty:.2f} sh</b> (${_ba_hedge_cost_usd:.2f} notional)\n"
                        f"Below Polymarket $ min (min ${_poly_min_hedge:.2f})\n"
                        f"\n"
                        f"Predict sell @ {_unwind_price:.2f}: {_uw_status}\n"
                        + (f"Loss ~${_uw_loss:.3f}\n" if _uw_filled else "")
                        + (f"Error: {_unwind_err}\n" if _unwind_err else "")
                    )
                    _append_jsonl(trades_file, row)
                    return {"status": "incident", "reason": "bid_ask_hedge_below_min_unwind"}

            _ba_hedge_price = _ba_hedge_vwap
            # Limit price: worst (highest) ask level touched in the VWAP sweep, rounded UP
            # to Poly's 0.001 tick. This guarantees all required price levels fill completely.
            # Fallback to VWAP × 1.02 if worst_price is unavailable.
            import math as _math
            _ba_worst_price = _live_worst_ba if _live_worst_ba else _ba_hedge_vwap * 1.02
            _ba_lp_mult = float(os.environ.get("POLY_BA_HEDGE_LIMIT_MULT", "1.02") or "1.02")
            _ba_limit_price = min(
                0.99, _math.ceil(_ba_worst_price * _ba_lp_mult * 1000) / 1000
            )
            # Poly CLOB SDK internally does round_down(size, 2) before signing the order.
            # Round gross qty UP to 2 decimal places so SDK preserves exactly our value.
            _ba_final_hedge_qty_gross = _math.ceil(_ba_final_hedge_qty_gross * 100) / 100
            _ba_hedge_leg = OpportunityLeg(
                **{**poly_leg.model_dump(), "shares": _ba_final_hedge_qty_gross, "stake_usd": _ba_final_hedge_qty_gross * _ba_hedge_price}
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
            _poly_ba_gtc = os.environ.get("POLY_BA_HEDGE_GTC_SEC", "").strip()
            _ba_gtc_to = float(_poly_ba_gtc) if _poly_ba_gtc else 25.0
            # Retry poly hedge on network errors — predict is already filled so hedge is critical
            _POLY_HEDGE_RETRIES = 3
            for _poly_attempt in range(_POLY_HEDGE_RETRIES):
                try:
                    polymarket_result_ba = _place_polymarket_limit_buy_exact_shares(
                        str(poly_leg.token_id),
                        shares=_ba_final_hedge_qty_gross,
                        price=_ba_limit_price,
                        private_key=_poly_pk,
                        funder=_poly_funder,
                        signature_type=_poly_sig_type,
                        poly_api_key=_poly_api_key,
                        poly_secret=_poly_secret,
                        poly_passphrase=_poly_passphrase,
                        fak_fallback=True,
                        gtc_fill_timeout_sec=_ba_gtc_to,
                    )
                    _poly_timing_ba["ack_ts"] = time.time()
                    poly_exec_error_ba = None
                    break
                except Exception as _e_ba_poly:
                    poly_exec_error_ba = _e_ba_poly
                    _err_s = str(_e_ba_poly)
                    # Retry on network errors AND on Poly 500 ("could not run the execution")
                    # 500 is often transient (matching engine hiccup), worth one retry
                    _is_network_err = (
                        "Request exception" in _err_s
                        or "ConnectionError" in _err_s
                        or "Timeout" in _err_s
                        or "status_code=None" in _err_s
                        or "status_code=500" in _err_s
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

            # CLOB may return poly_gtc_not_filled after cancel while the wallet already has CTF
            # shares (GTC fill vs cancel race). Like Predict (REST + BSC + WS): data-api + Polygon
            # CTF balance + optional newHead wait, then optional GTC gap top-up.
            if poly_exec_error_ba is not None and _ba_net_sell_qty > 0.01:
                _pr_need = float(_ba_net_sell_qty)
                _pr_lo = max(0.5, 0.95 * _pr_need)
                _pr_pos: tuple[float, float] | None = None
                _pr_sh = 0.0
                _pr_src_latest: dict[str, float | None] = {}
                for _pr_i in (0, 1):
                    if _pr_i:
                        time.sleep(2.0)
                    _g0 = _poly_head_gen_snapshot()
                    _pr_m, _pr_davg, _pr_src_latest = _poly_ba_reconcile_shares(
                        str(poly_leg.token_id), _poly_funder
                    )
                    _pr_sh = _pr_m
                    print(
                        f"[TRADER][POLY][RECONCILE_CHECK] pass={_pr_i + 1} max_sh={_pr_sh:.4f} "
                        f"need>={_pr_lo:.4f} data_api={_pr_src_latest.get('data_api')} "
                        f"polygon_ctf={_pr_src_latest.get('polygon_ctf')}"
                    )
                    if _pr_sh >= _pr_lo:
                        _pav = (
                            _pr_davg
                            if _pr_davg and _pr_davg > 0
                            else float(_ba_limit_price)
                        )
                        _pr_pos = (_pr_sh, _pav)
                        break
                    _poly_wait_new_head_or_timeout(_g0, 3.0)
                if _pr_sh < _pr_lo:
                    _gap = max(0.0, _pr_lo - _pr_sh)
                    _gap_sh = _math.ceil(_gap * 100) / 100
                    if _gap_sh >= 0.01:
                        try:
                            _g_book = _polymarket_book(str(poly_leg.token_id))
                            _g_vw = _vwap_and_worst_from_poly_book(_g_book, _gap_sh)
                            if _g_vw:
                                _g_vwap, _g_worst = _g_vw
                                _g_lp = min(0.99, _math.ceil(_g_worst * _ba_lp_mult * 1000) / 1000)
                                _place_polymarket_limit_buy_exact_shares(
                                    str(poly_leg.token_id),
                                    shares=_gap_sh,
                                    price=_g_lp,
                                    private_key=_poly_pk,
                                    funder=_poly_funder,
                                    signature_type=_poly_sig_type,
                                    poly_api_key=_poly_api_key,
                                    poly_secret=_poly_secret,
                                    poly_passphrase=_poly_passphrase,
                                    fak_fallback=True,
                                    gtc_fill_timeout_sec=_ba_gtc_to,
                                )
                                time.sleep(1.5)
                                _pr_m, _pr_davg, _pr_src_latest = _poly_ba_reconcile_shares(
                                    str(poly_leg.token_id), _poly_funder
                                )
                                _pr_sh = _pr_m
                                if _pr_sh >= _pr_lo:
                                    _pav = (
                                        _pr_davg
                                        if _pr_davg and _pr_davg > 0
                                        else float(_ba_limit_price)
                                    )
                                    _pr_pos = (_pr_sh, _pav)
                        except Exception as _gap_e:
                            print(
                                f"[TRADER][POLY][RECONCILE_GAP] label={opp.label} "
                                f"gap_sh={_gap_sh:.2f} err={_gap_e}"
                            )
                if _pr_sh >= _pr_lo and _pr_pos is not None:
                    _pavg = float(_pr_pos[1]) if len(_pr_pos) > 1 else float(_ba_limit_price)
                    _pavg = _pavg if _pavg > 0 else float(_ba_limit_price)
                    _pmk = _pr_sh * _pavg
                    print(
                        f"[TRADER][POLY][POSITION_RECONCILE] CLOB err was {poly_exec_error_ba!s} but "
                        f"visible sh={_pr_sh:.4f} (need>={_pr_lo:.4f}) — treating Poly leg as filled"
                    )
                    poly_exec_error_ba = None
                    polymarket_result_ba = {
                        "token_id": str(poly_leg.token_id),
                        "shares_requested": _ba_final_hedge_qty_gross,
                        "price": _pavg,
                        "response": {
                            "success": True,
                            "status": "matched",
                            "takingAmount": str(_pr_sh),
                            "makingAmount": str(_pmk),
                            "transactionsHashes": [],
                        },
                        "order_type": "POSITION_RECONCILE",
                        "filled": True,
                    }
                    row["polymarket"] = polymarket_result_ba
                    row["polymarket_position_reconciled"] = True
                    row["polymarket_reconcile_sources"] = dict(_pr_src_latest)

            _ba_poly_resp = (polymarket_result_ba.get("response") or {}) if polymarket_result_ba else {}
            _ba_poly_txhashes = _ba_poly_resp.get("transactionsHashes") or []
            _ba_poly_resp_status = (_ba_poly_resp.get("status") or "").lower()
            _ba_poly_size_matched = float(_ba_poly_resp.get("size_matched") or 0)
            # Success: normal matched response (success+txhashes) OR post-cancel auth check
            # (status=matched/filled or size_matched>0 from get_order) OR synthetic trade-history response
            poly_filled_ba = (
                (_ba_poly_resp.get("success") is True and bool(_ba_poly_txhashes))
                or _ba_poly_resp_status in ("matched", "filled")
                or _ba_poly_size_matched > 0
            )
            _ba_poly_qty: float = 0.0
            try:
                _ba_poly_qty = float(_ba_poly_resp.get("takingAmount") or 0)
            except (ValueError, TypeError):
                pass
            # size_matched fallback: used when GTC order was verified via get_order() after
            # cancel (auth_order path) — that response has size_matched but no takingAmount.
            if _ba_poly_qty == 0 and _ba_poly_size_matched > 0:
                _ba_poly_qty = _ba_poly_size_matched
            # takingAmount = gross shares BEFORE Poly fee deduction from shares.
            # Net shares actually credited = gross * (1 - feeRate * max(fill_price, 1-fill_price)).
            _ba_poly_qty_actual = _ba_poly_qty
            try:
                _ba_poly_making_val = float(_ba_poly_resp.get("makingAmount") or 0)
                if _ba_poly_making_val > 0 and _ba_poly_qty > 0:
                    _ba_poly_fill_price = _ba_poly_making_val / _ba_poly_qty
                    _ba_poly_net_fee_factor = _ba_fee_rate * (1.0 - _ba_poly_fill_price)
                    _ba_poly_qty_actual = _ba_poly_qty * (1.0 - _ba_poly_net_fee_factor)
            except Exception:
                pass
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
                _ba_poly_making = 0.0  # actual USDC paid to Poly (set inside try below)
                try:
                    _ba_poly_making = float(_ba_poly_resp.get("makingAmount") or 0)
                    _ba_poly_taking = float(_ba_poly_resp.get("takingAmount") or 0)
                    if _ba_poly_making > 0 and _ba_poly_taking > 0:
                        _ba_poly_price = _ba_poly_making / _ba_poly_taking
                except Exception:
                    pass
                _ba_pred_fee_paid = _ba_pred_fee_bps / 10_000 * _ba_pred_price * _ba_hedge_qty
                # P&L: actual cash flows.
                # USDC paid to Poly = makingAmount (gross shares × price, includes share-fee overhead).
                # USDC paid/received for Predict DOWN = hedge_qty × pred_price.
                # At resolution: _ba_poly_qty_actual NET UP shares pay out $1 each.
                _ba_poly_usdc = _ba_poly_making if _ba_poly_making > 0 else _ba_poly_qty_actual * _ba_poly_price
                _ba_net_pnl = _ba_poly_qty_actual - _ba_poly_usdc - _ba_hedge_qty * _ba_pred_price - _ba_pred_fee_paid
                _trade_pnl_log.append((time.time(), _ba_net_pnl))
                # Store authoritative net_pnl so downstream code doesn't need to recalculate
                row["net_pnl"] = round(_ba_net_pnl, 6)
                # Update live_hedge_recheck with actual executed prices so stored PnL is accurate
                # Fee rate as share deduction fraction: feeRate * (1-price)
                _actual_poly_fee = _ba_fee_rate * (1.0 - _ba_poly_price)
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

                # Mismatch correction: if Poly partially filled fewer shares than Predict,
                # sell the excess Predict shares back to close the unhedged exposure.
                _ba_mismatch_shares = _ba_net_sell_qty - _ba_poly_qty_actual
                _ba_mismatch_threshold = 0.01
                _ba_mismatch_corrected = False
                _ba_mismatch_sell_result: dict = {}
                _ba_mismatch_sell_err: str | None = None
                _mm_poly_bought = False
                _mm_poly_price: float | None = None

                if _ba_mismatch_shares > _ba_mismatch_threshold:
                    # First try: buy the missing shares on Poly at current market price.
                    # Cheaper than selling excess on Predict (avoids bid-ask spread loss).
                    _mm_action = "sell_predict"
                    try:
                        _mm_book = _polymarket_book(str(poly_leg.token_id))
                        _mm_vwap_worst = _vwap_and_worst_from_poly_book(_mm_book, _ba_mismatch_shares)
                        if _mm_vwap_worst:
                            _mm_vwap, _mm_worst = _mm_vwap_worst
                            # Only buy if arb edge still holds AND price hasn't drifted too far
                            _mm_edge = 1.0 - _ba_actual_pred_bid - _mm_vwap - _ba_fee_rate * _mm_vwap * (1.0 - _mm_vwap)
                            _mm_price_drift = _mm_vwap - _ba_poly_price
                            _mm_max_drift = float(os.environ.get("MISMATCH_MAX_POLY_DRIFT", "0.1") or "0.1")
                            if _mm_edge > 0 and _mm_price_drift <= _mm_max_drift:
                                _mm_limit = min(0.99, _math.ceil(_mm_worst * 1000) / 1000)
                                _mm_poly_result = _place_polymarket_limit_buy_exact_shares(
                                    str(poly_leg.token_id),
                                    shares=_ba_mismatch_shares,
                                    price=_mm_limit,
                                    private_key=_poly_pk,
                                    funder=_poly_funder,
                                    signature_type=_poly_sig_type,
                                    poly_api_key=_poly_api_key,
                                    poly_secret=_poly_secret,
                                    poly_passphrase=_poly_passphrase,
                                )
                                _mm_poly_bought = _mm_poly_result.get("filled", False)
                                _mm_poly_price = _mm_vwap
                                _mm_action = "buy_poly"
                    except Exception as _mm_poly_e:
                        print(f"[TRADER][MISMATCH] poly_rebuy failed: {_mm_poly_e}")
                        # The request may have been received by Polymarket before the timeout
                        # (classic "sent but response lost"). Re-read actual Poly balance before
                        # deciding the rebuy failed — avoids selling excess Predict when Poly
                        # actually executed.
                        try:
                            time.sleep(2.5)
                            _mm_recheck_sh, _, _ = _poly_ba_reconcile_shares(
                                str(poly_leg.token_id), _poly_funder
                            )
                            _mm_expected = _ba_poly_qty_actual + _ba_mismatch_shares
                            if _mm_recheck_sh >= _mm_expected * 0.95:
                                _mm_poly_bought = True
                                _mm_poly_price = _mm_poly_price or _ba_poly_price
                                _mm_action = "buy_poly"
                                print(
                                    f"[TRADER][MISMATCH] rebuy_timeout_but_reconcile_ok "
                                    f"expected={_mm_expected:.4f} actual={_mm_recheck_sh:.4f} "
                                    f"— treating rebuy as filled"
                                )
                            else:
                                print(
                                    f"[TRADER][MISMATCH] rebuy_timeout_reconcile_fail "
                                    f"expected={_mm_expected:.4f} actual={_mm_recheck_sh:.4f} "
                                    f"err={_mm_poly_e}"
                                )
                        except Exception as _mm_rc_e:
                            print(f"[TRADER][MISMATCH] rebuy_recheck_failed err={_mm_rc_e}")

                    if _mm_poly_bought:
                        _mm_status = f"✅ bought {_ba_mismatch_shares:.3f} shares @ {_mm_poly_price:.2f} on Poly"
                        notify(
                            f"⚠️ <b>POLY PARTIAL FILL — rebuying on Poly</b>\n"
                            f"<i>Poly filled {_ba_poly_qty_actual:.3f} of {_ba_net_sell_qty:.3f} shares</i>\n"
                            f"\n"
                            f"- Excess: <b>{_ba_mismatch_shares:.3f} shares</b>\n"
                            f"- {_mm_status}\n"
                        )
                        _ba_mismatch_corrected = True
                    else:
                        # Fallback: sell excess on Predict.
                        # If the remaining Predict position after selling the excess would be
                        # below the minimum viable size, sell ALL shares and also unwind Poly.
                        _mm_tick = float(os.environ.get("PREDICT_TICK_SIZE", "0.01") or "0.01")
                        _mm_unwind_price = _predict_live_sell_price(
                            None, int(pred_leg.market_id), pred_leg.side,
                            fallback=_ba_actual_pred_bid, tick=_mm_tick,
                        )
                        _mm_min_usd = float(os.environ.get("PREDICT_MIN_FILL_USD", "1.0") or "1.0")
                        _mm_remaining_usd = (_ba_hedge_qty - _ba_mismatch_shares) * _ba_pred_price
                        _mm_sell_all = _mm_remaining_usd < _mm_min_usd
                        _mm_sell_qty = _ba_hedge_qty if _mm_sell_all else _ba_mismatch_shares
                        try:
                            _ba_mismatch_sell_result = _place_predict_limit_sell(
                                pred_leg,
                                sell_qty=_mm_sell_qty,
                                sell_price=_mm_unwind_price,
                                fill_timeout_sec=60.0,
                                trace_id=trace_id,
                            )
                            _ba_mismatch_corrected = _ba_mismatch_sell_result.get("filled", False)
                            if _ba_mismatch_sell_result.get("positions_seen_sec") is not None:
                                row["timing"]["positions_seen_sec"] = _ba_mismatch_sell_result["positions_seen_sec"]
                        except Exception as _mm_e:
                            _ba_mismatch_sell_err = str(_mm_e)

                        # If we sold ALL Predict shares, also sell the Poly position
                        _mm_poly_sell_qty = 0.0
                        _mm_poly_sell_err: str | None = None
                        _mm_poly_sell_status = ""
                        if _mm_sell_all and _ba_poly_qty_actual >= 0.01:
                            try:
                                _mm_pb = _polymarket_book(str(poly_leg.token_id))
                                _mm_pb_bids = _mm_pb.get("bids") or []
                                _mm_pb_bid = float(_mm_pb_bids[0]["price"]) if _mm_pb_bids else 0.0
                                if _mm_pb_bid > 0.01:
                                    import math as _mm_math
                                    _mm_poly_sell_price = max(0.01, _mm_math.floor(_mm_pb_bid * 1000) / 1000)
                                    _mm_gp_pk = _normalize_hex_key(os.environ.get("POLY_PRIVATE_KEY", ""))
                                    _mm_gp_funder = os.environ.get("POLY_FUNDER", "").strip()
                                    _mm_gp_sig = int(os.environ.get("POLY_SIGNATURE_TYPE", "0").strip() or "0")
                                    _mm_gp_ak = os.environ.get("POLY_API_KEY", "").strip()
                                    _mm_gp_sec = os.environ.get("POLY_SECRET", "").strip()
                                    _mm_gp_pp = os.environ.get("POLY_PASSPHRASE", "").strip()
                                    from py_clob_client.order_builder.constants import SELL as _MM_SELL
                                    _mm_gp_cli = ClobClient("https://clob.polymarket.com", chain_id=137,
                                                             key=_mm_gp_pk, signature_type=_mm_gp_sig, funder=_mm_gp_funder)
                                    if _mm_gp_ak and _mm_gp_sec and _mm_gp_pp:
                                        _mm_gp_cli.set_api_creds(ApiCreds(api_key=_mm_gp_ak, api_secret=_mm_gp_sec, api_passphrase=_mm_gp_pp))
                                    else:
                                        _mm_gp_cli.set_api_creds(_mm_gp_cli.create_or_derive_api_creds())
                                    _mm_gp_ord = _mm_gp_cli.create_order(OrderArgs(
                                        token_id=str(poly_leg.token_id),
                                        price=_mm_poly_sell_price,
                                        size=round(_ba_poly_qty_actual, 4),
                                        side=_MM_SELL,
                                    ))
                                    _mm_gp_resp = _mm_gp_cli.post_order(_mm_gp_ord, OrderType.GTC)
                                    _mm_gp_status = (_mm_gp_resp.get("status") or "").lower()
                                    if _mm_gp_status in ("matched", "filled") or _mm_gp_resp.get("transactionsHashes"):
                                        _mm_poly_sell_qty = _ba_poly_qty_actual
                                    _mm_poly_sell_status = (
                                        f"✅ sold {_mm_poly_sell_qty:.3f} @ {_mm_poly_sell_price:.2f}"
                                        if _mm_poly_sell_qty > 0
                                        else f"❌ status={_mm_gp_status}"
                                    )
                                    print(
                                        f"[TRADER][MISMATCH_FULL_UNWIND] poly sell "
                                        f"qty={_ba_poly_qty_actual:.4f} price={_mm_poly_sell_price:.4f} "
                                        f"status={_mm_gp_status}"
                                    )
                            except Exception as _mm_gp_e:
                                _mm_poly_sell_err = str(_mm_gp_e)
                                _mm_poly_sell_status = f"❌ err: {str(_mm_gp_e)[:100]}"
                                print(f"[TRADER][MISMATCH_FULL_UNWIND] poly sell error: {_mm_gp_e}")

                        _mm_pred_sold_qty = _ba_mismatch_sell_result.get("filled_qty", 0.0) if _ba_mismatch_corrected else 0.0
                        _mm_status = "✅ sold" if _ba_mismatch_corrected else f"❌ failed{(' — ' + _ba_mismatch_sell_err[:100]) if _ba_mismatch_sell_err else ''}"
                        if _mm_sell_all:
                            _mm_poly_line = f"\n- Poly sell {_ba_poly_qty_actual:.3f} sh: {_mm_poly_sell_status}" if _mm_poly_sell_status else ""
                            notify(
                                f"⚠️ <b>POLY PARTIAL FILL — full unwind</b>\n"
                                f"<i>Poly filled {_ba_poly_qty_actual:.3f} of {_ba_net_sell_qty:.3f} shares</i>\n"
                                f"<i>Remainder ${_mm_remaining_usd:.2f} &lt; min ${_mm_min_usd:.2f} → selling all</i>\n"
                                f"\n"
                                f"- Predict sell {_mm_sell_qty:.3f} sh @ {_mm_unwind_price:.2f}: {_mm_status}"
                                f"{_mm_poly_line}\n"
                            )
                        else:
                            notify(
                                f"⚠️ <b>POLY PARTIAL FILL — excess Predict sold</b>\n"
                                f"<i>Poly filled {_ba_poly_qty_actual:.3f} of {_ba_net_sell_qty:.3f} shares</i>\n"
                                f"\n"
                                f"- Excess: <b>{_ba_mismatch_shares:.3f} shares</b>\n"
                                f"- Predict sell @ {_mm_unwind_price:.2f}: {_mm_status}\n"
                            )
                    print(
                        f"[TRADER][MISMATCH] label={opp.label} "
                        f"pred={_ba_net_sell_qty:.4f} poly={_ba_poly_qty_actual:.4f} "
                        f"excess={_ba_mismatch_shares:.4f} action={_mm_action} "
                        f"drift={locals().get('_mm_price_drift', 0):.4f} "
                        f"corrected={_ba_mismatch_corrected} err={_ba_mismatch_sell_err}"
                    )

                row["mismatch_correction"] = {
                    "pred_net_qty": round(_ba_net_sell_qty, 6),
                    "poly_filled_qty_actual": round(_ba_poly_qty_actual, 6),
                    "poly_filled_qty_gross": round(_ba_poly_qty, 6),
                    "mismatch_shares": round(_ba_mismatch_shares, 6),
                    "corrected": _ba_mismatch_corrected,
                    "action": locals().get("_mm_action", "none"),
                    "sell_all": locals().get("_mm_sell_all", False),
                    "sell_qty": locals().get("_mm_sell_qty", _ba_mismatch_shares),
                    "sell_result": _ba_mismatch_sell_result,
                    "sell_error": _ba_mismatch_sell_err,
                    "poly_sell_qty": locals().get("_mm_poly_sell_qty", 0.0),
                    "poly_sell_err": locals().get("_mm_poly_sell_err", None),
                }

                # Recompute PnL to reflect actual cash flows after mismatch correction.
                # The initial _ba_net_pnl was calculated before correction and used the full
                # Predict qty (_ba_hedge_qty) without subtracting sell-back proceeds.
                if _ba_mismatch_shares > _ba_mismatch_threshold and _ba_mismatch_corrected:
                    _mm_action_used = locals().get("_mm_action", "none")
                    _mm_sell_all_used = locals().get("_mm_sell_all", False)
                    if _mm_action_used == "sell_predict":
                        _mm_sell_price_used = locals().get("_mm_unwind_price", 0.0)
                        _mm_sell_qty_used = locals().get("_mm_sell_qty", _ba_mismatch_shares)
                        _mm_proceeds = _mm_sell_qty_used * _mm_sell_price_used
                        _mm_poly_sold_back = locals().get("_mm_poly_sell_qty", 0.0)
                        _mm_poly_sell_p = locals().get("_mm_poly_sell_price", 0.0)
                        _mm_poly_proceeds = _mm_poly_sold_back * _mm_poly_sell_p
                        if _mm_sell_all_used:
                            # Sold all Predict + Poly: net PnL is purely the cash flows
                            # (no remaining position to resolve)
                            _ba_net_pnl = (
                                _mm_proceeds
                                + _mm_poly_proceeds
                                - _ba_poly_usdc
                                - _ba_hedge_qty * _ba_pred_price
                                - _ba_pred_fee_paid
                            )
                        else:
                            # Sold only excess Predict shares
                            _ba_net_pnl = (
                                _ba_poly_qty_actual
                                - _ba_poly_usdc
                                - _ba_hedge_qty * _ba_pred_price
                                + _mm_proceeds
                                - _ba_pred_fee_paid
                            )
                        row["net_pnl"] = round(_ba_net_pnl, 6)
                    elif _mm_action_used == "buy_poly":
                        # Additional cost from rebuying missing Poly shares
                        _mm_rebuy_price = locals().get("_mm_poly_price", 0.0)
                        _ba_net_pnl = (
                            _ba_hedge_qty
                            - _ba_poly_usdc
                            - _ba_mismatch_shares * _mm_rebuy_price
                            - _ba_hedge_qty * _ba_pred_price
                            - _ba_pred_fee_paid
                        )
                    row["net_pnl"] = round(_ba_net_pnl, 6)

                _append_jsonl(trades_file, row)
                _append_jsonl(success_trades_file, row)

                # Effective quantities for notification: adjust for mismatch correction (no extra lines in TG)
                _notif_poly_qty = _ba_poly_qty_actual  # net shares after Poly fee (≈gross at high prices)
                _notif_pred_qty = _ba_hedge_qty
                if _ba_mismatch_shares > _ba_mismatch_threshold and _ba_mismatch_corrected:
                    _mm_action_final = locals().get("_mm_action", "sell_predict")
                    _mm_poly_bought_final = locals().get("_mm_poly_bought", False)
                    _mm_sell_all_final = locals().get("_mm_sell_all", False)
                    if _mm_action_final == "buy_poly" and _mm_poly_bought_final:
                        _notif_poly_qty = _ba_poly_qty_actual + _ba_mismatch_shares
                    elif _mm_sell_all_final:
                        _notif_pred_qty = 0.0
                        _notif_poly_qty = 0.0
                    else:
                        _notif_pred_qty = _ba_hedge_qty - _ba_mismatch_shares

                _tkey = str(poly_leg.token_id)
                _prev = _ba_fill_state.get(_tkey)
                _GROUP_TTL = 1800  # 30 min window to group fills for same market
                _is_grouped = _prev is not None and (time.time() - _prev[3]) < _GROUP_TTL
                _reply_to_id = _prev[0] if _is_grouped else None
                _cum_pnl = (_prev[1] + _ba_net_pnl) if _is_grouped else _ba_net_pnl
                _fill_n = (_prev[2] + 1) if _is_grouped else 1

                # ROI relative to total stake
                _total_stake = _ba_poly_usdc + _ba_hedge_qty * _ba_pred_price  # actual USDC invested
                _roi_pct = (_ba_net_pnl / _total_stake * 100) if _total_stake > 0 else 0.0

                # Market title from legs
                _mkt_title = ""
                for _leg in (row.get("legs") or []):
                    if _leg.get("title"):
                        _mkt_title = _leg["title"]
                        break

                if _ba_net_pnl >= 0:
                    _pnl_suffix = " — in the green"
                else:
                    _pnl_suffix = " — in the red"
                _pnl_emoji = "📈" if _ba_net_pnl >= 0 else "📉"
                # Recalculate ROI based on corrected PnL and actual net stake
                _net_pred_cost = _notif_pred_qty * _ba_pred_price
                _net_poly_cost = _notif_poly_qty * _ba_poly_price
                _total_stake_corrected = _net_poly_cost + _net_pred_cost
                _roi_pct = (_ba_net_pnl / _total_stake_corrected * 100) if _total_stake_corrected > 0 else 0.0
                _cum_line = f"<i>total ×{_fill_n}: {_cum_pnl:+.2f}$</i>\n" if _fill_n > 1 else ""
                _title = f"🟢🟢🟢 <b>HEDGE FILLED ×{_fill_n}</b>" if _fill_n > 1 else "🟢🟢🟢 <b>HEDGE FILLED</b>"

                # Forensics in docker logs only (not Telegram): match Poly/Predict UI + trades.jsonl
                try:
                    _nrep = len(_ba_pred_resp.get("replaced_order_hashes") or [])
                    _rfs = float(_ba_quote_meta.get("replaced_filled_shares") or 0.0)
                    _oh = str(pred_hash_ba) if pred_hash_ba else "n/a"
                    print(
                        f"[TRADER][BA_HEDGE_LOG] mkt_id={getattr(pred_leg, 'market_id', None)} "
                        f"label={opp.label!r} final_order={_oh[:20]}... "
                        f"pred_sh={_notif_pred_qty:.4f} poly_sh={_notif_poly_qty:.4f} "
                        f"replaced_orders={_nrep} replaced_filled_sh={_rfs:.4f}"
                    )
                except Exception:
                    pass

                _msg_id = notify(
                    f"{_title}\n"
                    + (f"<i>{_mkt_title}</i>\n" if _mkt_title else "")
                    + f"\n"
                    f"<b>{opp.label}</b>\n"
                    f"\n"
                    f"<b>Polymarket</b>  {poly_leg.side.upper()}\n"
                    f"  {_notif_poly_qty:.3f} shares  @  <code>{_ba_poly_price:.2f}</code>  =  <b>${_net_poly_cost:.2f}</b>\n"
                    f"<b>Predict</b>  {pred_leg.side.upper()}\n"
                    f"  {_notif_pred_qty:.3f} shares  @  <code>{_ba_pred_price:.2f}</code>  =  <b>${_net_pred_cost:.2f}</b>\n"
                    + f"\n"
                    f"{_pnl_emoji} <b>{_ba_net_pnl:+.2f}$</b>  ({_roi_pct:+.2f}%){_pnl_suffix}\n"
                    + _cum_line
                    + f"\n"
                    f"<i>⏱ fill={_ba_quote_meta.get('time_to_first_fill_ms', 0)/1000:.1f}s  unhedged={_ba_unhedged_sec:.1f}s  total={_ba_total_sec:.1f}s</i>\n",
                    reply_to_message_id=_reply_to_id,
                )
                # Second CLOB order (mismatch top-up) → reply in thread so 2nd buy is visible
                if _mm_poly_bought and _msg_id is not None and _ba_mismatch_shares > _ba_mismatch_threshold:
                    _mm_p2 = _mm_poly_price if _mm_poly_price is not None else _ba_poly_price
                    notify(
                        f"➕ <b>Polymarket top-up (2nd order)</b>\n"
                        f"+{_ba_mismatch_shares:.3f} sh  @  <code>{_mm_p2:.2f}</code>\n"
                        f"<i>First hedge was short vs Predict — 2nd buy to match</i>\n",
                        reply_to_message_id=_msg_id,
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
                _unwind_sell_price = _predict_live_sell_price(
                    None, int(pred_leg.market_id), pred_leg.side,
                    fallback=_ba_actual_pred_bid, tick=_unwind_tick,
                )
                _uw2_result: dict[str, Any] = {}
                _uw2_err: str | None = None
                _uw2_total_sold = 0.0

                # Balance index wait is inside _place_predict_limit_sell (all Predict sells).

                # Now attempt the sell — up to 3 retries in case of transient errors
                # (not balance lag, since we already waited for it above).
                _UW2_RETRIES = 3
                for _uw2_attempt in range(_UW2_RETRIES):
                    if _uw2_attempt > 0:
                        time.sleep(3.0)
                    _uw2_remaining = max(0.01, _ba_net_sell_qty - _uw2_total_sold)
                    if _uw2_remaining < 0.01:
                        break
                    _uw2_result = {}
                    _uw2_err = None
                    try:
                        _uw2_result = _place_predict_limit_sell(
                            pred_leg,
                            sell_qty=_uw2_remaining,
                            sell_price=_unwind_sell_price,
                            fill_timeout_sec=60.0,
                            trace_id=trace_id,
                        )
                        _uw2_total_sold += _uw2_result.get("filled_qty", 0.0)
                        if _uw2_result.get("positions_seen_sec") is not None:
                            row["timing"]["positions_seen_sec"] = _uw2_result["positions_seen_sec"]
                        if _uw2_total_sold >= _ba_net_sell_qty * 0.99:
                            break
                    except Exception as _uw2_e:
                        _uw2_err = str(_uw2_e)
                        print(
                            f"[TRADER][UNWIND_ERROR] unhedged_predict unwind "
                            f"attempt={_uw2_attempt+1}/{_UW2_RETRIES} err={_uw2_e}"
                        )
                        # On balance lag (still not indexed) keep retrying; other errors → stop
                        # insufficient_shares_balance = shares already gone → terminal, don't retry
                        if "insufficient" in _uw2_err.lower():
                            break
                        _is_balance_lag = "400" in _uw2_err
                        if not _is_balance_lag:
                            break

                _uw2_filled = _uw2_total_sold >= _ba_net_sell_qty * 0.99
                _uw2_qty = _uw2_total_sold

                # ── Ghost Poly fill check ──
                # Even though polymarket_result_ba showed 0 shares, a GTC order may have
                # matched on-chain after we declared failure (race between cancel and fill).
                # Check actual Poly position: if we own shares we didn't account for → sell them.
                _ghost_poly_sold = 0.0
                _ghost_poly_sell_err: str | None = None
                _ghost_poly_price: float | None = None
                _gp_shares_seen = 0.0
                _gp_threshold_un = 0.5
                try:
                    _gp_pos = _fetch_poly_position(str(poly_leg.token_id), timeout=4.0)
                    _gp_shares = float(_gp_pos[0]) if _gp_pos else 0.0
                    _gp_shares_seen = _gp_shares
                    # Only act if we see meaningful shares that weren't reported filled
                    if _gp_shares >= _gp_threshold_un:
                        print(
                            f"[TRADER][GHOST_POLY] detected position on Poly "
                            f"token={str(poly_leg.token_id)[:16]} shares={_gp_shares:.4f} "
                            f"— selling back"
                        )
                        # Sell on Poly: place GTC SELL limit at bid (taker, fills immediately)
                        _gp_book = _polymarket_book(str(poly_leg.token_id))
                        _gp_bids = _gp_book.get("bids") or []
                        _gp_best_bid = float(_gp_bids[0]["price"]) if _gp_bids else 0.0
                        if _gp_best_bid > 0.01:
                            import math as _gp_math
                            _gp_sell_price = max(0.01, _gp_math.floor(_gp_best_bid * 1000) / 1000)
                            _ghost_poly_price = _gp_sell_price
                            _gp_pk = _normalize_hex_key(os.environ.get("POLY_PRIVATE_KEY", ""))
                            _gp_funder = os.environ.get("POLY_FUNDER", "").strip()
                            _gp_sig_type = int(os.environ.get("POLY_SIGNATURE_TYPE", "0").strip() or "0")
                            _gp_api_key = os.environ.get("POLY_API_KEY", "").strip()
                            _gp_secret = os.environ.get("POLY_SECRET", "").strip()
                            _gp_pass = os.environ.get("POLY_PASSPHRASE", "").strip()
                            from py_clob_client.order_builder.constants import SELL as _POLY_SELL
                            _gp_client = ClobClient(
                                "https://clob.polymarket.com",
                                chain_id=137,
                                key=_gp_pk,
                                signature_type=_gp_sig_type,
                                funder=_gp_funder,
                            )
                            if _gp_api_key and _gp_secret and _gp_pass:
                                _gp_client.set_api_creds(ApiCreds(api_key=_gp_api_key, api_secret=_gp_secret, api_passphrase=_gp_pass))
                            else:
                                _gp_client.set_api_creds(_gp_client.create_or_derive_api_creds())
                            _gp_order = _gp_client.create_order(OrderArgs(
                                token_id=str(poly_leg.token_id),
                                price=_gp_sell_price,
                                size=round(_gp_shares, 4),
                                side=_POLY_SELL,
                            ))
                            _gp_resp = _gp_client.post_order(_gp_order, OrderType.GTC)
                            _gp_status = (_gp_resp.get("status") or "").lower()
                            if _gp_status in ("matched", "filled") or _gp_resp.get("transactionsHashes"):
                                _ghost_poly_sold = _gp_shares
                            print(
                                f"[TRADER][GHOST_POLY] sell result status={_gp_status} "
                                f"shares={_gp_shares:.4f} price={_gp_sell_price:.4f} "
                                f"sold={_ghost_poly_sold:.4f}"
                            )
                except Exception as _gp_e:
                    _ghost_poly_sell_err = str(_gp_e)
                    print(f"[TRADER][GHOST_POLY] check/sell error: {_gp_e}")

                if _uw2_filled:
                    _uw2_status = f"✅ sold {_uw2_qty:.2f} sh @ {_unwind_sell_price:.2f}"
                elif _uw2_qty > 0:
                    _uw2_remaining_qty = _ba_net_sell_qty - _uw2_qty
                    _uw2_status = (
                        f"⚠️ PARTIAL {_uw2_qty:.2f}/{_ba_net_sell_qty:.2f} sh — "
                        f"left {_uw2_remaining_qty:.2f} sh — check manually"
                        + (f" err: {_uw2_err[:200]}" if _uw2_err else "")
                    )
                else:
                    _uw2_status = f"❌ failed — check manually{(' err: ' + _uw2_err[:200]) if _uw2_err else ''}"
                _gp_tg = ""
                if _ghost_poly_sold > 0:
                    _gp_tg = f"\nGhost Poly: ✅ sold {_ghost_poly_sold:.2f} sh @ {(_ghost_poly_price or 0):.2f}"
                elif _ghost_poly_sell_err:
                    _gp_tg = f"\nGhost Poly: ❌ err: {_ghost_poly_sell_err[:200]}"
                _poly_ghost_resolved = bool(
                    (_gp_shares_seen < _gp_threshold_un and not _ghost_poly_sell_err)
                    or (
                        _gp_shares_seen >= _gp_threshold_un
                        and not _ghost_poly_sell_err
                        and _ghost_poly_sold > 0
                        and _ghost_poly_sold >= 0.99 * _gp_shares_seen
                    )
                )
                _ug_tg_resolved = bool(_uw2_filled and _poly_ghost_resolved)
                _em_ug = _incident_tg_emoji(_ug_tg_resolved)
                _ug_sub = " (auto-resolved)" if _ug_tg_resolved else " — action required"
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
                    "unwind_filled": _uw2_filled,
                    "unwind_qty": _uw2_qty,
                    "ghost_poly_shares_seen": round(_gp_shares_seen, 6),
                    "ghost_poly_sold": _ghost_poly_sold,
                    "ghost_poly_sell_err": _ghost_poly_sell_err,
                    "ghost_poly_price": _ghost_poly_price,
                    "self_resolved": _ug_tg_resolved,
                    "quote_meta": _ba_quote_meta,
                }
                _append_jsonl(incidents_file, _ba_inc2)
                row["ok"] = False
                row["summary"]["status"] = "incident"
                row["summary"]["reason_code"] = "bid_ask_unhedged_predict"
                print(
                    f"[TRADER][INCIDENT] BID_ASK_UNHEDGED_PREDICT label={opp.label} "
                    f"pred_qty={_ba_hedge_qty:.6f} poly_err={poly_exec_error_ba} "
                    f"residual={_ba_residual:.6f} unwind_filled={_uw2_filled} unwind_qty={_uw2_qty:.4f} "
                    f"ghost_poly_sold={_ghost_poly_sold:.4f} self_resolved={_ug_tg_resolved}"
                )
                notify(
                    f"{_em_ug} <b>INCIDENT: UNHEDGED PREDICT</b>{_ug_sub}\n"
                    f"\n"
                    f"<b>{opp.label}</b>\n"
                    f"\n"
                    f"Predict ({pred_leg.side.upper()} BID)\n"
                    f"price: {_ba_actual_pred_bid:.2f} - stake: ${_ba_hedge_qty * _ba_actual_pred_bid:.2f} - shares: {_ba_hedge_qty:.2f}\n"
                    f"Polymarket ({poly_leg.side.upper()} ASK) ❌\n"
                    f"price: {(_live_vwap_ba if _live_vwap_ba is not None else float(poly_leg.ask)):.2f} (est.) - err: {str(poly_exec_error_ba)[:200] if poly_exec_error_ba else 'unknown'}\n"
                    f"\n"
                    f"Predict unwind: {_uw2_status}"
                    f"{_gp_tg}\n"
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
