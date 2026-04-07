from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
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
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

from trader.config import CONFIG as CFG

# Патч глобального httpx клиента py_clob_client для поддержки прокси
_proxy_url = os.environ.get("PROXY_URL", "").strip()
if _proxy_url:
    import py_clob_client.http_helpers.helpers as _clob_helpers
    _clob_helpers._http_client = httpx.Client(
        http2=True,
        mounts={
            "http://": httpx.HTTPTransport(proxy=_proxy_url),
            "https://": httpx.HTTPTransport(proxy=_proxy_url),
        },
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


_predict_client = _PredictClient()


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


# In-memory cooldown to prevent repeated buys during testing.
_predict_market_last_buy_ts: dict[int, float] = {}
# Набор market_id которые сейчас в процессе исполнения — блокирует параллельные трейды
_predict_market_in_flight: set[int] = set()
_predict_market_in_flight_lock = _threading.Lock()
# State for grouping repeated HEDGE FILLED notifications in the same market
# key: poly token_id  value: (message_id, cumulative_pnl, fill_count, timestamp)
_ba_fill_state: dict[str, tuple[int, float, int, float]] = {}


def _fmt_usd(x: float | int | None) -> str:
    if x is None:
        return "n/a"
    try:
        return f"${float(x):.4f}"
    except Exception:
        return "n/a"


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

    if opp.type not in {"arbitrage", "bid_ask_arbitrage"}:
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
    if opp.type not in {"arbitrage", "bid_ask_arbitrage"}:
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
            if _ticks_behind > _passive_ticks_miss:
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
                            try:
                                if order_id:
                                    _predict_remove_orders(session, [order_id])
                            except Exception:
                                pass
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

    if order_hash and need_final_get_check and not filled:
        # poly_hedge_no_edge cancels: on-chain fill can arrive up to ~30s after
        # API cancel (BSC block confirmation lag). Poll longer to catch ghost fills.
        _is_hedge_cancel = (cancel_reason or "").startswith("poly_hedge_no_edge")
        _FINAL_GET_RETRIES = 60 if _is_hedge_cancel else 3
        _FINAL_GET_SLEEP_SEC = 1.0 if _is_hedge_cancel else 0.25
        if _is_hedge_cancel:
            print(
                f"[PREDICT_LIMIT]{_trace} ghost_fill_watch hash={order_hash} "
                f"cancel_reason={cancel_reason} polling up to {_FINAL_GET_RETRIES}s"
            )
        for _attempt in range(_FINAL_GET_RETRIES):
            try:
                last_get = _predict_get_order_by_hash(session, order_hash)
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
                pass
            if _attempt < _FINAL_GET_RETRIES - 1:
                time.sleep(_FINAL_GET_SLEEP_SEC)

    # ── Final fill check (partial fills count as filled) ──
    total_filled_wei = prev_filled_wei
    if not filled and total_filled_wei > 0:
        filled = True  # partial fill → still hedge what we got

    # ── Cleanup: cancel unfilled remainder ──
    remove_resp: dict[str, Any] | None = None
    if order_id:
        try:
            # Always try to cancel — if fully filled, API will just ignore it
            remove_resp = _predict_remove_orders(session, [order_id])
        except Exception as _re:
            remove_resp = {"success": False, "error": str(_re)}

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
                _append_jsonl(trades_file, row)
                return {"status": "skipped", "reason": _skip_code_ba}

            # Predict filled (full or partial) → record cooldown
            if pred_leg.market_id is not None:
                _predict_market_last_buy_ts[int(pred_leg.market_id)] = time.time()

            # Determine hedge quantity: use actual filled shares from predict, not requested
            _ba_hedge_qty = _ba_total_filled_shares if _ba_total_filled_shares > 0 else float(pred_leg.shares)

            # Step 2: Live net-edge recheck before hedging (poly quote may be stale)
            # Fetch live poly orderbook, calculate VWAP at hedge qty, recompute full net-edge
            _ba_actual_pred_bid = float(_ba_quote_meta.get("final_bid_price") or pred_leg.ask)
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
                _ba_inc_hp = {
                    "ts": datetime.utcnow().isoformat() + "Z",
                    "type": "bid_ask_hedge_price_cap",
                    "label": opp.label,
                    "live_poly_vwap": round(_live_vwap_ba, 6),
                    "poly_max_hedge_price": _poly_max_hedge_price,
                    "pred_bid": _ba_actual_pred_bid,
                    "pred_filled_qty": float(_ba_hedge_qty),
                    "quote_meta": _ba_quote_meta,
                }
                _append_jsonl(incidents_file, _ba_inc_hp)
                row["ok"] = False
                row["summary"]["status"] = "incident"
                row["summary"]["reason_code"] = "bid_ask_hedge_price_cap"
                print(
                    f"[TRADER][INCIDENT] BID_ASK_HEDGE_PRICE_CAP label={opp.label} "
                    f"live_vwap={_live_vwap_ba:.4f} cap={_poly_max_hedge_price:.4f}"
                )
                _append_jsonl(trades_file, row)
                return {"status": "incident", "reason": "bid_ask_hedge_price_cap"}

            # Step 3: FOK hedge on Polymarket — hedge actual filled quantity
            # Guard: partial fill may be below Poly's $1 min order.
            _ba_hedge_cost_usd = _ba_hedge_qty * (_live_vwap_ba if _live_vwap_ba else float(poly_leg.ask))
            _poly_min_hedge = float(os.environ.get("POLY_MIN_ORDER_USD", "1.0") or "1.0")
            if _ba_hedge_cost_usd < _poly_min_hedge:
                _ba_inc_min = {
                    "ts": datetime.utcnow().isoformat() + "Z",
                    "type": "bid_ask_hedge_below_min",
                    "label": opp.label,
                    "pred_filled_qty": float(_ba_hedge_qty),
                    "hedge_cost_usd": round(_ba_hedge_cost_usd, 4),
                    "poly_min_order_usd": _poly_min_hedge,
                }
                _append_jsonl(incidents_file, _ba_inc_min)
                row["ok"] = False
                row["summary"]["status"] = "incident"
                row["summary"]["reason_code"] = "bid_ask_hedge_below_min"
                print(
                    f"[TRADER][INCIDENT] BID_ASK_HEDGE_BELOW_MIN label={opp.label} "
                    f"pred_filled={_ba_hedge_qty:.4f} hedge_cost=${_ba_hedge_cost_usd:.2f} min=${_poly_min_hedge}"
                )
                _append_jsonl(trades_file, row)
                return {"status": "incident", "reason": "bid_ask_hedge_below_min"}

            _ba_hedge_price = _live_vwap_ba if _live_vwap_ba else float(poly_leg.ask)
            _ba_hedge_leg = OpportunityLeg(
                **{**poly_leg.model_dump(), "shares": _ba_hedge_qty, "stake_usd": _ba_hedge_qty * _ba_hedge_price}
            )
            polymarket_result_ba: dict[str, Any] | None = None
            poly_exec_error_ba: Exception | None = None
            _poly_timing_ba["submit_ts"] = time.time()
            try:
                polymarket_result_ba = _place_polymarket_fok_market_buy(_ba_hedge_leg, fak_fallback=True)
                _poly_timing_ba["ack_ts"] = time.time()
            except Exception as _e_ba_poly:
                poly_exec_error_ba = _e_ba_poly
                _poly_timing_ba["fail_ts"] = time.time()
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
            poly_filled_ba = _ba_poly_resp.get("success") is True
            _ba_poly_qty: float = 0.0
            try:
                _ba_poly_qty = float(_ba_poly_resp.get("takingAmount") or 0)
            except (ValueError, TypeError):
                pass
            _ba_residual = abs(_ba_hedge_qty - _ba_poly_qty)

            row["fill_analysis"] = {
                "unhedged_ms": row["timing"].get("unhedged_ms"),
                "predict_fill_to_poly_submit_ms": row["timing"].get("predict_fill_to_poly_submit_ms"),
                "poly_submit_to_fill_ms": row["timing"].get("poly_submit_to_fill_ms"),
                "first_fill_venue": "predict",
                "first_fill_qty": round(_ba_hedge_qty, 6),
                "residual_unhedged_qty": round(_ba_residual, 6),
                "pred_filled_qty": round(_ba_hedge_qty, 6),
                "poly_filled_qty": round(_ba_poly_qty, 6),
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
                f"taking={_ba_poly_resp.get('takingAmount')} "
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
                _append_jsonl(trades_file, row)
                _append_jsonl(success_trades_file, row)
                _ba_order_type = (polymarket_result_ba or {}).get("order_type", "FOK")
                _ba_unhedged_sec = (row["timing"].get("unhedged_ms") or 0) / 1000
                _ba_total_sec = (row["timing"].get("total_ms") or 0) / 1000
                _ba_pred_price = _ba_actual_pred_bid
                _ba_poly_price = _live_vwap_ba if _live_vwap_ba else float(poly_leg.ask)
                _ba_poly_fee_paid = _ba_fee_rate * _ba_poly_price * (1.0 - _ba_poly_price) * _ba_hedge_qty
                _ba_pred_fee_paid = _ba_pred_fee_bps / 10_000 * _ba_pred_price * _ba_hedge_qty
                _ba_gross = _ba_hedge_qty * (1.0 - _ba_pred_price - _ba_poly_price)
                _ba_net_pnl = _ba_gross - _ba_poly_fee_paid - _ba_pred_fee_paid

                _tkey = str(poly_leg.token_id)
                _prev = _ba_fill_state.get(_tkey)
                _GROUP_TTL = 1800  # 30 min window to group fills for same market
                _is_grouped = _prev is not None and (time.time() - _prev[3]) < _GROUP_TTL
                _reply_to_id = _prev[0] if _is_grouped else None
                _cum_pnl = (_prev[1] + _ba_net_pnl) if _is_grouped else _ba_net_pnl
                _fill_n = (_prev[2] + 1) if _is_grouped else 1

                _pnl_line = (
                    f"<b>{_ba_net_pnl:+.2f}$ - TYANUCHKA IS CANCELED</b>\n"
                    if (_ba_pred_price + _ba_poly_price) < 1.0
                    else f"<b>{_ba_net_pnl:+.2f}$</b>\n"
                )
                _cum_line = f"<i>total ×{_fill_n}: {_cum_pnl:+.2f}$</i>\n" if _fill_n > 1 else ""
                _title = f"🟢🟢🟢 <b>HEDGE FILLED ×{_fill_n}</b> 🟢🟢🟢" if _fill_n > 1 else "🟢🟢🟢 <b>HEDGE FILLED</b> 🟢🟢🟢"

                _msg_id = notify(
                    f"{_title}\n"
                    f"\n"
                    f"<b>{opp.label}</b>\n"
                    f"\n"
                    f"Polymarket ({poly_leg.side.upper()} ASK)\n"
                    f"price: {_ba_poly_price:.2f} - stake: ${_ba_poly_qty * _ba_poly_price:.2f} - shares: {_ba_poly_qty:.2f}\n"
                    f"Predict ({pred_leg.side.upper()} BID)\n"
                    f"price: {_ba_pred_price:.2f} - stake: ${_ba_hedge_qty * _ba_pred_price:.2f} - shares: {_ba_hedge_qty:.2f}\n"
                    f"\n"
                    + _pnl_line
                    + _cum_line
                    + f"\n"
                    f"<i>⏱ fill={_ba_quote_meta.get('time_to_first_fill_ms', 0)/1000:.1f}s  unhedged={_ba_unhedged_sec:.1f}s  total={_ba_total_sec:.1f}s</i>",
                    reply_to_message_id=_reply_to_id,
                )
                # Store state: use original msg_id for the whole group so all replies chain to first
                _stored_id = (_prev[0] if _is_grouped else _msg_id) if _msg_id is not None else (_reply_to_id or 0)
                if _stored_id:
                    _ba_fill_state[_tkey] = (_stored_id, _cum_pnl, _fill_n, time.time())
                return {"status": "ok"}
            else:
                # Predict filled, poly failed → unhedged predict incident
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
                    "quote_meta": _ba_quote_meta,
                }
                _append_jsonl(incidents_file, _ba_inc2)
                row["ok"] = False
                row["summary"]["status"] = "incident"
                row["summary"]["reason_code"] = "bid_ask_unhedged_predict"
                print(
                    f"[TRADER][INCIDENT] BID_ASK_UNHEDGED_PREDICT label={opp.label} "
                    f"pred_qty={_ba_hedge_qty:.6f} poly_err={poly_exec_error_ba} "
                    f"residual={_ba_residual:.6f}"
                )
                notify(
                    f"🔴🔴🔴 <b>INCIDENT: UNHEDGED PREDICT</b> 🔴🔴🔴\n"
                    f"\n"
                    f"<b>{opp.label}</b>\n"
                    f"\n"
                    f"Predict ({pred_leg.side.upper()} BID)\n"
                    f"price: {_ba_actual_pred_bid:.2f} - stake: ${_ba_hedge_qty * _ba_actual_pred_bid:.2f} - shares: {_ba_hedge_qty:.2f}\n"
                    f"Polymarket ({poly_leg.side.upper()} ASK) ❌\n"
                    f"price: {_live_vwap_ba:.2f} (est.) - err: {str(poly_exec_error_ba)[:50] if poly_exec_error_ba else 'unknown'}\n"
                    f"\n"
                    f"<b>{_ba_hedge_qty * (_live_vwap_ba - (1.0 - _ba_actual_pred_bid)):+.2f}$ - TYANUCHKA</b>\n"                )
                _append_jsonl(trades_file, row)
                return {"status": "incident", "reason": "bid_ask_unhedged_predict"}

        # ════════════════════════════════════════════════════════════════
        # ASK+ASK: параллельная отправка обеих ног (оригинальная логика)
        # ════════════════════════════════════════════════════════════════
        _pred_timing: dict[str, Any] = {}
        _poly_timing: dict[str, Any] = {}

        def _run_predict_leg() -> dict[str, Any]:
            _pred_timing["submit_ts"] = time.time()
            try:
                res = _place_predict_market_buy(pred_leg, timing=_pred_timing)
                _pred_timing["ack_ts"] = time.time()
                return res
            except Exception:
                _pred_timing["fail_ts"] = time.time()
                raise
            finally:
                _pred_timing["end"] = time.time()

        def _run_poly_leg() -> dict[str, Any]:
            _poly_timing["submit_ts"] = time.time()
            try:
                res = _place_polymarket_fok_market_buy(poly_leg)
                _poly_timing["ack_ts"] = time.time()
                return res
            except Exception:
                _poly_timing["fail_ts"] = time.time()
                raise
            finally:
                _poly_timing["end"] = time.time()

        print(
            f"[TRADER] parallel_exec_start label={opp.label} "
            f"pred_stake={_fmt_usd(pred_leg.stake_usd)} poly_stake={_fmt_usd(poly_leg.stake_usd)} "
            f"poly_book_age={_book_freshness.get('poly_book_age_at_submit_ms', 'n/a'):.0f}ms "
            f"pred_book_age={_book_freshness.get('pred_book_age_at_submit_ms', 'n/a'):.0f}ms"
        )

        predict_result: dict[str, Any] | None = None
        polymarket_result: dict[str, Any] | None = None
        pred_exec_error: Exception | None = None
        poly_exec_error: Exception | None = None

        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="leg") as _pool:
            _pred_future = _pool.submit(_run_predict_leg)
            _poly_future = _pool.submit(_run_poly_leg)

            try:
                predict_result = _pred_future.result()
            except Exception as _e:
                pred_exec_error = _e
                _pred_timing.setdefault("end", time.time())
            try:
                polymarket_result = _poly_future.result()
            except Exception as _e:
                poly_exec_error = _e
                _poly_timing.setdefault("end", time.time())

        # ── Timing ──────────────────────────────────────────────────────
        row["timing"]["predict_start"] = _pred_timing.get("submit_ts")
        row["timing"]["predict_end"] = _pred_timing.get("end")
        row["timing"]["predict_ack_ts"] = _pred_timing.get("ack_ts")
        row["timing"]["predict_create_request_ts"] = _pred_timing.get("predict_create_request_ts")
        row["timing"]["poly_start"] = _poly_timing.get("submit_ts")
        row["timing"]["poly_end"] = _poly_timing.get("end")
        row["timing"]["poly_ack_ts"] = _poly_timing.get("ack_ts")
        row["timing"]["t_end"] = time.time()
        row["timing"]["total_ms"] = (row["timing"]["t_end"] - t0) * 1000.0

        _ps = _pred_timing.get("submit_ts")
        _pe = _pred_timing.get("end")
        _pys = _poly_timing.get("submit_ts")
        _pye = _poly_timing.get("end")
        if _ps and _pe:
            row["timing"]["predict_ms"] = (_pe - _ps) * 1000.0
        if _pys and _pye:
            row["timing"]["poly_ms"] = (_pye - _pys) * 1000.0
        # submit_gap: разница старта двух submit (0 = идеально одновременно)
        if _ps and _pys:
            row["timing"]["submit_gap_ms"] = abs(_ps - _pys) * 1000.0
        # overlap: сколько мс обе ноги работали одновременно
        if _ps and _pe and _pys and _pye:
            overlap_start = max(_ps, _pys)
            overlap_end = min(_pe, _pye)
            row["timing"]["overlap_ms"] = max(0.0, (overlap_end - overlap_start) * 1000.0)

        # first_ack / second_ack / unhedged_ms
        _pred_ack = _pred_timing.get("ack_ts")
        _poly_ack = _poly_timing.get("ack_ts")
        if _pred_ack and _poly_ack:
            _first_ack_ts = min(_pred_ack, _poly_ack)
            _submit_start = min(_ps or _pred_ack, _pys or _poly_ack)
            row["timing"]["first_ack_ms"] = (_first_ack_ts - _submit_start) * 1000.0
            row["timing"]["second_ack_ms"] = (max(_pred_ack, _poly_ack) - _submit_start) * 1000.0
            row["timing"]["unhedged_ms"] = abs(_pred_ack - _poly_ack) * 1000.0
            row["timing"]["first_fill_venue"] = "predict" if _pred_ack <= _poly_ack else "polymarket"
        elif _pred_ack:
            row["timing"]["first_fill_venue"] = "predict"
            row["timing"]["unhedged_ms"] = ((_pye or time.time()) - _pred_ack) * 1000.0
        elif _poly_ack:
            row["timing"]["first_fill_venue"] = "polymarket"
            row["timing"]["unhedged_ms"] = ((_pe or time.time()) - _poly_ack) * 1000.0

        create_req_ts = row["timing"].get("predict_create_request_ts")
        if isinstance(create_req_ts, (int, float)):
            calc_epoch = _dt_to_epoch_s(analyzer_calc_dt)
            tick_epoch = _dt_to_epoch_s(analyzer_tick_max_dt)
            if calc_epoch is not None:
                row["timing"]["analyzer_calc_to_predict_create_ms"] = (create_req_ts - calc_epoch) * 1000.0
            if tick_epoch is not None:
                row["timing"]["tick_ts_to_predict_create_ms"] = (create_req_ts - tick_epoch) * 1000.0

        # ── Predict result processing ───────────────────────────────────
        pred_filled = False
        pred_hash: str | None = None
        filled_shares_dbg: float | None = None
        _pred_filled_qty: float = 0.0

        if predict_result is not None:
            row["predict"] = predict_result
            resp_obj = predict_result.get("response") or {}
            pred_hash = resp_obj.get("orderHash")
            if not pred_hash:
                try:
                    pred_hash = (((resp_obj.get("create") or {}).get("data") or {}) or {}).get("orderHash")
                except Exception:
                    pred_hash = None

            filled_shares_dbg = _extract_predict_filled_shares(predict_result, requested_shares=float(pred_leg.shares))
            _pred_filled_qty = filled_shares_dbg if (filled_shares_dbg is not None and filled_shares_dbg > 1e-9) else 0.0
            try:
                last_get = resp_obj.get("get") or {}
                data = (last_get.get("data") or {}) if isinstance(last_get, dict) else {}
                status = str(data.get("status") or "")
                amount_filled_usdt_wei = data.get("amountFilled")
            except Exception:
                status = ""
                amount_filled_usdt_wei = None

            row["summary"]["predict_exec"] = {
                "order_hash": pred_hash,
                "status": status,
                "amountFilled_usdt_wei": amount_filled_usdt_wei,
                "filled_shares": filled_shares_dbg,
                "requested_shares": float(pred_leg.shares),
                "requested_stake_usd": float(pred_leg.stake_usd),
            }

            pred_filled = _predict_resp_is_filled(resp_obj)
            # Дополнительная защита: если amountFilled=0 и статус CANCELLED — сброс
            if pred_filled:
                try:
                    _lg = resp_obj.get("get") or {}
                    _ld = (_lg.get("data") or {}) if isinstance(_lg, dict) else {}
                    _lst = str(_ld.get("status") or "").upper()
                    _laf = int(str(_ld.get("amountFilled") or "0"))
                    if _lst in {"CANCELLED", "EXPIRED", "REJECTED"} and _laf <= 0:
                        pred_filled = False
                except Exception:
                    pass
            if not pred_filled:
                _pred_filled_qty = 0.0
                filled_shares_dbg = None
            print(
                "[TRADER] predict_done "
                f"order_hash={pred_hash} status={status} "
                f"amountFilled_usdt_wei={amount_filled_usdt_wei} "
                f"filled_shares={filled_shares_dbg} filled={pred_filled}"
            )
            if pred_filled and filled_shares_dbg is not None and filled_shares_dbg > 1e-9 and pred_leg.market_id is not None:
                _predict_market_last_buy_ts[int(pred_leg.market_id)] = time.time()
        else:
            print(f"[TRADER] predict_error err={pred_exec_error}")

        # ── Poly result processing ──────────────────────────────────────
        poly_filled = False
        _poly_filled_qty: float = 0.0
        if polymarket_result is not None:
            row["polymarket"] = polymarket_result
            _poly_resp = (polymarket_result.get("response") or {})
            poly_filled = _poly_resp.get("success") is True
            try:
                _poly_filled_qty = float(_poly_resp.get("takingAmount") or 0)
            except (ValueError, TypeError):
                _poly_filled_qty = 0.0
            print(
                "[TRADER] poly_done "
                f"success={poly_filled} status={_poly_resp.get('status')} "
                f"making={_poly_resp.get('makingAmount')} taking={_poly_resp.get('takingAmount')}"
            )
        else:
            print(f"[TRADER] poly_error err={poly_exec_error}")

        row["predict_filled_shares"] = filled_shares_dbg

        # ── Fill analysis: 5 ключевых метрик ────────────────────────────
        _first_fill_venue = row["timing"].get("first_fill_venue")
        _first_fill_qty = _pred_filled_qty if _first_fill_venue == "predict" else _poly_filled_qty
        _residual_qty = abs(_pred_filled_qty - _poly_filled_qty)

        # hedge_drift_bps: насколько реальная цена хедж-ноги отклонилась от trigger
        _hedge_drift_bps: float | None = None
        if pred_filled and poly_filled:
            try:
                _pr = (polymarket_result.get("response") or {})
                _poly_mk = float(_pr.get("makingAmount") or 0)
                _poly_tk = float(_pr.get("takingAmount") or 0)
                if _poly_tk > 0:
                    _actual_poly_price = _poly_mk / _poly_tk
                    _trigger_poly_price = float(poly_leg.ask)
                    if _trigger_poly_price > 0:
                        _hedge_drift_bps = ((_actual_poly_price - _trigger_poly_price) / _trigger_poly_price) * 10_000
            except Exception:
                pass

        row["fill_analysis"] = {
            "unhedged_ms": row["timing"].get("unhedged_ms"),
            "first_fill_venue": _first_fill_venue,
            "first_fill_qty": round(_first_fill_qty, 6),
            "hedge_drift_bps": round(_hedge_drift_bps, 2) if _hedge_drift_bps is not None else None,
            "residual_qty_after_abort": round(_residual_qty, 6),
            "pred_filled_qty": round(_pred_filled_qty, 6),
            "poly_filled_qty": round(_poly_filled_qty, 6),
            "requested_qty": round(float(opp.shares), 6),
        }

        row["parallel_result"] = {
            "pred_filled": pred_filled,
            "poly_filled": poly_filled,
            "pred_error": str(pred_exec_error) if pred_exec_error else None,
            "poly_error": str(poly_exec_error) if poly_exec_error else None,
        }

        # ── Incident helper ─────────────────────────────────────────────
        def _write_incident(incident_type: str, details: dict[str, Any]) -> None:
            incident = {
                "ts": datetime.utcnow().isoformat() + "Z",
                "type": incident_type,
                "label": opp.label,
                "fill_analysis": row.get("fill_analysis"),
                "book_freshness": row.get("book_freshness"),
                "timing": {
                    "total_ms": row["timing"].get("total_ms"),
                    "unhedged_ms": row["timing"].get("unhedged_ms"),
                    "submit_gap_ms": row["timing"].get("submit_gap_ms"),
                    "predict_ms": row["timing"].get("predict_ms"),
                    "poly_ms": row["timing"].get("poly_ms"),
                },
                "trigger_book": {
                    "poly_ask": float(poly_leg.ask),
                    "poly_ask_sz": float(poly_leg.ask_sz),
                    "pred_ask": float(pred_leg.ask),
                    "pred_ask_sz": float(pred_leg.ask_sz),
                },
                **details,
            }
            _append_jsonl(incidents_file, incident)

        # ── Outcome routing ─────────────────────────────────────────────
        if pred_filled and poly_filled:
            # ✓ Обе ноги исполнены
            row["ok"] = True
            row["summary"]["status"] = "ok"
            row["summary"]["reason_code"] = "ok"
        elif not pred_filled and not poly_filled:
            # Обе ноги не исполнены — чисто, ничего не потеряно
            skip_code = "both_legs_failed"
            if pred_exec_error and "insufficient_collateral_balance" in str(pred_exec_error):
                skip_code = "predict_insufficient_collateral_balance"
            row["skipped"] = True
            row["skip_reason"] = {"code": skip_code, "pred_error": str(pred_exec_error), "poly_error": str(poly_exec_error)}
            row["summary"]["status"] = "skipped"
            row["summary"]["reason_code"] = skip_code
            row["summary"]["reason"] = row["skip_reason"]
            print(f"[TRADER]{_t}[SKIP] label={opp.label} reason={skip_code}")
            _append_jsonl(trades_file, row)
            return {"status": "skipped", "reason": skip_code}
        elif pred_filled and not poly_filled:
            # ⚠ Predict исполнен, Poly нет — незахеджированная predict позиция
            row["ok"] = False
            row["summary"]["status"] = "incident"
            row["summary"]["reason_code"] = "unhedged_predict"
            row["summary"]["reason"] = {"poly_error": str(poly_exec_error)}
            row["unhedged"] = {
                "leg": "predict",
                "side": pred_leg.side,
                "market_id": pred_leg.market_id,
                "filled_shares": _pred_filled_qty,
                "stake_usd": float(pred_leg.stake_usd),
                "residual_qty": _residual_qty,
            }
            _write_incident("unhedged_predict", {
                "unhedged_leg": "predict",
                "unhedged_side": pred_leg.side,
                "unhedged_market_id": pred_leg.market_id,
                "unhedged_qty": _pred_filled_qty,
                "unhedged_stake_usd": float(pred_leg.stake_usd),
                "poly_error": str(poly_exec_error),
            })
            print(
                f"[TRADER][INCIDENT] UNHEDGED_PREDICT label={opp.label} "
                f"pred_filled_qty={_pred_filled_qty:.6f} poly_error={poly_exec_error} "
                f"unhedged_ms={row['timing'].get('unhedged_ms', 'n/a')}"
            )
            notify(
                f"🔴🔴🔴 <b>INCIDENT: UNHEDGED PREDICT</b> 🔴🔴🔴\n"
                f"\n"
                f"<b>{opp.label}</b>\n"
                f"\n"
                f"Predict ({pred_leg.side.upper()} ASK)\n"
                f"stake: ${float(pred_leg.stake_usd):.2f} - shares: {_pred_filled_qty:.2f}\n"
                f"Polymarket ({poly_leg.side.upper()} ASK) ❌\n"
                f"err: {str(poly_exec_error)[:60] if poly_exec_error else 'unknown'}\n"
                f"\n"
                f"<b>TYANUCHKA</b>\n"            )
            _append_jsonl(trades_file, row)
            return {"status": "incident", "reason": "unhedged_predict", "unhedged": "predict"}
        else:
            # ⚠ Poly исполнен, Predict нет — незахеджированная poly позиция
            _cancel_result: dict[str, Any] | None = None
            if predict_result is not None:
                _resp = predict_result.get("response") or {}
                _oid = _resp.get("orderId")
                if _oid:
                    try:
                        _cancel_result = _predict_remove_orders(session=_predict_client.get()[0], ids=[_oid])
                        row["predict_cancel"] = _cancel_result
                        print(f"[TRADER] predict_cancel_attempt order_id={_oid} result={_cancel_result}")
                    except Exception as _ce:
                        row["predict_cancel"] = {"error": str(_ce)}
                        _cancel_result = {"error": str(_ce)}
                        print(f"[TRADER] predict_cancel_failed order_id={_oid} err={_ce}")

            row["ok"] = False
            row["summary"]["status"] = "incident"
            row["summary"]["reason_code"] = "unhedged_poly"
            row["summary"]["reason"] = {"pred_error": str(pred_exec_error)}
            row["unhedged"] = {
                "leg": "polymarket",
                "side": poly_leg.side,
                "token_id": poly_leg.token_id,
                "filled_shares": _poly_filled_qty,
                "stake_usd": float(poly_leg.stake_usd),
                "residual_qty": _residual_qty,
                "predict_cancel": _cancel_result,
            }
            _write_incident("unhedged_poly", {
                "unhedged_leg": "polymarket",
                "unhedged_side": poly_leg.side,
                "unhedged_token_id": poly_leg.token_id,
                "unhedged_qty": _poly_filled_qty,
                "unhedged_stake_usd": float(poly_leg.stake_usd),
                "pred_error": str(pred_exec_error),
                "predict_cancel": _cancel_result,
            })
            print(
                f"[TRADER][INCIDENT] UNHEDGED_POLY label={opp.label} "
                f"poly_filled_qty={_poly_filled_qty:.6f} pred_error={pred_exec_error} "
                f"cancel={_cancel_result} unhedged_ms={row['timing'].get('unhedged_ms', 'n/a')}"
            )
            _append_jsonl(trades_file, row)
            return {"status": "incident", "reason": "unhedged_poly", "unhedged": "polymarket"}

        # ── Реальные исполнения для статистики (обе ноги OK) ────────────
        try:
            poly_resp_data = (polymarket_result.get("response") or {})
            poly_making = float(poly_resp_data.get("makingAmount") or 0)
            poly_taking = float(poly_resp_data.get("takingAmount") or 0)
            poly_actual_price = (poly_making / poly_taking) if poly_taking > 0 else None
        except Exception:
            poly_making = poly_taking = 0.0
            poly_actual_price = None

        try:
            pred_req_order = (((predict_result.get("request") or {}).get("data") or {}).get("order") or {})
            pred_making = int(str(pred_req_order.get("makerAmount") or "0")) / 10**18
            pred_taking = int(str(pred_req_order.get("takerAmount") or "0")) / 10**18
            pred_price_per_share = (pred_making / pred_taking) if pred_taking > 0 else None
        except Exception:
            pred_making = pred_taking = 0.0
            pred_price_per_share = None

        total_spent = poly_making + pred_making
        bundle_sum = (poly_actual_price + pred_price_per_share) if (poly_actual_price and pred_price_per_share) else None
        min_shares = min(poly_taking, pred_taking) if (poly_taking > 0 and pred_taking > 0) else None
        actual_profit = (min_shares - total_spent) if (min_shares and total_spent > 0) else None

        row["actual_execution"] = {
            "poly": {
                "spent_usd": round(poly_making, 6),
                "shares_received": round(poly_taking, 6),
                "price_per_share": round(poly_actual_price, 6) if poly_actual_price else None,
                "side": poly_leg.side,
            },
            "pred": {
                "spent_usd": round(pred_making, 6),
                "shares_received": round(pred_taking, 6),
                "price_per_share": round(pred_price_per_share, 6) if pred_price_per_share else None,
                "side": pred_leg.side,
            },
            "total_spent_usd": round(total_spent, 6),
            "bundle_sum": round(bundle_sum, 6) if bundle_sum else None,
            "min_shares": round(min_shares, 6) if min_shares else None,
            "actual_profit_usd": round(actual_profit, 6) if actual_profit else None,
            "estimated_profit_usd": round(opp.profit_usd, 6),
        }
        row["summary"]["actual_execution"] = row["actual_execution"]

        print(
            "[TRADER][OK] "
            f"label={opp.label} shares={opp.shares:.4f} stake={_fmt_usd(opp.stake_usd)} "
            f"actual_bundle={f'{bundle_sum:.4f}' if bundle_sum else 'n/a'} actual_profit={_fmt_usd(actual_profit)} "
            f"unhedged_ms={row['timing'].get('unhedged_ms', 'n/a')} "
            f"submit_gap_ms={row['timing'].get('submit_gap_ms', 'n/a')} "
            f"hedge_drift_bps={_hedge_drift_bps or 'n/a'} "
            f"residual_qty={_residual_qty:.6f}"
        )
        _append_jsonl(trades_file, row)
        _append_jsonl(success_trades_file, row)
        return {"status": "ok"}
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
