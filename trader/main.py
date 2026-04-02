from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
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


app = FastAPI()


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

    if opp.type != "arbitrage":
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
    if opp.type != "arbitrage":
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


def _place_polymarket_fok_market_buy(leg: OpportunityLeg) -> dict[str, Any]:
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
    try:
        client = ClobClient(
            host=host,
            chain_id=137,
            key=private_key,
            signature_type=signature_type,
            funder=funder,
        )

        if poly_api_key and poly_secret and poly_passphrase:
            client.set_api_creds(ApiCreds(api_key=poly_api_key, api_secret=poly_secret, api_passphrase=poly_passphrase))
        else:
            client.set_api_creds(client.create_or_derive_api_creds())

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
        }
    except Exception as e:
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


def _place_predict_limit_buy(leg: OpportunityLeg) -> dict[str, Any]:
    api_key = os.environ.get("PREDICT_API_KEY", "").strip()
    private_key = _normalize_hex_key(os.environ.get("PREDICT_PRIVATE_KEY", ""))
    if not api_key:
        raise RuntimeError("missing_env:PREDICT_API_KEY")
    if not private_key:
        raise RuntimeError("missing_env:PREDICT_PRIVATE_KEY")
    if leg.market_id is None:
        raise RuntimeError("predict_missing_market_id")

    session = requests.Session()
    session.headers.update({"Accept": "application/json", "x-api-key": api_key})

    if CFG.predict_proxy_url:
        session.proxies.update({"http": CFG.predict_proxy_url, "https": CFG.predict_proxy_url})

    chain_id = _get_predict_chain_id()
    predict_account = os.environ.get("PREDICT_ACCOUNT", "").strip() or None
    builder = OrderBuilder.make(chain_id, private_key, OrderBuilderOptions(predict_account=predict_account) if predict_account else None)

    jwt_token = _predict_get_jwt(session, private_key, predict_account=predict_account, builder=builder)
    session.headers.update({"Authorization": f"Bearer {jwt_token}"})

    market = _predict_market(session, int(leg.market_id))
    fee_rate_bps = int(market.get("feeRateBps") or 0)
    is_neg_risk = bool(market.get("isNegRisk"))
    is_yield_bearing = bool(market.get("isYieldBearing"))
    token_id = leg.token_id or _predict_token_id_for_side(market, leg.side)

    price_per_share_wei = _wei_from_float(float(leg.ask))
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
            "pricePerShare": str(int(round(float(leg.ask) * 10**18))),
            "strategy": "LIMIT",
            "slippageBps": "0",
            "order": order_api,
        }
    }

    headers: dict[str, str] = {"Content-Type": "application/json"}

    r = session.post(
        "https://api.predict.fun/v1/orders",
        headers=headers,
        data=json.dumps(payload),
        timeout=float(CFG.timeout_sec),
    )
    if not r.ok:
        raise RuntimeError(f"predict_order_http_{r.status_code}: {r.text[:500]}")
    out = r.json()
    if not out.get("success"):
        raise RuntimeError(f"predict_create_order_failed resp={out}")

    create_data = out.get("data") if isinstance(out.get("data"), dict) else {}
    order_id = str(create_data.get("orderId") or "").strip() or None
    order_hash = str(create_data.get("orderHash") or "").strip() or None

    fill_timeout_sec = float(CFG.predict_fill_timeout_sec)
    poll_interval_sec = float(CFG.predict_fill_poll_interval_sec)
    t_deadline = time.time() + max(0.0, fill_timeout_sec)
    last_get: dict[str, Any] | None = None
    filled = False
    if order_hash:
        while time.time() < t_deadline:
            last_get = _predict_get_order_by_hash(session, order_hash)
            if _predict_resp_is_filled(last_get):
                filled = True
                break
            time.sleep(max(0.05, poll_interval_sec))

    remove_resp: dict[str, Any] | None = None
    if not filled and order_id:
        remove_resp = _predict_remove_orders(session, [order_id])

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
    book_obj = Book(
        market_id=int(leg.market_id),
        update_timestamp_ms=0,
        bids=[],
        asks=[[float(leg.ask), float(leg.ask_sz)]],
    )

    slippage_bps = int(os.environ.get("PREDICT_SLIPPAGE_BPS", "0") or "0")
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

    t0 = time.time()

    dry_run = bool(CFG.dry_run)
    trades_file = str(CFG.trades_file)
    success_trades_file = os.environ.get("TRADER_SUCCESS_TRADES_FILE", "/data/trades_success.jsonl")
    test_mode = _test_mode()

    # Minimal audit log for operator.
    print(
        "[TRADER] recv "
        f"label={opp.label} shares={opp.shares:.2f} "
        f"cost=${opp.stake_usd:.2f} payout=${opp.payout_usd:.2f} profit=${opp.profit_usd:.2f} "
        f"sent_at={opp.sent_at} recv_at={datetime.utcnow().isoformat()}Z"
    )
    for i, leg in enumerate(opp.legs, start=1):
        print(
            f"[TRADER] leg{i} source={leg.source} side={leg.side} ask={leg.ask} "
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
                "[TRADER][SKIP] "
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
                "[TRADER][SKIP] "
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
                    "[TRADER][SKIP] "
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
                        "[TRADER][SKIP] "
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
        # LIVE POLY PRICE CHECK — перед тем как коммитить
        # predict-капитал, убеждаемся что цена poly ещё OK.
        # Если poly сдвинулась и сумма >= порога — пропускаем.
        # ────────────────────────────────────────────────────
        poly_max_live_sum = float(os.environ.get("POLY_MAX_LIVE_SUM", "1.0"))
        try:
            live_book = _polymarket_book(str(poly_leg.token_id))
            live_asks_raw = live_book.get("asks") or []
            live_poly_ask: float | None = None
            if live_asks_raw:
                # asks: list of {"price": str, "size": str} — берём минимальную цену
                live_poly_ask = min(float(x["price"]) for x in live_asks_raw if float(x.get("price", 0)) > 0)
            if live_poly_ask is not None:
                live_sum = live_poly_ask + float(pred_leg.ask)
                row["live_poly_precheck"] = {
                    "stale_poly_ask": float(poly_leg.ask),
                    "live_poly_ask": live_poly_ask,
                    "pred_ask": float(pred_leg.ask),
                    "live_sum": round(live_sum, 6),
                    "threshold": poly_max_live_sum,
                }
                print(
                    "[TRADER] poly_live_precheck "
                    f"stale={poly_leg.ask} live={live_poly_ask:.4f} "
                    f"pred={pred_leg.ask} sum={live_sum:.4f} threshold={poly_max_live_sum}"
                )
                if live_sum >= poly_max_live_sum:
                    row["skipped"] = True
                    row["skip_reason"] = {
                        "code": "poly_price_moved",
                        "stale_poly_ask": float(poly_leg.ask),
                        "live_poly_ask": live_poly_ask,
                        "live_sum": round(live_sum, 6),
                        "threshold": poly_max_live_sum,
                    }
                    row["summary"]["status"] = "skipped"
                    row["summary"]["reason_code"] = "poly_price_moved"
                    row["summary"]["reason"] = row["skip_reason"]
                    print(
                        "[TRADER][SKIP] "
                        f"label={opp.label} reason=poly_price_moved "
                        f"stale={poly_leg.ask} live={live_poly_ask:.4f} sum={live_sum:.4f}"
                    )
                    _append_jsonl(trades_file, row)
                    return {"status": "skipped", "reason": "poly_price_moved"}
        except Exception as _e_poly_check:
            print(f"[TRADER] poly_live_precheck_failed (non-fatal): {_e_poly_check}")

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
            print(
                "[TRADER] predict_done "
                f"order_hash={pred_hash} status={status} "
                f"amountFilled_usdt_wei={amount_filled_usdt_wei} "
                f"filled_shares={filled_shares_dbg} filled={pred_filled}"
            )
            if filled_shares_dbg is not None and filled_shares_dbg > 1e-9 and pred_leg.market_id is not None:
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
            print(f"[TRADER][SKIP] label={opp.label} reason={skip_code}")
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
