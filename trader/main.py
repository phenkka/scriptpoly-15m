from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import requests

from predict_sdk import BuildOrderInput, ChainId, LimitHelperInput, OrderBuilder, Side
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

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


app = FastAPI()


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_max_trade_usd() -> float:
    try:
        return float(os.environ.get("TRADER_MAX_TRADE_USD", "1"))
    except ValueError:
        return 1.0


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

    return errs


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

    client = ClobClient(
        host="https://clob.polymarket.com",
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


def _place_predict_limit_buy(leg: OpportunityLeg) -> dict[str, Any]:
    api_key = os.environ.get("PREDICT_API_KEY", "").strip()
    private_key = _normalize_hex_key(os.environ.get("PREDICT_PRIVATE_KEY", ""))
    if not api_key:
        raise RuntimeError("missing_env:PREDICT_API_KEY")
    if not private_key:
        raise RuntimeError("missing_env:PREDICT_PRIVATE_KEY")
    if leg.market_id is None:
        raise RuntimeError("missing_market_id")

    session = requests.Session()
    session.headers.update({"Accept": "application/json", "x-api-key": api_key})
    proxy_url = os.environ.get("PROXY_URL", "")
    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})

    market = _predict_market(session, int(leg.market_id))
    fee_rate_bps = int(market.get("feeRateBps") or 0)
    is_neg_risk = bool(market.get("isNegRisk"))
    is_yield_bearing = bool(market.get("isYieldBearing"))
    token_id = leg.token_id or _predict_token_id_for_side(market, leg.side)

    builder = OrderBuilder.make(ChainId.BNB_MAINNET, private_key)

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
    if "signature" not in order_obj:
        order_obj["signature"] = signature

    payload = {
        "data": {
            "pricePerShare": str(leg.ask),
            "strategy": "LIMIT",
            "slippageBps": "0",
            "isFillOrKill": True,
            "order": order_obj,
        }
    }

    r = session.post(
        "https://api.predict.fun/v1/orders",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=float(os.environ.get("TRADER_TIMEOUT_SEC", "2.0")),
    )
    if not r.ok:
        raise RuntimeError(f"predict_order_http_{r.status_code}: {r.text[:500]}")
    out = r.json()
    if not out.get("success"):
        raise RuntimeError(f"predict_create_order_failed resp={out}")
    return {
        "market": {
            "id": market.get("id"),
            "title": market.get("title"),
            "feeRateBps": fee_rate_bps,
            "isNegRisk": is_neg_risk,
            "isYieldBearing": is_yield_bearing,
        },
        "token_id": token_id,
        "request": payload,
        "response": out,
    }


@app.post("/opportunity")
def opportunity(opp: Opportunity) -> dict:
    opp = _cap_opportunity(opp)

    dry_run = _env_bool("TRADER_DRY_RUN", True)
    trades_file = os.environ.get("TRADER_TRADES_FILE", "/data/trades.jsonl")

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

    row: dict[str, Any] = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "mode": "dry_run" if dry_run else "live",
        "label": opp.label,
        "cap_max_trade_usd": _get_max_trade_usd(),
        "shares": opp.shares,
        "stake_usd": opp.stake_usd,
        "payout_usd": opp.payout_usd,
        "profit_usd": opp.profit_usd,
        "legs": [l.model_dump() for l in opp.legs],
        "ok": False,
    }

    errs = _preflight(opp, poly_leg, pred_leg)
    if errs:
        row["preflight_errors"] = errs
        _append_jsonl(trades_file, row)
        raise HTTPException(status_code=400, detail={"preflight_errors": errs})

    if dry_run:
        _append_jsonl(trades_file, row)
        return {"status": "ok", "mode": "dry_run"}

    try:
        polymarket_result = _place_polymarket_fok_market_buy(poly_leg)
        row["polymarket"] = polymarket_result

        predict_result = _place_predict_limit_buy(pred_leg)
        row["predict"] = predict_result
        row["ok"] = True
        _append_jsonl(trades_file, row)
        return {"status": "ok"}
    except Exception as e:
        row["error"] = str(e)
        _append_jsonl(trades_file, row)
        raise
