from __future__ import annotations

from datetime import datetime
import json
import os
from threading import Lock
from typing import Literal
import urllib.error
import urllib.request
import socket

from fastapi import FastAPI
from pydantic import BaseModel, Field


class SideQuote(BaseModel):
    bid: float | None = None
    bid_sz: float = 0.0
    ask: float | None = None
    ask_sz: float = 0.0


class TickMeta(BaseModel):
    question: str | None = None
    slot: int | None = None
    token_ids: dict[str, str | None] | None = None
    market_id: int | None = None
    title: str | None = None


class Tick(BaseModel):
    source: Literal["polymarket", "predict"]
    ts: datetime = Field(..., description="Event timestamp")
    up: SideQuote
    down: SideQuote
    meta: TickMeta | None = None


app = FastAPI()

_state_lock = Lock()
_last: dict[str, Tick] = {}
_best_profit_usd: dict[tuple[str, int], float] = {}


def _post_opportunity(payload: dict) -> None:
    url = os.environ.get("TRADER_URL", "").strip()
    if not url:
        return

    timeout_s = float(os.environ.get("TRADER_TIMEOUT_SEC", "0.5"))

    label = payload.get("label")
    stake_usd = payload.get("stake_usd")
    profit_usd = payload.get("profit_usd")
    print(f"[TRADER] send label={label} stake_usd={stake_usd} profit_usd={profit_usd} url={url}")

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            try:
                status = getattr(resp, "status", None)
            except Exception:
                status = None
            print(f"[TRADER] sent label={label} status={status}")
            return
    except (urllib.error.URLError, ValueError, TimeoutError, socket.timeout) as e:
        print(f"[TRADER] send_failed label={label} err={type(e).__name__}")
        return


def _check_arbitrage() -> None:
    poly = _last.get("polymarket")
    pred = _last.get("predict")
    if not poly or not pred:
        return

    bankroll_usd = float(os.environ.get("BANKROLL_USD", "100"))
    trader_max_trade_usd = float(os.environ.get("TRADER_MAX_TRADE_USD", "5"))
    poly_bankroll_usd = float(os.environ.get("POLY_BANKROLL_USD", str(bankroll_usd)))
    pred_bankroll_usd = float(os.environ.get("PRED_BANKROLL_USD", str(bankroll_usd)))
    min_stake_usd = float(os.environ.get("MIN_STAKE_USD", "1"))
    min_profit_usd = float(os.environ.get("MIN_PROFIT_USD", "0"))

    def _get_ask_and_sz(t: Tick, side: Literal["up", "down"]) -> tuple[float | None, float]:
        q = t.up if side == "up" else t.down
        return q.ask, float(q.ask_sz)

    # Buy UP on A and DOWN on B => ask_up(A) + ask_down(B) < 1
    # Only keep unique combinations; swapped legs would be identical.
    combos: list[tuple[str, Tick, Literal["up", "down"], Tick, Literal["up", "down"]]] = [
        ("UP@poly + DOWN@pred", poly, "up", pred, "down"),
        ("DOWN@poly + UP@pred", poly, "down", pred, "up"),
    ]

    for label, t1, side1, t2, side2 in combos:
        t1_slot = t1.meta.slot if t1.meta else None
        t2_slot = t2.meta.slot if t2.meta else None
        if t1_slot is None or t2_slot is None or t1_slot != t2_slot:
            continue

        slot_key = int(t1_slot)
        key = (label, slot_key)

        ask1, sz1 = _get_ask_and_sz(t1, side1)
        ask2, sz2 = _get_ask_and_sz(t2, side2)

        if ask1 is None or ask2 is None:
            continue
        if ask1 <= 0.0 or ask2 <= 0.0:
            continue
        if ask1 >= 1.0 or ask2 >= 1.0:
            continue

        s = ask1 + ask2
        edge = 1.0 - s
        if edge <= 0.0:
            continue

        # Sizing model: buy the SAME number of shares Q on both legs.
        # Then payout is the same regardless of which side wins (payout_usd = Q).
        # Costs per leg differ: cost_leg = Q * ask.
        sz1_f = max(0.0, float(sz1))
        sz2_f = max(0.0, float(sz2))
        pool1_usd = ask1 * sz1_f
        pool2_usd = ask2 * sz2_f

        t1_bankroll = poly_bankroll_usd if t1.source == "polymarket" else pred_bankroll_usd
        t2_bankroll = poly_bankroll_usd if t2.source == "polymarket" else pred_bankroll_usd

        q_limits = [
            sz1_f,
            sz2_f,
            (t1_bankroll / ask1) if t1_bankroll > 0 else 0.0,
            (t2_bankroll / ask2) if t2_bankroll > 0 else 0.0,
            (bankroll_usd / s) if bankroll_usd > 0 else 0.0,
            (trader_max_trade_usd / s) if trader_max_trade_usd > 0 else 0.0,
        ]
        q = min(q_limits)
        if q <= 0.0:
            continue

        cost1_usd = q * ask1
        cost2_usd = q * ask2
        total_cost_usd = q * s
        profit_usd = q * edge

        if total_cost_usd < min_stake_usd:
            continue
        if profit_usd < min_profit_usd:
            continue

        # Expected profit on total spend (stake) using bundle-arb model:
        # ROI on cost = (1 - s) / s
        roi = edge / s

        prev_best = _best_profit_usd.get(key)
        if prev_best is not None and profit_usd <= prev_best + 1e-6:
            continue

        _best_profit_usd[key] = profit_usd

        if s < 1.0:
            t1_token_id = None
            if t1.meta and t1.meta.token_ids:
                t1_token_id = t1.meta.token_ids.get(side1)
            t2_token_id = None
            if t2.meta and t2.meta.token_ids:
                t2_token_id = t2.meta.token_ids.get(side2)

            payload = {
                "type": "arbitrage",
                "label": label,
                "sum": s,
                "edge": edge,
                "roi": roi,
                "shares": q,
                "stake_usd": total_cost_usd,
                "payout_usd": q,
                "profit_usd": profit_usd,
                "bankroll_usd": bankroll_usd,
                "min_stake_usd": min_stake_usd,
                "min_profit_usd": min_profit_usd,
                "legs": [
                    {
                        "source": t1.source,
                        "side": side1,
                        "ts": t1.ts.isoformat(),
                        "ask": ask1,
                        "ask_sz": float(sz1),
                        "pool_usd": pool1_usd,
                        "shares": q,
                        "stake_usd": cost1_usd,
                        "token_id": t1_token_id,
                        "market_id": t1.meta.market_id if t1.meta else None,
                        "title": t1.meta.title if t1.meta else None,
                    },
                    {
                        "source": t2.source,
                        "side": side2,
                        "ts": t2.ts.isoformat(),
                        "ask": ask2,
                        "ask_sz": float(sz2),
                        "pool_usd": pool2_usd,
                        "shares": q,
                        "stake_usd": cost2_usd,
                        "token_id": t2_token_id,
                        "market_id": t2.meta.market_id if t2.meta else None,
                        "title": t2.meta.title if t2.meta else None,
                    },
                ],
                "sent_at": datetime.utcnow().isoformat() + "Z",
            }
            print(
                f"[ARBITRAGE] {label} sum={s:.4f} edge={edge:.4f} roi={roi:.4f} "
                f"shares={q:.2f} cost=${total_cost_usd:.2f} payout=${q:.2f} profit=${profit_usd:.2f} "
                f"| leg1 {t1.source}:{side1}@{t1.ts.isoformat()} ask={ask1} sz={sz1_f:.1f} stake=${cost1_usd:.2f} pool=${pool1_usd:.2f} "
                f"| leg2 {t2.source}:{side2}@{t2.ts.isoformat()} ask={ask2} sz={sz2_f:.1f} stake=${cost2_usd:.2f} pool=${pool2_usd:.2f}"
            )

            _post_opportunity(payload)


@app.post("/ingest")
def ingest(tick: Tick) -> dict[str, str]:
    with _state_lock:
        _last[tick.source] = tick
        _check_arbitrage()
    return {"status": "ok"}
