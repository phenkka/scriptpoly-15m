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

from analyzer.config import CONFIG as CFG


class SideQuote(BaseModel):
    bid: float | None = None
    bid_sz: float = 0.0
    ask: float | None = None
    ask_sz: float = 0.0
    bids: list[list[float]] | None = None  # [[price, qty], ...]
    asks: list[list[float]] | None = None  # [[price, qty], ...]


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
    url = CFG.trader_url
    if not url:
        return

    timeout_s = float(CFG.trader_timeout_sec)

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

    trader_max_trade_usd = float(CFG.trader_max_trade_usd)
    poly_bankroll_usd = float(CFG.poly_bankroll_usd)
    pred_bankroll_usd = float(CFG.pred_bankroll_usd)
    min_stake_usd = float(CFG.min_stake_usd)
    min_profit_usd = float(CFG.min_profit_usd)

    poly_buffer_bps = float(CFG.poly_vwap_buffer_bps)
    pred_buffer_bps = float(CFG.pred_vwap_buffer_bps)
    max_book_levels = int(CFG.vwap_max_levels)

    def _buffer_bps_for_source(source: str) -> float:
        if source == "polymarket":
            return poly_buffer_bps
        return pred_buffer_bps

    def _side_obj(t: Tick, side: Literal["up", "down"]) -> SideQuote:
        return t.up if side == "up" else t.down

    def _levels_asks(t: Tick, side: Literal["up", "down"]) -> list[tuple[float, float]]:
        q = _side_obj(t, side)
        levels = q.asks or []
        out: list[tuple[float, float]] = []
        for lvl in levels[: max(0, max_book_levels)]:
            try:
                p = float(lvl[0])
                sz = float(lvl[1])
            except Exception:
                continue
            if p <= 0.0 or p >= 1.0 or sz <= 0.0:
                continue
            out.append((p, sz))
        if out:
            return out
        if q.ask is None or q.ask_sz <= 0:
            return []
        return [(float(q.ask), float(q.ask_sz))]

    def _depth_shares_from_asks(levels: list[tuple[float, float]]) -> float:
        return float(sum(sz for _, sz in levels))

    def _vwap_for_shares(levels: list[tuple[float, float]], shares: float) -> float | None:
        if shares <= 0.0:
            return None
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
        if got <= 0.0:
            return None
        return cost / got

    def _get_ask_and_sz(t: Tick, side: Literal["up", "down"]) -> tuple[float | None, float]:
        q = _side_obj(t, side)
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

        ask1, _sz1_top = _get_ask_and_sz(t1, side1)
        ask2, _sz2_top = _get_ask_and_sz(t2, side2)

        if ask1 is None or ask2 is None:
            continue
        if ask1 <= 0.0 or ask2 <= 0.0:
            continue
        if ask1 >= 1.0 or ask2 >= 1.0:
            continue

        # Use VWAP-effective asks (depth-aware) for sizing and profitability.
        levels1 = _levels_asks(t1, side1)
        levels2 = _levels_asks(t2, side2)
        depth1 = _depth_shares_from_asks(levels1)
        depth2 = _depth_shares_from_asks(levels2)
        if depth1 <= 0.0 or depth2 <= 0.0:
            continue

        # First estimate with top-of-book ask as an initial sizing hint.
        # We'll later re-evaluate costs with VWAP at the final q.
        s_top = float(ask1) + float(ask2)
        if s_top <= 0.0:
            continue

        # Sizing model: buy the SAME number of shares Q on both legs.
        q_limits = [
            depth1,
            depth2,
        ]

        t1_bankroll = poly_bankroll_usd if t1.source == "polymarket" else pred_bankroll_usd
        t2_bankroll = poly_bankroll_usd if t2.source == "polymarket" else pred_bankroll_usd

        q_limits.extend([
            (t1_bankroll / float(ask1)) if t1_bankroll > 0 else 0.0,
            (t2_bankroll / float(ask2)) if t2_bankroll > 0 else 0.0,
            (trader_max_trade_usd / s_top) if trader_max_trade_usd > 0 else 0.0,
        ])

        q = min(q_limits)
        if q <= 0.0:
            continue

        vwap1 = _vwap_for_shares(levels1, q)
        vwap2 = _vwap_for_shares(levels2, q)
        if vwap1 is None or vwap2 is None:
            continue

        bps1 = _buffer_bps_for_source(t1.source)
        bps2 = _buffer_bps_for_source(t2.source)
        eff1 = float(vwap1) * (1.0 + float(bps1) / 10000.0)
        eff2 = float(vwap2) * (1.0 + float(bps2) / 10000.0)

        s_eff = eff1 + eff2
        edge_eff = 1.0 - s_eff
        if edge_eff <= 0.0:
            continue

        pool1_usd = float(vwap1) * depth1
        pool2_usd = float(vwap2) * depth2

        cost1_usd = q * eff1
        cost2_usd = q * eff2
        total_cost_usd = q * s_eff
        profit_usd = q * edge_eff

        if total_cost_usd < min_stake_usd:
            continue
        if profit_usd < min_profit_usd:
            continue

        # Expected profit on total spend (stake) using bundle-arb model:
        # ROI on cost = (1 - s) / s
        roi = edge_eff / s_eff

        prev_best = _best_profit_usd.get(key)
        if prev_best is not None and profit_usd <= prev_best + 1e-6:
            continue

        _best_profit_usd[key] = profit_usd

        if s_eff < 1.0:
            t1_token_id = None
            if t1.meta and t1.meta.token_ids:
                t1_token_id = t1.meta.token_ids.get(side1)
            t2_token_id = None
            if t2.meta and t2.meta.token_ids:
                t2_token_id = t2.meta.token_ids.get(side2)

            payload = {
                "type": "arbitrage",
                "label": label,
                "sum": s_eff,
                "edge": edge_eff,
                "roi": roi,
                "shares": q,
                "stake_usd": total_cost_usd,
                "payout_usd": q,
                "profit_usd": profit_usd,
                "min_stake_usd": min_stake_usd,
                "min_profit_usd": min_profit_usd,
                "analyzer_calc_at": datetime.utcnow().isoformat() + "Z",
                "analyzer_tick_ts_max": max(t1.ts, t2.ts).isoformat(),
                "legs": [
                    {
                        "source": t1.source,
                        "side": side1,
                        "ts": t1.ts.isoformat(),
                        "ask": float(vwap1),
                        "ask_sz": float(depth1),
                        "vwap": float(vwap1),
                        "buffer_bps": float(bps1),
                        "ask_top": float(ask1),
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
                        "ask": float(vwap2),
                        "ask_sz": float(depth2),
                        "vwap": float(vwap2),
                        "buffer_bps": float(bps2),
                        "ask_top": float(ask2),
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
                f"[ARBITRAGE] {label} sum={s_eff:.4f} edge={edge_eff:.4f} roi={roi:.4f} "
                f"shares={q:.2f} cost=${total_cost_usd:.2f} payout=${q:.2f} profit=${profit_usd:.2f} "
                f"| leg1 {t1.source}:{side1}@{t1.ts.isoformat()} vwap={float(vwap1):.4f} top={float(ask1):.4f} depth={depth1:.1f} stake=${cost1_usd:.2f} "
                f"| leg2 {t2.source}:{side2}@{t2.ts.isoformat()} vwap={float(vwap2):.4f} top={float(ask2):.4f} depth={depth2:.1f} stake=${cost2_usd:.2f}"
            )

            _post_opportunity(payload)


@app.post("/ingest")
def ingest(tick: Tick) -> dict[str, str]:
    with _state_lock:
        _last[tick.source] = tick
        _check_arbitrage()
    return {"status": "ok"}
