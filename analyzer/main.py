from __future__ import annotations

from datetime import datetime
import os
from threading import Lock
from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel, Field


class SideQuote(BaseModel):
    bid: float | None = None
    bid_sz: float = 0.0
    ask: float | None = None
    ask_sz: float = 0.0


class Tick(BaseModel):
    source: Literal["polymarket", "predict"]
    ts: datetime = Field(..., description="Event timestamp")
    up: SideQuote
    down: SideQuote


app = FastAPI()

_state_lock = Lock()
_last: dict[str, Tick] = {}
_best_profit_usd: dict[str, float] = {}


def _check_arbitrage() -> None:
    poly = _last.get("polymarket")
    pred = _last.get("predict")
    if not poly or not pred:
        return

    bankroll_usd = float(os.environ.get("BANKROLL_USD", "100"))
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
        ask1, sz1 = _get_ask_and_sz(t1, side1)
        ask2, sz2 = _get_ask_and_sz(t2, side2)

        if ask1 is None or ask2 is None:
            continue
        if ask1 <= 0.0 or ask2 <= 0.0:
            continue
        if ask1 >= 1.0 or ask2 >= 1.0:
            continue

        # Liquidity / pool: how many USD we can realistically spend on each leg
        # (as user-defined approximation): pool_usd_leg = ask * ask_sz
        # Use the lower pool as max stake, also capped by bankroll.
        pool1_usd = ask1 * max(0.0, float(sz1))
        pool2_usd = ask2 * max(0.0, float(sz2))

        t1_bankroll = poly_bankroll_usd if t1.source == "polymarket" else pred_bankroll_usd
        t2_bankroll = poly_bankroll_usd if t2.source == "polymarket" else pred_bankroll_usd

        max_stake_usd = min(pool1_usd, pool2_usd, bankroll_usd, t1_bankroll, t2_bankroll)
        if max_stake_usd < min_stake_usd:
            continue

        s = ask1 + ask2
        edge = 1.0 - s
        if edge <= 0.0:
            continue

        # Expected profit on total spend (stake) using bundle-arb model:
        # ROI on cost = (1 - s) / s
        roi = edge / s
        profit_usd = max_stake_usd * roi
        if profit_usd < min_profit_usd:
            continue

        prev_best = _best_profit_usd.get(label)
        if prev_best is not None and profit_usd <= prev_best + 1e-6:
            continue

        _best_profit_usd[label] = profit_usd

        if s < 1.0:
            print(
                f"[ARBITRAGE] {label} sum={s:.4f} edge={edge:.4f} roi={roi:.4f} "
                f"stake=${max_stake_usd:.2f} profit=${profit_usd:.2f} "
                f"| leg1 {t1.source}:{side1}@{t1.ts.isoformat()} ask={ask1} sz={float(sz1):.1f} pool=${pool1_usd:.2f} "
                f"| leg2 {t2.source}:{side2}@{t2.ts.isoformat()} ask={ask2} sz={float(sz2):.1f} pool=${pool2_usd:.2f}"
            )


@app.post("/ingest")
def ingest(tick: Tick) -> dict[str, str]:
    with _state_lock:
        _last[tick.source] = tick
        _check_arbitrage()
    return {"status": "ok"}
