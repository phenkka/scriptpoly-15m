from __future__ import annotations

from datetime import datetime
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


def _check_arbitrage() -> None:
    poly = _last.get("polymarket")
    pred = _last.get("predict")
    if not poly or not pred:
        return

    # Buy UP on A and DOWN on B => ask_up(A) + ask_down(B) < 1
    combos = [
        ("UP@poly + DOWN@pred", poly.up.ask, pred.down.ask, poly, pred),
        ("DOWN@poly + UP@pred", poly.down.ask, pred.up.ask, poly, pred),
        ("UP@pred + DOWN@poly", pred.up.ask, poly.down.ask, pred, poly),
        ("DOWN@pred + UP@poly", pred.down.ask, poly.up.ask, pred, poly),
    ]

    for label, ask1, ask2, t1, t2 in combos:
        if ask1 is None or ask2 is None:
            continue
        s = ask1 + ask2
        if s < 1.0:
            print(
                f"[ARBITRAGE] {label} sum={s:.4f} "
                f"| {t1.source}@{t1.ts.isoformat()} "
                f"UP(ask={t1.up.ask}) DOWN(ask={t1.down.ask}) "
                f"| {t2.source}@{t2.ts.isoformat()} "
                f"UP(ask={t2.up.ask}) DOWN(ask={t2.down.ask})"
            )


@app.post("/ingest")
def ingest(tick: Tick) -> dict[str, str]:
    with _state_lock:
        _last[tick.source] = tick
        _check_arbitrage()
    return {"status": "ok"}
