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
    poly_fee_rate: float | None = None  # dynamic taker fee rate from CLOB API
    end_date: str | None = None


class Tick(BaseModel):
    source: Literal["polymarket", "predict"]
    ts: datetime = Field(..., description="Event timestamp")
    up: SideQuote
    down: SideQuote
    meta: TickMeta | None = None


app = FastAPI()

_state_lock = Lock()
_last: dict[str, Tick] = {}
_last_target_bid: dict[tuple[str, int], float] = {}


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
        print(f"[ANALYZER] skip no_data poly={'yes' if poly else 'no'} pred={'yes' if pred else 'no'}")
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

    def _levels_bids(t: Tick, side: Literal["up", "down"]) -> list[tuple[float, float]]:
        """Bid levels sorted highest-first (best bid at index 0)."""
        q = _side_obj(t, side)
        levels = q.bids or []
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
            return sorted(out, key=lambda x: -x[0])
        if q.bid is None or q.bid_sz <= 0:
            return []
        return [(float(q.bid), float(q.bid_sz))]

    # ════════════════════════════════════════════════════════════════════
    # BID+ASK: Predict maker-bid + Polymarket taker-ask < 1.0
    # Размещаем пассивный LIMIT BID на Predict; хеджируем на Poly после фила.
    # ════════════════════════════════════════════════════════════════════
    predict_fee_bps = float(CFG.predict_fee_bps)
    # Dynamic fee rate: prefer tick meta (from CLOB API), fallback to config
    _poly_meta_rate = None
    if poly and poly.meta:
        _poly_meta_rate = poly.meta.poly_fee_rate
    poly_fee_rate = float(_poly_meta_rate) if _poly_meta_rate is not None and _poly_meta_rate > 0 else float(CFG.poly_fee_rate)
    ba_safety_buffer_bps = float(CFG.ba_safety_buffer_bps)
    ba_min_net_edge_bps = float(CFG.ba_min_net_edge_bps)
    predict_max_bid_price = float(CFG.predict_max_bid_price)
    poly_max_hedge_price = float(CFG.poly_max_hedge_price)
    pred_reserve_usd = float(CFG.pred_reserve_usd)
    poly_reserve_usd = float(CFG.poly_reserve_usd)
    poly_bank_util_max = float(CFG.poly_bank_util_max)
    poly_min_order_usd = float(CFG.poly_min_order_usd)
    predict_min_order_usd = float(CFG.predict_min_order_usd)

    ba_combos: list[tuple[str, Tick, Literal["up", "down"], Tick, Literal["up", "down"]]] = [
        ("BA:UP@poly + DOWN@pred", poly, "up", pred, "down"),
        ("BA:DOWN@poly + UP@pred", poly, "down", pred, "up"),
    ]

    for ba_label, t_poly, side_poly, t_pred, side_pred in ba_combos:
        ba_t1_slot = t_poly.meta.slot if t_poly.meta else None
        ba_t2_slot = t_pred.meta.slot if t_pred.meta else None
        if ba_t1_slot is None or ba_t2_slot is None or ba_t1_slot != ba_t2_slot:
            print(f"[ANALYZER] skip slot_mismatch label={ba_label} poly_slot={ba_t1_slot} pred_slot={ba_t2_slot}")
            continue

        ba_slot_key = int(ba_t1_slot)
        ba_key = (ba_label, ba_slot_key)

        # BID+ASK цена бида ВЫЧИСЛЯЕТСЯ из цены Poly — не из существующей книги Predict.
        # Мы размещаем LIMIT BID на Predict по нашей целевой цене; книга Predict не нужна.
        ask_levels_poly = _levels_asks(t_poly, side_poly)
        if not ask_levels_poly:
            continue

        poly_ask_top = ask_levels_poly[0][0]
        if poly_ask_top <= 0.0:
            continue

        # Фильтр: Poly top-of-book слишком дорог
        if poly_ask_top >= poly_max_hedge_price:
            continue

        pred_fee_frac = predict_fee_bps / 10000.0
        safety_frac = ba_safety_buffer_bps / 10000.0
        poly_slippage_frac = poly_buffer_bps / 10000.0

        # Вычисляем максимальную прибыльную цену бида на Predict (по top-of-book poly)
        _dyn_fee_top = poly_fee_rate * poly_ask_top * (1.0 - poly_ask_top)
        _eff_poly_top = poly_ask_top * (1.0 + poly_slippage_frac) + _dyn_fee_top
        pred_fee_mult = 1.0 + pred_fee_frac
        max_bid = (1.0 - _eff_poly_top - safety_frac) / pred_fee_mult if pred_fee_mult > 0 else 0.0

        if max_bid <= 0.0:
            continue

        # Применяем лимит цены; итоговая target_bid — цена нашего лимитного ордера
        target_bid = min(max_bid, predict_max_bid_price)
        if target_bid <= 0.0:
            continue

        # Размещение идёт только если poly позволяет нормально хеджировать
        depth_poly_ask = _depth_shares_from_asks(ask_levels_poly)
        if depth_poly_ask <= 0.0:
            continue

        q_ba = min(
            depth_poly_ask,
            (max(0.0, poly_bankroll_usd - poly_reserve_usd) / poly_ask_top) if poly_bankroll_usd > 0 else depth_poly_ask,
            (max(0.0, pred_bankroll_usd - pred_reserve_usd) / target_bid) if pred_bankroll_usd > 0 else depth_poly_ask,
            (trader_max_trade_usd / (target_bid + poly_ask_top)) if trader_max_trade_usd > 0 else depth_poly_ask,
        )
        # Floor to 2 decimal places: keeps order sizes clean on both exchanges.
        # Non-round q_ba causes Predict bid to be filled in fragments and Poly hedge
        # to be an odd number of shares (e.g. 10.3401 instead of 10.34).
        q_ba = int(q_ba * 100) / 100
        if q_ba <= 0.0:
            continue

        # VWAP на Poly для точного расчёта по фактическому размеру
        vwap_poly_ask = _vwap_for_shares(ask_levels_poly, q_ba)
        if vwap_poly_ask is None:
            continue

        # Фильтр: VWAP poly слишком высок
        if vwap_poly_ask >= poly_max_hedge_price:
            continue

        # Уточняем динамическую комиссию по VWAP
        poly_dynamic_fee = poly_fee_rate * vwap_poly_ask * (1.0 - vwap_poly_ask)
        eff_poly_ask = vwap_poly_ask * (1.0 + poly_slippage_frac) + poly_dynamic_fee

        # Наш бид на Predict = target_bid (worst-case edge: платим полную target_bid)
        vwap_pred_bid = target_bid
        eff_pred_bid = vwap_pred_bid * pred_fee_mult

        ba_s_eff = eff_pred_bid + eff_poly_ask + safety_frac
        ba_edge = 1.0 - ba_s_eff
        ba_net_edge_bps = ba_edge * 10000.0

        # net_edge уже гарантирован safety_frac внутри max_bid.
        # Дополнительная проверка нужна только если ba_min_net_edge_bps > 0.
        if ba_min_net_edge_bps > 0 and ba_net_edge_bps < ba_min_net_edge_bps:
            print(f"[ANALYZER] skip low_edge label={ba_label} net_edge_bps={ba_net_edge_bps:.1f} min={ba_min_net_edge_bps} poly_ask={poly_ask_top:.4f} target_bid={target_bid:.4f}")
            continue

        ba_cost_pred = q_ba * eff_pred_bid
        ba_cost_poly = q_ba * eff_poly_ask
        ba_total_cost = ba_cost_pred + ba_cost_poly
        ba_profit = q_ba * ba_edge
        ba_roi = ba_edge / ba_s_eff

        if ba_total_cost < min_stake_usd:
            continue
        if ba_profit < min_profit_usd:
            continue

        # Фильтр: poly стоимость превышает лимит использования банкролла
        if poly_bankroll_usd > 0 and ba_cost_poly > poly_bank_util_max * poly_bankroll_usd:
            continue

        # Фильтр: минимальные размеры ордеров (poly = немедленный hedge, pred = limit bid)
        if ba_cost_poly < poly_min_order_usd:
            continue
        if ba_cost_pred < predict_min_order_usd:
            continue

        prev_target = _last_target_bid.get(ba_key)
        if prev_target is not None and abs(target_bid - prev_target) < 0.01 - 1e-9:
            continue
        _last_target_bid[ba_key] = target_bid

        ba_poly_token_id = None
        if t_poly.meta and t_poly.meta.token_ids:
            ba_poly_token_id = t_poly.meta.token_ids.get(side_poly)
        ba_pred_token_id = None
        if t_pred.meta and t_pred.meta.token_ids:
            ba_pred_token_id = t_pred.meta.token_ids.get(side_pred)

        ba_payload = {
            "type": "bid_ask_arbitrage",
            "label": ba_label,
            "sum": ba_s_eff,
            "edge": ba_edge,
            "net_edge_bps": round(ba_net_edge_bps, 2),
            "roi": ba_roi,
            "shares": q_ba,
            "stake_usd": ba_total_cost,
            "payout_usd": q_ba,
            "profit_usd": ba_profit,
            "min_stake_usd": min_stake_usd,
            "min_profit_usd": min_profit_usd,
            "poly_dynamic_fee": round(poly_dynamic_fee, 6),
            "poly_fee_rate": poly_fee_rate,
            "predict_fee_bps": predict_fee_bps,
            "safety_buffer_bps": ba_safety_buffer_bps,
            "predict_max_bid_price": predict_max_bid_price,
            "analyzer_calc_at": datetime.utcnow().isoformat() + "Z",
            "analyzer_tick_ts_max": max(t_poly.ts, t_pred.ts).isoformat(),
            "legs": [
                {
                    "source": t_poly.source,
                    "side": side_poly,
                    "ts": t_poly.ts.isoformat(),
                    "ask": float(vwap_poly_ask),
                    "ask_sz": float(depth_poly_ask),
                    "vwap": float(vwap_poly_ask),
                    "buffer_bps": float(poly_buffer_bps),
                    "ask_top": float(poly_ask_top),
                    "pool_usd": float(vwap_poly_ask) * depth_poly_ask,
                    "shares": q_ba,
                    "stake_usd": ba_cost_poly,
                    "token_id": ba_poly_token_id,
                    "market_id": t_poly.meta.market_id if t_poly.meta else None,
                    "title": t_poly.meta.title if t_poly.meta else None,
                },
                {
                    "source": t_pred.source,
                    "side": side_pred,
                    "ts": t_pred.ts.isoformat(),
                    "ask": float(target_bid),      # = наша целевая цена лимитного бида
                    "ask_sz": float(q_ba),
                    "vwap": float(vwap_pred_bid),
                    "buffer_bps": 0.0,
                    "ask_top": float(target_bid),
                    "pool_usd": float(vwap_pred_bid) * q_ba,
                    "shares": q_ba,
                    "stake_usd": ba_cost_pred,
                    "token_id": ba_pred_token_id,
                    "market_id": t_pred.meta.market_id if t_pred.meta else None,
                    "title": t_pred.meta.title if t_pred.meta else None,
                    "pred_best_bid": float(_side_obj(t_pred, side_pred).bid) if _side_obj(t_pred, side_pred).bid is not None else None,
                    "pred_best_bid_sz": float(_side_obj(t_pred, side_pred).bid_sz),
                },
            ],
            "sent_at": datetime.utcnow().isoformat() + "Z",
            "end_date": t_pred.meta.end_date if t_pred.meta else None,
        }
        print(
            f"[BID+ASK] {ba_label} sum={ba_s_eff:.4f} edge={ba_edge:.4f} net_edge_bps={ba_net_edge_bps:.1f} "
            f"roi={ba_roi:.4f} shares={q_ba:.2f} cost=${ba_total_cost:.2f} profit=${ba_profit:.2f} "
            f"| poly({side_poly}) ask_vwap={vwap_poly_ask:.4f} eff={eff_poly_ask:.4f} top={poly_ask_top:.4f} "
            f"depth={depth_poly_ask:.1f} fee={poly_dynamic_fee:.4f} "
            f"| pred({side_pred}) target_bid={target_bid:.4f} max_bid={max_bid:.4f} eff={eff_pred_bid:.4f} "
            f"| safety={ba_safety_buffer_bps:.0f}bps"
        )
        _post_opportunity(ba_payload)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/predict_book")
def predict_book() -> dict:
    """Return latest Predict.fun book state (cached from collector, updated ~every 100ms)."""
    with _state_lock:
        pred = _last.get("predict")
    if pred is None:
        return {"status": "no_data"}
    return {
        "status": "ok",
        "ts": pred.ts.isoformat(),
        "up": pred.up.model_dump(),
        "down": pred.down.model_dump(),
    }


@app.post("/ingest")
def ingest(tick: Tick) -> dict[str, str]:
    with _state_lock:
        _last[tick.source] = tick
        _check_arbitrage()
    return {"status": "ok"}
