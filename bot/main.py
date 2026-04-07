"""Telegram-бот для мониторинга scriptpoly.

Команды:
  /start   — приветствие
  /stats   — сводная статистика (трейды, клеймы, инциденты)
  /trades  — последние 5 трейдов
  /claims  — последние 5 клеймов
  /help    — список команд

Push-уведомления (отправляются сервисами через HTTP POST /notify):
  • Успешный трейд
  • Успешный клейм
  • Инцидент
  • Балансировка
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aiohttp import web
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BOT] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

router = Router()

# ── Data helpers ─────────────────────────────────────────────────────────────

def _read_jsonl(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    out = []
    for line in p.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def _fmt_ts(ts: str | None) -> str:
    if not ts:
        return "?"
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%d %b %H:%M")
    except Exception:
        return ts[:16]


def _compute_stats() -> str:
    trades_file  = os.environ.get("TRADER_TRADES_FILE", "/data/trades.jsonl")
    incidents_file = os.environ.get("INCIDENTS_FILE", "/data/incidents.jsonl")
    claims_file  = os.environ.get("CLAIMS_FILE", "/data/claims.jsonl")

    trades    = _read_jsonl(trades_file)
    incidents = _read_jsonl(incidents_file)
    claims    = _read_jsonl(claims_file)

    # Трейды
    attempted = [t for t in trades if not t.get("skipped")]
    ok_trades = [t for t in attempted if t.get("ok")]
    total_stake  = sum(t.get("stake_usd") or 0 for t in ok_trades)
    total_profit = sum(t.get("profit_usd") or 0 for t in ok_trades)
    roi = (total_profit / total_stake * 100) if total_stake > 0 else 0.0
    win_rate = (len(ok_trades) / len(attempted) * 100) if attempted else 0.0

    # Клеймы
    total_claimed = sum(c.get("amount_usd") or 0 for c in claims)

    lines = [
        "📊 <b>Статистика</b>",
        "",
        f"<b>Трейды</b>",
        f"  Попыток: {len(attempted)}  •  Успешных: {len(ok_trades)}",
        f"  Win-rate: {win_rate:.1f}%",
        f"  Поставлено: ${total_stake:.2f}",
        f"  Прибыль: ${total_profit:+.2f}  ({roi:+.1f}%)",
        "",
        f"<b>Инциденты</b>: {len(incidents)}",
        "",
        f"<b>Клеймы</b>: {len(claims)}  •  Сумма: ${total_claimed:.2f}",
    ]
    return "\n".join(lines)


def _last_trades(n: int = 5) -> str:
    trades_file = os.environ.get("TRADER_TRADES_FILE", "/data/trades.jsonl")
    trades = [t for t in _read_jsonl(trades_file) if not t.get("skipped")]
    trades = trades[-n:]
    if not trades:
        return "Нет трейдов."
    lines = ["📈 <b>Последние трейды</b>", ""]
    for t in reversed(trades):
        ok   = "✅" if t.get("ok") else "❌"
        ts   = _fmt_ts(t.get("ts"))
        lbl  = t.get("label", "?")
        stk  = t.get("stake_usd") or 0
        prf  = t.get("profit_usd") or 0
        lines.append(f"{ok} {ts}  <b>{lbl}</b>")
        lines.append(f"   stake=${stk:.2f}  profit=${prf:+.2f}")
    return "\n".join(lines)


def _last_claims(n: int = 5) -> str:
    claims_file = os.environ.get("CLAIMS_FILE", "/data/claims.jsonl")
    claims = _read_jsonl(claims_file)[-n:]
    if not claims:
        return "Клеймов пока нет."
    lines = ["💰 <b>Последние клеймы</b>", ""]
    for c in reversed(claims):
        ts     = _fmt_ts(c.get("ts"))
        src    = c.get("source", "?")
        title  = c.get("title", "?")
        amount = c.get("amount_usd") or 0
        tx     = c.get("tx_hash", "")[:14]
        lines.append(f"✅ {ts}  [{src}]  <b>{title}</b>")
        lines.append(f"   ${amount:.2f}  tx={tx}...")
    return "\n".join(lines)


# ── Handlers ──────────────────────────────────────────────────────────────────

@router.message(Command("start", "help"))
async def cmd_help(message: Message) -> None:
    await message.answer(
        "🤖 <b>scriptpoly бот</b>\n\n"
        "/stats   — сводная статистика\n"
        "/trades  — последние трейды\n"
        "/claims  — последние клеймы\n"
        "/help    — эта справка",
        parse_mode="HTML",
    )


@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    await message.answer(_compute_stats(), parse_mode="HTML")


@router.message(Command("trades"))
async def cmd_trades(message: Message) -> None:
    await message.answer(_last_trades(), parse_mode="HTML")


@router.message(Command("claims"))
async def cmd_claims(message: Message) -> None:
    await message.answer(_last_claims(), parse_mode="HTML")


# ── HTTP /notify endpoint (вызывается другими сервисами) ─────────────────────

async def handle_notify(request: web.Request) -> web.Response:
    """POST /notify  body: {"text": "..."}  — отправляет сообщение в чат."""
    bot: Bot = request.app["bot"]
    chat_id: str = request.app["chat_id"]
    try:
        body = await request.json()
        text = str(body.get("text", "")).strip()
        if not text:
            return web.json_response({"ok": False, "error": "empty text"}, status=400)
        await bot.send_message(chat_id, text, parse_mode="HTML")
        return web.json_response({"ok": True})
    except Exception as e:
        log.error(f"notify_error {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    token   = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    port    = int(os.environ.get("BOT_PORT", "8080"))

    bot = Bot(token=token)
    dp  = Dispatcher()
    dp.include_router(router)

    # aiohttp для /notify
    app = web.Application()
    app["bot"]     = bot
    app["chat_id"] = chat_id
    app.router.add_post("/notify", handle_notify)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"bot_started notify_port={port}")

    # Стартовое сообщение
    try:
        await bot.send_message(chat_id, "🟢 <b>scriptpoly запущен</b>", parse_mode="HTML")
    except Exception as e:
        log.warning(f"startup_notify_failed {e}")

    # Polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
