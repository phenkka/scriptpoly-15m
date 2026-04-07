"""Telegram-бот для мониторинга scriptpoly.

Команды:
  /start   — приветствие
  /help    — список команд

Push-уведомления (отправляются сервисами через HTTP POST /notify):
  • Успешный трейд
  • Успешный клейм
  • Инцидент
  • Балансировка
"""
from __future__ import annotations

import logging
import os

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


# ── Handlers ──────────────────────────────────────────────────────────────────

@router.message(Command("start", "help"))
async def cmd_help(message: Message) -> None:
    await message.answer(
        "🤖 <b>scriptpoly bot</b>\n\n"
        "/help    — this message",
        parse_mode="HTML",
    )


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
        reply_to = body.get("reply_to_message_id")
        reply_to_id = int(reply_to) if reply_to else None
        msg = await bot.send_message(
            chat_id, text, parse_mode="HTML",
            reply_to_message_id=reply_to_id,
        )
        return web.json_response({"ok": True, "message_id": msg.message_id})
    except Exception as e:
        log.error(f"notify_error {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    token   = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    port    = int(os.environ.get("BOT_PORT", "8080"))
    proxy   = os.environ.get("PROXY_URL", "").strip() or None

    from aiogram.client.session.aiohttp import AiohttpSession
    if proxy:
        session = AiohttpSession(proxy=proxy)
    else:
        session = AiohttpSession()
    bot = Bot(token=token, session=session)
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

    # Polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
