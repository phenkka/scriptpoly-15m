"""Telegram-бот для мониторинга scriptpoly.

Команды:
  /start, /help  — приветствие
  /settings      — настройка параметров бота (многостраничный редактор)

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
from pathlib import Path
from typing import Any

from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BOT] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

router = Router()

# Set in main() after reading env
_allowed_chat_id: int = 0

# chat_id → {key, typ, default, page, prompt_msg_id}  — replaces FSM
_pending_edit: dict[int, dict] = {}

# ── Settings storage ──────────────────────────────────────────────────────────

SETTINGS_FILE = Path("/data/settings.json")

# (env_key, display_label, type, env_default)
# type: "int" | "float" | "bool"
PAGE_GROUPS: list[tuple[str, list[tuple[str, str, str, Any]]]] = [
    ("⚔️ Strategy", [
        ("BA_SAFETY_BUFFER_BPS",               "Safety buffer bps",    "int",   300),
        ("BA_MIN_NET_EDGE_BPS",                "Min net edge bps",     "int",   0),
        ("PREDICT_PASSIVE_BID_MAX_TICKS_MISS", "Max ticks miss",       "int",   0),
        ("PREDICT_MAX_BID_PRICE",              "Max bid price",        "float", 0.99),
        ("POLY_MAX_HEDGE_PRICE",               "Max hedge price",      "float", 0.99),
    ]),
    ("📊 Queue / VWAP", [
        ("PREDICT_QUEUE_THRESHOLD_USD",        "Queue threshold $",    "float", 20.0),
        ("PREDICT_HARD_MAX_QUEUE_USD",         "Hard max queue $",     "float", 100.0),
        ("PRED_VWAP_BUFFER_BPS",               "Pred VWAP buffer bps", "int",   200),
        ("POLY_VWAP_BUFFER_BPS",               "Poly VWAP buffer bps", "int",   30),
        ("PREDICT_SLIPPAGE_BPS",               "Pred slippage bps",    "int",   200),
    ]),
    ("💰 Capital", [
        ("TRADER_MAX_TRADE_USD",               "Max trade $",          "float", 5.0),
        ("POLY_BANKROLL_USD",                  "Poly bankroll $",      "float", 10.0),
        ("PRED_BANKROLL_USD",                  "Pred bankroll $",      "float", 10.0),
        ("PRED_RESERVE_USD",                   "Pred reserve $",       "float", 0.50),
        ("POLY_RESERVE_USD",                   "Poly reserve $",       "float", 0.75),
        ("BALANCER_THRESHOLD_USD",             "Rebalance threshold $", "float", 10.0),
        ("BALANCER_TARGET_USD",                "Rebalance target $",   "float", 25.0),
        ("BALANCER_ENABLE_TRANSFERS",          "Enable transfers",     "bool",  False),
        ("BOT_STOP_TOTAL_USD",                 "Stop bot below $",     "float", 25.0),
    ]),
    ("🚦 Limits", [
        ("POLY_BANK_UTIL_MAX",                 "Poly util max",        "float", 0.85),
        ("MIN_PROFIT_USD",                     "Min profit $",         "float", 0.0),
        ("MIN_STAKE_USD",                      "Min stake $",          "float", 1.0),
        ("POLY_MIN_ORDER_USD",                 "Poly min order $",     "float", 1.0),
        ("PREDICT_MIN_ORDER_USD",              "Pred min order $",     "float", 0.9),
    ]),
    ("⏱ Timers", [
        ("PREDICT_LIMIT_FILL_TIMEOUT_SEC",     "Limit fill timeout s", "float", 30.0),
        ("PREDICT_QUOTE_TTL_SEC",              "Quote TTL s",          "float", 3.0),
        ("PREDICT_QUOTE_MAX_REPLACE",          "Max replaces",         "int",   3),
        ("PREDICT_MARKET_COOLDOWN_SEC",        "Market cooldown s",    "float", 30.0),
        ("PREDICT_FILL_TIMEOUT_SEC",           "FOK fill timeout s",   "float", 3.0),
    ]),
    ("⚙️ Fees / Misc", [
        ("PREDICT_FEE_BPS",                    "Predict fee bps",      "float", 0.0),
        ("POLY_FEE_RATE",                      "Poly fee rate",        "float", 0.072),
        ("PREDICT_TICK_SIZE",                  "Tick size",            "float", 0.01),
        ("TRADER_DRY_RUN",                     "Dry run",              "bool",  False),
        ("TRADER_TEST_MODE",                   "Test mode",            "bool",  False),
    ]),
]

# Flat lookup: env_key → (label, type, env_default)
_ALL_SETTINGS: dict[str, tuple[str, str, Any]] = {
    key: (label, typ, default)
    for _, defs in PAGE_GROUPS
    for key, label, typ, default in defs
}


def _read_settings() -> dict:
    try:
        return json.loads(SETTINGS_FILE.read_text())
    except Exception:
        return {}


def _write_setting(key: str, value: Any) -> None:
    s = _read_settings()
    s[key] = value
    SETTINGS_FILE.write_text(json.dumps(s, indent=2, ensure_ascii=False))


def _current_value(key: str, default: Any) -> Any:
    s = _read_settings()
    if key in s:
        return s[key]
    env = os.environ.get(key)
    if env is not None:
        return env
    return default


# ── Message / keyboard builders ───────────────────────────────────────────────

def _format_value(v: Any, typ: str) -> str:
    if typ == "float":
        f = float(v)
        return f"{f:g}"
    if typ == "int":
        return str(int(float(v)))
    return str(v)


def _settings_text(page: int) -> str:
    _, defs = PAGE_GROUPS[page]
    s = _read_settings()
    lines = ["⚙️ <b>SETTINGS</b>", ""]
    for key, label, typ, default in defs:
        raw = s.get(key, os.environ.get(key, str(default)))
        cur = _format_value(raw, typ)
        env_raw = os.environ.get(key, str(default))
        env_str = _format_value(env_raw, typ)
        star = " ✏️" if cur != env_str else ""
        lines.append(f"<code>{key}</code> = <b>{cur}</b>{star}")
    return "\n".join(lines)


def _settings_keyboard(page: int) -> InlineKeyboardMarkup:
    _, defs = PAGE_GROUPS[page]
    n_pages = len(PAGE_GROUPS)
    s = _read_settings()
    builder = InlineKeyboardBuilder()
    for key, label, typ, default in defs:
        raw = s.get(key, os.environ.get(key, str(default)))
        cur = _format_value(raw, typ)
        builder.button(
            text=f"{label}: {cur}",
            callback_data=f"set:edit:{key}",
        )
    builder.adjust(1)

    # Navigation row
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀", callback_data=f"set:page:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1} / {n_pages}", callback_data="set:noop"))
    if page < n_pages - 1:
        nav.append(InlineKeyboardButton(text="▶", callback_data=f"set:page:{page + 1}"))
    builder.row(*nav)
    builder.row(InlineKeyboardButton(text="✖ Exit", callback_data="set:exit"))

    return builder.as_markup()


# ── Handlers ──────────────────────────────────────────────────────────────────
@router.message(Command("settings"))
async def cmd_settings(message: Message) -> None:
    if message.chat.id != _allowed_chat_id:
        return
    _pending_edit.pop(message.chat.id, None)
    try:
        await message.delete()
    except Exception:
        pass
    await message.answer(
        _settings_text(0),
        parse_mode="HTML",
        reply_markup=_settings_keyboard(0),
    )


@router.message(Command("balance"))
async def cmd_balance(message: Message) -> None:
    if message.chat.id != _allowed_chat_id:
        return
    wait_msg = await message.answer("⏳ Fetching live on-chain balance…")
    live_url = os.environ.get("BALANCER_LIVE_URL", "http://balancer:8081/balance/live")
    try:
        import aiohttp
        async with aiohttp.ClientSession() as sess:
            async with sess.get(live_url, timeout=30) as resp:
                data = await resp.json()
    except Exception as e:
        await wait_msg.edit_text(f"❌ Failed to fetch balance: {e}")
        return

    def _fmt(v) -> str:
        try:
            return f"${float(v):.2f}"
        except Exception:
            return "—"

    def _fmt_delta(v) -> str:
        try:
            f = float(v)
            sign = "+" if f >= 0 else ""
            return f"{sign}${f:.2f}"
        except Exception:
            return "—"

    poly_cash = data.get("poly_cash")
    poly_port = data.get("poly_portfolio")
    poly_total = data.get("poly_total")
    pred_cash = data.get("pred_cash")
    pred_port = data.get("predict_portfolio")
    pred_total = data.get("pred_total")
    total = data.get("total_with_pos")
    pnl = data.get("pnl_from_trades")
    trade_count = data.get("trade_count", 0)
    balance_delta = data.get("balance_delta")
    baseline_total = data.get("baseline_total")
    baseline_ts = data.get("baseline_ts", "")
    fetched_at = data.get("fetched_at", "")[:19].replace("T", " ")

    poly_pos = (poly_port or 0) - 0
    pred_pos = (pred_port or 0) - 0

    def _side_line(name: str, cash, portfolio, total_v) -> str:
        if portfolio and float(portfolio) > 0.01:
            return f"{name}: <b>{_fmt(total_v)}</b>  <i>(cash {_fmt(cash)} + pos {_fmt(portfolio)})</i>\n"
        return f"{name}: <b>{_fmt(cash)}</b>\n"

    text = (
        f"<b>📊 LIVE BALANCE</b>  <i>{fetched_at} UTC</i>\n\n"
        + _side_line("Polymarket", poly_cash, poly_port, poly_total)
        + _side_line("Predict", pred_cash, pred_port, pred_total)
        + f"\n<b>TOTAL: {_fmt(total)}</b>\n"
    )

    if baseline_total is not None:
        bl_date = (baseline_ts or "")[:10]
        delta_emoji = "📈" if (balance_delta or 0) >= 0 else "📉"
        text += f"\n{delta_emoji} vs baseline ({bl_date}): <b>{_fmt_delta(balance_delta)}</b>  <i>(was {_fmt(baseline_total)})</i>\n"

    if pnl is not None:
        pnl_emoji = "📈" if float(pnl) >= 0 else "📉"
        text += f"{pnl_emoji} PnL from trades ({trade_count} trades): <b>{_fmt_delta(pnl)}</b>\n"

    if baseline_total is None:
        text += "\n<i>💡 Use /setbaseline to set current balance as reference point for PnL tracking</i>\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📍 Set as baseline", callback_data="balance:set_baseline"),
    ]])
    try:
        await wait_msg.delete()
    except Exception:
        pass
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.message(Command("setbaseline"))
async def cmd_setbaseline(message: Message) -> None:
    if message.chat.id != _allowed_chat_id:
        return
    wait_msg = await message.answer("⏳ Fetching live balance to set baseline…")
    baseline_url = os.environ.get("BALANCER_BASELINE_URL", "http://balancer:8081/balance/baseline")
    try:
        import aiohttp
        async with aiohttp.ClientSession() as sess:
            async with sess.post(baseline_url, timeout=30) as resp:
                data = await resp.json()
    except Exception as e:
        await wait_msg.edit_text(f"❌ Failed: {e}")
        return
    if data.get("ok"):
        ts = (data.get("ts") or "")[:19].replace("T", " ")
        await wait_msg.edit_text(
            f"✅ Baseline set: <b>${float(data['baseline']):.2f}</b>  <i>({ts} UTC)</i>\n\n"
            f"Future /balance will show balance change relative to this point.",
            parse_mode="HTML",
        )
    else:
        await wait_msg.edit_text(f"❌ Error: {data.get('error')}")


@router.callback_query(F.data == "balance:set_baseline")
async def cb_set_baseline(callback: CallbackQuery) -> None:
    if callback.message.chat.id != _allowed_chat_id:
        await callback.answer()
        return
    await callback.answer("Setting baseline…")
    baseline_url = os.environ.get("BALANCER_BASELINE_URL", "http://balancer:8081/balance/baseline")
    try:
        import aiohttp
        async with aiohttp.ClientSession() as sess:
            async with sess.post(baseline_url, timeout=30) as resp:
                data = await resp.json()
        if data.get("ok"):
            ts = (data.get("ts") or "")[:19].replace("T", " ")
            await callback.message.answer(
                f"✅ Baseline set: <b>${float(data['baseline']):.2f}</b>  <i>({ts} UTC)</i>",
                parse_mode="HTML",
            )
        else:
            await callback.message.answer(f"❌ Error: {data.get('error')}")
    except Exception as e:
        await callback.message.answer(f"❌ Failed: {e}")


@router.message(Command("cancel"))
async def cmd_cancel(message: Message) -> None:
    if message.chat.id != _allowed_chat_id:
        return
    _pending_edit.pop(message.chat.id, None)
    await message.answer("❌ Cancelled.", parse_mode="HTML")


@router.callback_query(F.data == "set:noop")
async def cb_noop(callback: CallbackQuery) -> None:
    await callback.answer()


@router.callback_query(F.data == "set:exit")
async def cb_settings_exit(callback: CallbackQuery) -> None:
    chat_id = callback.message.chat.id
    _pending_edit.pop(chat_id, None)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("set:page:"))
async def cb_settings_page(callback: CallbackQuery) -> None:
    page = int(callback.data.split(":")[2])
    await callback.message.edit_text(
        _settings_text(page),
        parse_mode="HTML",
        reply_markup=_settings_keyboard(page),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("set:edit:"))
async def cb_settings_edit(callback: CallbackQuery) -> None:
    key = callback.data.split(":", 2)[2]
    if key not in _ALL_SETTINGS:
        await callback.answer("Unknown setting", show_alert=True)
        return

    page = 0
    for i, (_, defs) in enumerate(PAGE_GROUPS):
        if any(k == key for k, *_ in defs):
            page = i
            break

    label, typ, default = _ALL_SETTINGS[key]
    cur = _current_value(key, default)
    env_val = os.environ.get(key, str(default))

    try:
        await callback.message.delete()
    except Exception:
        pass

    hint = "true/false" if typ == "bool" else typ
    prompt = await callback.message.answer(
        f"✏️ <b>{label}</b>\n"
        f"<code>{key}</code>\n\n"
        f"Current:  <code>{_format_value(cur, typ)}</code>\n"
        f"Default:  <code>{_format_value(env_val, typ)}</code>\n\n"
        f"Enter new value ({hint})\n"
        f"or /cancel to abort:",
        parse_mode="HTML",
        reply_markup=ForceReply(selective=False),
    )

    _pending_edit[callback.message.chat.id] = {
        "key": key,
        "typ": typ,
        "default": default,
        "page": page,
        "prompt_msg_id": prompt.message_id,
    }
    await callback.answer()


@router.message()
async def handle_any_message(message: Message) -> None:
    if message.chat.id != _allowed_chat_id:
        return

    edit = _pending_edit.get(message.chat.id)
    if not edit:
        return

    key: str = edit["key"]
    typ: str = edit["typ"]
    default = edit["default"]
    page: int = edit["page"]
    prompt_msg_id: int = edit["prompt_msg_id"]

    raw = (message.text or "").strip()

    # Ignore commands — let dedicated handlers deal with them
    if raw.startswith("/"):
        return

    try:
        if typ == "int":
            value: Any = int(raw)
        elif typ == "float":
            value = float(raw)
        elif typ == "bool":
            if raw.lower() in {"1", "true", "yes", "y", "on"}:
                value = True
            elif raw.lower() in {"0", "false", "no", "n", "off"}:
                value = False
            else:
                raise ValueError
        else:
            value = raw
    except (ValueError, TypeError):
        await message.answer(
            f"❌ Invalid format. Expected <b>{typ}</b>.\n"
            f"Try again or /cancel:",
            parse_mode="HTML",
        )
        return

    try:
        _write_setting(key, value)
    except Exception as e:
        _pending_edit.pop(message.chat.id, None)
        await message.answer(f"❌ Failed to save: <code>{e}</code>", parse_mode="HTML")
        return

    _pending_edit.pop(message.chat.id, None)

    try:
        await message.bot.delete_message(message.chat.id, prompt_msg_id)
    except Exception:
        pass
    try:
        await message.delete()
    except Exception:
        pass

    await message.answer(
        _settings_text(page),
        parse_mode="HTML",
        reply_markup=_settings_keyboard(page),
    )


# ── HTTP /notify endpoint (вызывается другими сервисами) ─────────────────────

async def handle_notify(request: web.Request) -> web.Response:
    """POST /notify  body: {"text": "..."}  — отправляет сообщение в чат."""
    bot: Bot = request.app["bot"]
    # store chat_id as string in app; convert to int for send_message
    try:
        chat_id = int(request.app["chat_id"])
    except Exception:
        chat_id = request.app.get("chat_id")

    try:
        # Accept JSON, form-encoded, or raw text bodies for compatibility
        try:
            body = await request.json()
        except Exception:
            raw = (await request.text()).strip()
            body = {"text": raw} if raw else {}

        text = str(body.get("text", "")).strip()
        if not text:
            return web.json_response({"ok": False, "error": "empty text"}, status=400)

        reply_to = body.get("reply_to_message_id") or body.get("reply_to")
        reply_to_id = int(reply_to) if reply_to else None

        log.info(f"notify: sending to chat_id={chat_id} reply_to={reply_to_id}")

        msg = await bot.send_message(
            chat_id, text, parse_mode="HTML",
            reply_to_message_id=reply_to_id,
        )
        return web.json_response({"ok": True, "message_id": msg.message_id})
    except Exception as e:
        log.exception("notify_error")
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
    global _allowed_chat_id
    _allowed_chat_id = int(chat_id)

    bot = Bot(token=token, session=session)
    dp  = Dispatcher()
    dp.include_router(router)

    @web.middleware
    async def _catch_mw(request: web.Request, handler: Any) -> Any:
        try:
            return await handler(request)
        except web.HTTPException:
            raise
        except Exception as e:
            log.exception("request_unhandled err=%s", e)
            return web.json_response({"ok": False, "error": "internal"}, status=500)

    async def handle_health(_request: web.Request) -> web.Response:
        return web.json_response({"ok": True, "service": "bot"})

    # aiohttp для /notify + GET /health (прокси/мониторинг; снижает шум от мусорных коннектов)
    app = web.Application(middlewares=[_catch_mw])
    app["bot"] = bot
    app["chat_id"] = chat_id
    app.router.add_get("/health", handle_health)
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
