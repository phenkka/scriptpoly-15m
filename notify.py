"""Утилита для отправки уведомлений в Telegram через bot /notify endpoint."""
from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger(__name__)

_session: requests.Session | None = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
    return _session


def notify(text: str, *, reply_to_message_id: int | None = None, timeout: float = 3.0) -> int | None:
    """Отправляет текстовое сообщение боту. Возвращает message_id или None при ошибке."""
    url = os.environ.get("BOT_NOTIFY_URL", "").strip()
    if not url:
        print("[notify] skipped: BOT_NOTIFY_URL empty (no message sent)", flush=True)
        log.warning("notify_skipped: BOT_NOTIFY_URL is empty — no Telegram; check .env in trader container")
        return None
    try:
        payload: dict = {"text": text}
        if reply_to_message_id is not None:
            payload["reply_to_message_id"] = reply_to_message_id
        resp = _get_session().post(url, json=payload, timeout=timeout)
        if resp.status_code >= 400:
            print(f"[notify] http {resp.status_code} {(resp.text or '')[:300]}", flush=True)
            log.warning("notify_http_%s: %s", resp.status_code, (resp.text or "")[:500])
        data = resp.json()
        return data.get("message_id")
    except Exception as e:
        print(f"[notify] failed: {e}", flush=True)
        log.warning("notify_failed url=%s err=%s", url, e)
        return None
