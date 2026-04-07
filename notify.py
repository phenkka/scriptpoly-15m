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
        return None
    try:
        payload: dict = {"text": text}
        if reply_to_message_id is not None:
            payload["reply_to_message_id"] = reply_to_message_id
        resp = _get_session().post(url, json=payload, timeout=timeout)
        data = resp.json()
        return data.get("message_id")
    except Exception as e:
        log.debug(f"notify_failed url={url} err={e}")
        return None
