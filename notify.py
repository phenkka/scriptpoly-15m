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


def notify(text: str, *, timeout: float = 3.0) -> None:
    """Отправляет текстовое сообщение боту. Молча проглатывает ошибки."""
    url = os.environ.get("BOT_NOTIFY_URL", "").strip()
    if not url:
        return
    try:
        _get_session().post(url, json={"text": text}, timeout=timeout)
    except Exception as e:
        log.debug(f"notify_failed url={url} err={e}")
