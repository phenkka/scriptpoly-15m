"""
predict.fun BTC/USD Up or Down - 15min Order Book Monitor
==========================================================
Каждую секунду получает лучший BID (покупка) и ASK (продажа)
для токенов UP (Yes) и DOWN (No) текущего 15-минутного рынка.

Требования:
  pip install requests python-dotenv

API ключ:
  Получить на Discord: https://discord.gg/predictdotfun
  Положить в .env: PREDICT_API_KEY=ваш_ключ
  ИЛИ передать через переменную окружения.

Структура ответа стакана:
  bids  = [[price, qty], ...]  — лучший bid первый (для UP/Yes)
  asks  = [[price, qty], ...]  — лучший ask первый (для UP/Yes)
  DOWN = complement: bid_down = 1 - ask_up[0], ask_down = 1 - bid_up[0]
"""

import io
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

# ── UTF-8 вывод на Windows ──────────────────────────────────────
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ── Загрузка .env если есть ────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

API_BASE = "https://api.predict.fun/v1"
ET_ZONE  = ZoneInfo("America/New_York")
SLOT_MIN = 15  # минут в окне

# ── Цвета ANSI ─────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"
RESET  = "\033[0m"


# ──────────────────────────────────────────────────────────────
#  Утилиты
# ──────────────────────────────────────────────────────────────

def get_api_key() -> str:
    key = os.environ.get("PREDICT_API_KEY", "").strip()
    if not key:
        print(f"{RED}[ERROR]{RESET} Переменная PREDICT_API_KEY не задана.")
        print("  Получить ключ: https://discord.gg/predictdotfun")
        print("  Создать .env:  PREDICT_API_KEY=ваш_ключ")
        sys.exit(1)
    return key


def make_session(api_key: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json",
        "x-api-key": api_key,
    })
    return s


def complement(price: float, decimal_precision: int = 2) -> float:
    """Вычисляет дополнение цены (Yes + No = 1.0)."""
    factor = 10 ** decimal_precision
    return (factor - round(price * factor)) / factor


# ──────────────────────────────────────────────────────────────
#  Время / слот
# ──────────────────────────────────────────────────────────────

def current_et_slot() -> datetime:
    """Возвращает начало текущего 15-минутного слота в ET."""
    now_et = datetime.now(tz=ET_ZONE)
    slot_min = (now_et.minute // SLOT_MIN) * SLOT_MIN
    return now_et.replace(minute=slot_min, second=0, microsecond=0)


def slot_to_slug(slot_et: datetime) -> str:
    """
    Формирует slug рынка из временного слота ET.
    Пример: btc-usd-up-down-2026-03-31-11-30-15-minutes
    """
    return (
        f"btc-usd-up-down-"
        f"{slot_et.year:04d}-{slot_et.month:02d}-{slot_et.day:02d}-"
        f"{slot_et.hour:02d}-{slot_et.minute:02d}-15-minutes"
    )


def slot_to_title(slot_et: datetime) -> str:
    """Строит поисковый запрос по слоту."""
    end_et = slot_et + timedelta(minutes=SLOT_MIN)
    hour_12  = slot_et.hour % 12 or 12
    ampm     = "AM" if slot_et.hour < 12 else "PM"
    end_h12  = end_et.hour % 12 or 12
    end_ampm = "AM" if end_et.hour < 12 else "PM"
    month = slot_et.strftime("%B")  # "March"
    return (
        f"BTC/USD Up or Down - {month} {slot_et.day}, "
        f"{hour_12}:{slot_et.minute:02d}-{end_h12}:{end_et.minute:02d}{end_ampm} ET"
    )


def slot_end_utc(slot_et: datetime) -> datetime:
    """Возвращает время конца слота в UTC."""
    end_et = slot_et + timedelta(minutes=SLOT_MIN)
    return end_et.astimezone(timezone.utc)


# ──────────────────────────────────────────────────────────────
#  Поиск рынка
# ──────────────────────────────────────────────────────────────

def find_market(session: requests.Session, slot_et: datetime) -> tuple[int, str, int] | tuple[None, None, None]:
    """
    Ищет рынок по слоту. Возвращает (market_id, title, decimal_precision).
    """
    query = slot_to_title(slot_et)
    slug  = slot_to_slug(slot_et)
    print(f"  Поиск: {CYAN}{query}{RESET}")
    print(f"  Slug : {slug}")

    try:
        resp = session.get(
            f"{API_BASE}/search",
            params={"query": query, "limit": "5"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
    except requests.RequestException as e:
        print(f"  {RED}[ERROR] Поиск недоступен: {e}{RESET}")
        return None, None, None

    # Ищем по slug или точному совпадению заголовка
    all_markets = data.get("markets", [])
    for m in data.get("categories", []):
        all_markets.extend(m.get("markets", []))

    for m in all_markets:
        cat_slug = m.get("categorySlug", "")
        title    = m.get("title", "")
        mid      = m.get("id")
        dp       = m.get("decimalPrecision", 2)
        if slug in cat_slug or slug in title.lower().replace(" ", "-") or query.lower() in title.lower():
            return mid, title, dp

    # Fallback: берём первый результат CRYPTO_UP_DOWN
    for m in all_markets:
        if m.get("marketVariant") == "CRYPTO_UP_DOWN" and "BTC" in m.get("title", "").upper():
            return m.get("id"), m.get("title", ""), m.get("decimalPrecision", 2)

    print(f"  {YELLOW}[WARN] Рынок не найден по запросу: {query}{RESET}")
    return None, None, None


# ──────────────────────────────────────────────────────────────
#  Стакан ордеров
# ──────────────────────────────────────────────────────────────

def get_orderbook(session: requests.Session, market_id: int) -> dict:
    resp = session.get(
        f"{API_BASE}/markets/{market_id}/orderbook",
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json().get("data", {})


def extract_best(book: dict, decimal_precision: int = 2) -> dict:
    """
    Возвращает лучшие BID/ASK для UP и DOWN.

    Стакан хранит данные для YES (UP):
      UP  BID (лучшая заявка на покупку UP) = bids[0][0]
      UP  ASK (лучшая заявка на продажу UP) = asks[0][0]
      DOWN BID = complement(asks[0][0])   (продать UP = купить DOWN)
      DOWN ASK = complement(bids[0][0])   (купить UP = продать DOWN)
    """
    bids = book.get("bids", [])
    asks = book.get("asks", [])

    result = {
        "up_bid":  None, "up_bid_sz":  0.0,
        "up_ask":  None, "up_ask_sz":  0.0,
        "dn_bid":  None, "dn_bid_sz":  0.0,
        "dn_ask":  None, "dn_ask_sz":  0.0,
    }

    if bids:
        result["up_bid"]    = float(bids[0][0])
        result["up_bid_sz"] = float(bids[0][1])
        result["dn_ask"]    = complement(float(bids[0][0]), decimal_precision)
        result["dn_ask_sz"] = float(bids[0][1])

    if asks:
        result["up_ask"]    = float(asks[0][0])
        result["up_ask_sz"] = float(asks[0][1])
        result["dn_bid"]    = complement(float(asks[0][0]), decimal_precision)
        result["dn_bid_sz"] = float(asks[0][1])

    return result


# ──────────────────────────────────────────────────────────────
#  Вывод
# ──────────────────────────────────────────────────────────────

SEP1 = "=" * 82
SEP2 = "-" * 82


def print_header(title: str, market_id: int, slot_et: datetime, decimal_precision: int) -> None:
    end_utc = slot_end_utc(slot_et)
    end_str = end_utc.strftime("%H:%M UTC")
    print(f"\n{SEP1}")
    print(f"  {CYAN}{title}{RESET}")
    print(f"  market_id: {market_id}  |  precision: {decimal_precision}  |  closes: {end_str}  |  Ctrl+C - exit")
    print(SEP2)
    print(
        f"  {'TIME':<13}  "
        f"{'SIDE':<4}  "
        f"{'BID (buy)':^24}  "
        f"{'ASK (sell)':^24}  "
        f"{'SPREAD':>7}"
    )
    print(SEP2)


def fmt_p(price, color: str) -> str:
    if price is None:
        return f"{DIM}  ---  {RESET}"
    return f"{color}{price:.2f}{RESET}"


def print_row(ts: str, side: str, bid, bid_sz, ask, ask_sz, color_bid: str, color_ask: str) -> None:
    b = f"{fmt_p(bid, color_bid)}  {DIM}sz:{bid_sz:>8.1f}{RESET}" if bid is not None else f"{DIM}{'no data':^24}{RESET}"
    a = f"{fmt_p(ask, color_ask)}  {DIM}sz:{ask_sz:>8.1f}{RESET}" if ask is not None else f"{DIM}{'no data':^24}{RESET}"

    if bid is not None and ask is not None:
        spread_s = f"{YELLOW}{ask - bid:.2f}{RESET}"
    else:
        spread_s = f"{DIM} --- {RESET}"

    print(f"  {ts}  {side:<4}  {b}  {a}  {spread_s}")


# ──────────────────────────────────────────────────────────────
#  Основной цикл
# ──────────────────────────────────────────────────────────────

def run_loop(session: requests.Session) -> None:
    active_slot: datetime | None = None
    market_id: int | None        = None
    market_title: str            = ""
    dp: int                       = 2

    while True:
        slot = current_et_slot()

        # Обновляем рынок при смене слота
        if slot != active_slot:
            print(f"\n  New slot: {slot.strftime('%Y-%m-%d %H:%M %Z')}")
            mid, title, d = find_market(session, slot)
            if mid is None:
                print("  Retrying in 5s...")
                time.sleep(5)
                continue
            active_slot  = slot
            market_id    = mid
            market_title = title
            dp           = d or 2
            print_header(market_title, market_id, active_slot, dp)

        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]

        try:
            book  = get_orderbook(session, market_id)
            data  = extract_best(book, dp)
            print_row(ts, "UP",   data["up_bid"], data["up_bid_sz"], data["up_ask"], data["up_ask_sz"], GREEN, RED)
            print_row(ts, "DOWN", data["dn_bid"], data["dn_bid_sz"], data["dn_ask"], data["dn_ask_sz"], GREEN, RED)
            print()
        except requests.HTTPError as e:
            print(f"  {ts}  HTTP {e.response.status_code}: {e}")
        except requests.RequestException as e:
            print(f"  {ts}  Network error: {e}")

        time.sleep(1)


# ──────────────────────────────────────────────────────────────
#  Точка входа
# ──────────────────────────────────────────────────────────────

def main() -> None:
    api_key = get_api_key()
    session = make_session(api_key)

    # Проверка связи
    try:
        r = session.get(f"{API_BASE}/markets", params={"first": "1"}, timeout=8)
        if r.status_code == 401:
            print(f"{RED}[ERROR]{RESET} Неверный API ключ (401 Unauthorized).")
            sys.exit(1)
    except requests.RequestException as e:
        print(f"{YELLOW}[WARN]{RESET} Проверка связи не удалась: {e}")

    try:
        run_loop(session)
    except KeyboardInterrupt:
        print("\n\n  Stopped.\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
