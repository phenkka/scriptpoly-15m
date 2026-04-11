"""
predict.fun BTC/USD Up or Down - 1h Order Book Monitor
==========================================================
Каждую секунду получает лучший BID (покупка) и ASK (продажа)
для токенов UP (Yes) и DOWN (No) текущего часового рынка.

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

import calendar
import io
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests


def post_tick(up: dict, down: dict) -> None:
    url = os.environ.get("ANALYZER_URL", "").strip()
    if not url:
        return

    payload = {
        "source": "predict",
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "up": up,
        "down": down,
    }

    try:
        requests.post(url, json=payload, timeout=0.5)
    except requests.RequestException:
        return

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
SLOT_MIN = 60  # минут в окне

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
    """Возвращает начало текущего часового слота в ET."""
    now_et = datetime.now(tz=ET_ZONE)
    return now_et.replace(minute=0, second=0, microsecond=0)


_MONTH_NAMES = {name.lower(): i for i, name in enumerate(calendar.month_name) if name}


def slot_to_slug(slot_et: datetime) -> str:
    """
    Формирует slug рынка из временного слота ET.
    Пример: bitcoin-up-or-down-april-3-2026-2pm-et
    """
    month = slot_et.strftime("%B").lower()   # "april"
    day   = slot_et.day                       # 3 (без нуля)
    year  = slot_et.year
    hour12 = slot_et.hour % 12 or 12
    ampm   = "am" if slot_et.hour < 12 else "pm"
    return f"bitcoin-up-or-down-{month}-{day}-{year}-{hour12}{ampm}-et"


def slot_to_title(slot_et: datetime) -> str:
    """Строит ожидаемый заголовок рынка."""
    month  = slot_et.strftime("%B")      # "April"
    hour12 = slot_et.hour % 12 or 12
    ampm   = "AM" if slot_et.hour < 12 else "PM"
    return f"Bitcoin Up or Down - {month} {slot_et.day}, {hour12}{ampm} ET"


def slot_end_utc(slot_et: datetime) -> datetime:
    """Возвращает время конца слота в UTC."""
    end_et = slot_et + timedelta(minutes=SLOT_MIN)
    return end_et.astimezone(timezone.utc)


# ──────────────────────────────────────────────────────────────
#  Поиск рынка
# ──────────────────────────────────────────────────────────────

def find_market(session: requests.Session, _slot_et: datetime) -> tuple[int, str, int, datetime] | tuple[None, None, None, None]:
    """
    Ищет активный или предстоящий Bitcoin 1-час Up/Down рынок.
    Возвращает (market_id, title, decimal_precision, ends_utc).
    """
    now_utc = datetime.now(tz=timezone.utc)

    # 1. Пробуем текущий слот напрямую по slug
    current_slot = current_et_slot()
    slug = slot_to_slug(current_slot)
    try:
        resp = session.get(f"{API_BASE}/categories/{slug}", timeout=10)
        if resp.status_code == 200:
            cat = resp.json().get("data", {})
            markets = cat.get("markets", [])
            if markets:
                end_et = current_slot + timedelta(minutes=SLOT_MIN)
                ends = end_et.astimezone(timezone.utc)
                ends_str = cat.get("endsAt")
                if ends_str:
                    ends = datetime.fromisoformat(ends_str.replace("Z", "+00:00"))
                if now_utc <= ends:
                    m = markets[0]
                    mid = m.get("id")
                    dp  = m.get("decimalPrecision", 2)
                    title = cat.get("title", slot_to_title(current_slot))
                    print(f"  {GREEN}Активный рынок: {title}{RESET}")
                    return mid, title, dp, ends
    except requests.RequestException:
        pass

    # 2. Fallback: поиск через /search
    try:
        resp = session.get(
            f"{API_BASE}/search",
            params={"query": "Bitcoin Up or Down 1 hour", "limit": "20"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
    except requests.RequestException as e:
        print(f"  {RED}[ERROR] Поиск недоступен: {e}{RESET}")
        return None, None, None, None

    # Собираем все BTC/USD CRYPTO_UP_DOWN рынки из категорий с временными метками
    active:   list[tuple[datetime, int, str, int, datetime]] = []
    upcoming: list[tuple[datetime, int, str, int, datetime]] = []

    for c in data.get("categories", []):
        if c.get("marketVariant") != "CRYPTO_UP_DOWN":
            continue
        title = c.get("title", "")
        title_up = title.upper()
        if "BTC" not in title_up and "BITCOIN" not in title_up:
            continue
        starts_str = c.get("startsAt")
        ends_str   = c.get("endsAt")
        if not starts_str or not ends_str:
            continue
        starts = datetime.fromisoformat(starts_str.replace("Z", "+00:00"))
        ends   = datetime.fromisoformat(ends_str.replace("Z", "+00:00"))
        for m in c.get("markets", []):
            mid = m.get("id")
            dp  = m.get("decimalPrecision", 2)
            if starts <= now_utc <= ends:
                active.append((starts, mid, title, dp, ends))
            elif starts > now_utc:
                upcoming.append((starts, mid, title, dp, ends))

    # Из top-level markets восстанавливаем время через categorySlug
    for m in data.get("markets", []):
        m_title = m.get("title", "")
        m_title_up = m_title.upper()
        if m.get("marketVariant") != "CRYPTO_UP_DOWN":
            continue
        if "BTC" not in m_title_up and "BITCOIN" not in m_title_up:
            continue
        cs = m.get("categorySlug", "")
        start_et: datetime | None = None
        # New format: bitcoin-up-or-down-april-3-2026-2pm-et
        if cs.startswith("bitcoin-up-or-down-"):
            try:
                rest = cs[len("bitcoin-up-or-down-"):].split("-")
                # rest = ["april", "3", "2026", "2pm", "et"]
                mo_num = _MONTH_NAMES.get(rest[0])
                dy_num = int(rest[1])
                yr_num = int(rest[2])
                t_str  = rest[3]  # "2pm" / "10am"
                is_pm  = t_str.endswith("pm")
                hh12   = int(t_str[:-2])
                hh24   = (hh12 % 12) + (12 if is_pm else 0)
                if mo_num:
                    start_et = datetime(yr_num, mo_num, dy_num, hh24, 0, tzinfo=ET_ZONE)
            except (IndexError, ValueError, TypeError):
                pass
        else:
            # Old format: btc-usd-up-down-YYYY-MM-DD-HH-MM-*
            try:
                parts = cs.split("-")
                yr2, mo2, dy2, hh2, mm2 = int(parts[4]), int(parts[5]), int(parts[6]), int(parts[7]), int(parts[8])
                start_et = datetime(yr2, mo2, dy2, hh2, mm2, tzinfo=ET_ZONE)
            except (IndexError, ValueError):
                pass
        if start_et is None:
            continue
        end_et = start_et + timedelta(minutes=SLOT_MIN)
        starts = start_et.astimezone(timezone.utc)
        ends   = end_et.astimezone(timezone.utc)
        mid    = m.get("id")
        dp     = m.get("decimalPrecision", 2)
        if starts <= now_utc <= ends:
            if not any(x[1] == mid for x in active):
                active.append((starts, mid, m_title, dp, ends))
        elif starts > now_utc:
            if not any(x[1] == mid for x in upcoming):
                upcoming.append((starts, mid, m_title, dp, ends))

    if active:
        active.sort()
        _, mid, title, dp, ends = active[0]
        print(f"  {GREEN}Активный рынок: {title}{RESET}")
        return mid, title, dp, ends

    if upcoming:
        upcoming.sort()
        _, mid, title, dp, ends = upcoming[0]
        start_dt = upcoming[0][0]
        wait_s = max(0.0, (start_dt - now_utc).total_seconds())
        local_start = start_dt.astimezone(ET_ZONE).strftime("%H:%M %Z")
        print(f"  {YELLOW}Ближайший рынок: {title} (старт {local_start}, через {wait_s:.0f}с){RESET}")
        return mid, title, dp, ends

    print(f"  {YELLOW}[WARN] Рынок BTC/USD 1-час не найден{RESET}")
    return None, None, None, None


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


def _levels_up_down(book: dict, decimal_precision: int, *, n: int) -> tuple[dict, dict]:
    """Return (up_payload, down_payload) including bids/asks level arrays."""
    bids = book.get("bids", []) or []
    asks = book.get("asks", []) or []

    up_bids: list[list[float]] = []
    up_asks: list[list[float]] = []
    dn_bids: list[list[float]] = []
    dn_asks: list[list[float]] = []

    for lvl in bids[: max(0, n)]:
        try:
            p = float(lvl[0])
            sz = float(lvl[1])
        except Exception:
            continue
        up_bids.append([p, sz])
        # DOWN ask comes from UP bid
        dn_asks.append([complement(p, decimal_precision), sz])

    for lvl in asks[: max(0, n)]:
        try:
            p = float(lvl[0])
            sz = float(lvl[1])
        except Exception:
            continue
        up_asks.append([p, sz])
        # DOWN bid comes from UP ask
        dn_bids.append([complement(p, decimal_precision), sz])

    up_payload = {"bids": up_bids, "asks": up_asks}
    down_payload = {"bids": dn_bids, "asks": dn_asks}
    return up_payload, down_payload


# ──────────────────────────────────────────────────────────────
#  Вывод
# ──────────────────────────────────────────────────────────────

SEP1 = "=" * 82
SEP2 = "-" * 82


def print_header(title: str, market_id: int, end_utc: datetime, decimal_precision: int) -> None:
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
    market_id:    int | None      = None
    market_title: str             = ""
    market_ends:  datetime | None = None
    market_slot:  int | None      = None
    dp: int                       = 2

    while True:
        now_utc = datetime.now(tz=timezone.utc)

        # Ищем новый рынок если нет активного или текущий истёк
        if market_id is None or (market_ends is not None and now_utc >= market_ends):
            now_et_str = now_utc.astimezone(ET_ZONE).strftime("%Y-%m-%d %H:%M %Z")
            print(f"\n  Поиск рынка ... ({now_et_str})")
            mid, title, d, ends = find_market(session, now_utc.astimezone(ET_ZONE))
            if mid is None:
                print("  Retrying in 10s...")
                time.sleep(10)
                continue
            market_id    = mid
            market_title = title
            market_ends  = ends
            if market_ends is not None:
                slot_start_utc = (market_ends - timedelta(minutes=SLOT_MIN)).astimezone(timezone.utc)
                market_slot = (int(slot_start_utc.timestamp()) // (SLOT_MIN * 60)) * (SLOT_MIN * 60)
            dp           = d or 2
            print_header(market_title, market_id, market_ends, dp)
            # Пауза 2 сек после перехода на новый рынок
            time.sleep(2.0)

        # Пауза 2 сек до конца рынка (десинхрон с polymarket)
        if market_ends is not None and now_utc < market_ends:
            secs_to_end = (market_ends - now_utc).total_seconds()
            if secs_to_end <= 2.0:
                time.sleep(0.5)
                continue

        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]

        try:
            n_levels = int(os.environ.get("ORDERBOOK_LEVELS", "25") or "25")
        except ValueError:
            n_levels = 25

        try:
            book  = get_orderbook(session, market_id)
            data  = extract_best(book, dp)
            print_row(ts, "UP",   data["up_bid"], data["up_bid_sz"], data["up_ask"], data["up_ask_sz"], GREEN, RED)
            print_row(ts, "DOWN", data["dn_bid"], data["dn_bid_sz"], data["dn_ask"], data["dn_ask_sz"], GREEN, RED)
            print()

            lv_up, lv_dn = _levels_up_down(book, dp, n=n_levels)
            up_payload = {
                "bid": data.get("up_bid"),
                "bid_sz": data.get("up_bid_sz", 0.0),
                "ask": data.get("up_ask"),
                "ask_sz": data.get("up_ask_sz", 0.0),
                **lv_up,
            }
            down_payload = {
                "bid": data.get("dn_bid"),
                "bid_sz": data.get("dn_bid_sz", 0.0),
                "ask": data.get("dn_ask"),
                "ask_sz": data.get("dn_ask_sz", 0.0),
                **lv_dn,
            }

            url = os.environ.get("ANALYZER_URL", "").strip()
            if url and market_id is not None:
                payload = {
                    "source": "predict",
                    "ts": datetime.now(tz=timezone.utc).isoformat(),
                    "up": up_payload,
                    "down": down_payload,
                    "meta": {
                        "slot": market_slot,
                        "market_id": market_id,
                        "title": market_title,
                        "end_date": market_ends.isoformat() if market_ends is not None else None,
                    },
                }
                try:
                    requests.post(url, json=payload, timeout=0.5)
                except requests.RequestException:
                    pass
        except requests.HTTPError as e:
            print(f"  {ts}  HTTP {e.response.status_code}: {e}")
        except requests.RequestException as e:
            print(f"  {ts}  Network error: {e}")

        time.sleep(0.5)


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
