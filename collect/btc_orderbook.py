"""
Polymarket BTC Up/Down 15-min — монитор стакана ордеров.

Каждую секунду получает лучшие BID и ASK для токенов UP и DOWN
из текущего 15-минутного окна. Автоматически переключается на
следующее окно, когда предыдущее закрывается.

Цены — вероятности от 0.00 до 1.00:
  BID (лучшая заявка на покупку)  = максимальная цена в bids
  ASK (лучшая заявка на продажу)  = минимальная цена в asks
  СПРЕД = ASK − BID
"""

import io
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# Принудительно UTF-8 на Windows (cp1251 не знает символов рамок)
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
SLOT_SEC  = 900  # 15 минут в секундах

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


def post_tick(
    ts_str: str,
    question: str,
    up: dict,
    down: dict,
    *,
    slot: int | None = None,
    token_ids: dict[str, str | None] | None = None,
) -> None:
    url = os.environ.get("ANALYZER_URL", "").strip()
    if not url:
        return

    # ts_str is local time string; analyzer expects ISO-like datetime.
    # Use UTC timestamp to avoid timezone ambiguity.
    payload = {
        "source": "polymarket",
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "up": up,
        "down": down,
        "meta": {
            "question": question,
            "slot": slot,
            "token_ids": token_ids,
        },
    }

    # Best-effort: do not crash collector on analyzer/network issues.
    try:
        SESSION.post(url, json=payload, timeout=0.5)
    except requests.RequestException:
        return


# ──────────────────────────────────────────────
#  Получение токенов текущего / следующего окна
# ──────────────────────────────────────────────

def fetch_market_tokens(slot_ts: int) -> tuple[str, dict[str, str]] | tuple[None, None]:
    """
    По временному слоту ищет рынок через Gamma API.
    Возвращает (question, {outcome: token_id, ...}) или (None, None).
    """
    slug = f"btc-updown-15m-{slot_ts}"
    try:
        resp = SESSION.get(
            f"{GAMMA_API}/events",
            params={"slug": slug},
            timeout=10,
        )
        resp.raise_for_status()
        events = resp.json()
    except requests.RequestException as e:
        print(f"  [WARN] Gamma API недоступен: {e}")
        return None, None

    if not events:
        return None, None

    markets = events[0].get("markets", [])
    if not markets:
        return None, None

    market = markets[0]
    question = market.get("question", slug)

    # clobTokenIds — JSON-строка с массивом ID, outcomes — JSON-строка с массивом исходов
    raw_ids      = market.get("clobTokenIds", "[]")
    raw_outcomes = market.get("outcomes", '["Up","Down"]')

    token_ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
    outcomes  = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes

    if not token_ids:
        return None, None

    token_map = {outcomes[i]: token_ids[i] for i in range(min(len(outcomes), len(token_ids)))}
    return question, token_map


def current_slot() -> int:
    """Возвращает Unix-метку начала текущего 15-минутного окна."""
    return (int(time.time()) // SLOT_SEC) * SLOT_SEC


# ──────────────────────────────────────────────
#  Стакан ордеров
# ──────────────────────────────────────────────

def get_book(token_id: str) -> dict:
    resp = SESSION.get(
        f"{CLOB_API}/book",
        params={"token_id": token_id},
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()


def best_bid_ask(book: dict) -> tuple[float | None, float, float | None, float]:
    """Возвращает (bid_price, bid_size, ask_price, ask_size)."""
    bids = book.get("bids", [])
    asks = book.get("asks", [])

    if bids:
        best = max(bids, key=lambda x: float(x["price"]))
        bid_price, bid_size = float(best["price"]), float(best["size"])
    else:
        bid_price, bid_size = None, 0.0

    if asks:
        best = min(asks, key=lambda x: float(x["price"]))
        ask_price, ask_size = float(best["price"]), float(best["size"])
    else:
        ask_price, ask_size = None, 0.0

    return bid_price, bid_size, ask_price, ask_size


# ──────────────────────────────────────────────
#  Форматирование строки вывода
# ──────────────────────────────────────────────

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
DIM    = "\033[2m"


def fmt_price(price: float | None, color: str) -> str:
    if price is None:
        return f"{DIM}  —     {RESET}"
    return f"{color}{price:.4f}{RESET}"


def print_header(question: str, token_map: dict[str, str], slot_ts: int) -> None:
    end_ts = slot_ts + SLOT_SEC
    end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%H:%M UTC")
    sep1 = "=" * 80
    sep2 = "-" * 80
    print(f"\n{sep1}")
    print(f"  {CYAN}{question}{RESET}")
    print(f"  EXIT: {end_dt}  |  Ctrl+C - EXIT")
    for outcome, tid in token_map.items():
        print(f"  [{outcome}] token: ...{tid[-16:]}")
    print(sep2)
    print(
        f"  {'TIME':<13}  "
        f"{'OUTCOME':<5}  "
        f"{'BID (BUY)':^22}  "
        f"{'ASK (SELL)':^22}  "
        f"{'SPREAD':>7}"
    )
    print(sep2)


def print_row(ts_str: str, outcome: str, bid: float | None, bid_sz: float,
              ask: float | None, ask_sz: float) -> None:
    bid_s = f"{fmt_price(bid, GREEN)} {DIM}({bid_sz:>8.1f}){RESET}" if bid else f"{DIM}    no data    {RESET}"
    ask_s = f"{fmt_price(ask, RED)} {DIM}({ask_sz:>8.1f}){RESET}"   if ask else f"{DIM}    no data    {RESET}"

    if bid is not None and ask is not None:
        spread = ask - bid
        sp_s = f"{YELLOW}{spread:.4f}{RESET}"
    else:
        sp_s = f"{DIM}  —  {RESET}"

    print(f"  {ts_str}  {outcome:<5}  {bid_s}  {ask_s}  {sp_s}")


# ──────────────────────────────────────────────
#  Основной цикл
# ──────────────────────────────────────────────

def run_loop() -> None:
    active_slot: int | None = None
    token_map: dict[str, str] = {}
    question: str = ""

    while True:
        slot = current_slot()

        # Загружаем токены при смене слота
        if slot != active_slot:
            print(f"\n  Новое окно — загружаю токены для слота {slot}…")
            q, tm = fetch_market_tokens(slot)
            if q is None:
                # Возможно, рынок ещё не создан — пробуем предыдущий
                q, tm = fetch_market_tokens(slot - SLOT_SEC)
            if q is None:
                print("  [ERROR] Рынок не найден. Повтор через 5 сек…")
                time.sleep(5)
                continue
            active_slot = slot
            token_map = tm
            question = q
            print_header(question, token_map, active_slot)

        ts_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]

        up_payload: dict = {"bid": None, "bid_sz": 0.0, "ask": None, "ask_sz": 0.0}
        down_payload: dict = {"bid": None, "bid_sz": 0.0, "ask": None, "ask_sz": 0.0}
        up_token_id: str | None = None
        down_token_id: str | None = None

        for outcome, token_id in token_map.items():
            try:
                book = get_book(token_id)
                bid, bid_sz, ask, ask_sz = best_bid_ask(book)
                print_row(ts_str, outcome, bid, bid_sz, ask, ask_sz)

                outcome_norm = outcome.strip().lower()
                if outcome_norm in {"up", "yes"}:
                    up_payload = {"bid": bid, "bid_sz": bid_sz, "ask": ask, "ask_sz": ask_sz}
                    up_token_id = token_id
                elif outcome_norm in {"down", "no"}:
                    down_payload = {"bid": bid, "bid_sz": bid_sz, "ask": ask, "ask_sz": ask_sz}
                    down_token_id = token_id
            except requests.HTTPError as e:
                print(f"  {ts_str}  [{outcome}]  HTTP {e.response.status_code}")
            except requests.RequestException as e:
                print(f"  {ts_str}  [{outcome}]  Ошибка: {e}")

        # Push aggregated tick once per second (best-effort)
        # Пропускаем тик если token_ids ещё не готовы (переход рынка)
        if up_token_id is not None and down_token_id is not None:
            post_tick(
                ts_str,
                question,
                up_payload,
                down_payload,
                slot=active_slot,
                token_ids={"up": up_token_id, "down": down_token_id},
            )

        # Пустая строка-разделитель между тиками, если токенов > 1
        if len(token_map) > 1:
            print()

        time.sleep(0.5)


# ──────────────────────────────────────────────
#  Точка входа
# ──────────────────────────────────────────────

def main() -> None:
    try:
        run_loop()
    except KeyboardInterrupt:
        print("\n\n  Остановлено.\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
