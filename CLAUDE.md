# scriptpoly — Claude Context

## Что это
Арбитражный бот между **Polymarket** (CLOB, Polygon) и **Predict.fun** (BNB).
Покупает оба исхода BTC Up/Down когда сумма цен < 1.0, фиксирует безрисковую прибыль.

## Стек
- Python 3.11, FastAPI, Docker Compose
- **Polymarket**: `py-clob-client-v2` (CLOB v2 API, апрель 2026), EIP-712 v2
- **Predict.fun**: `predict-sdk`, JWT-аутентификация, BSC/BNB
- **Балансировщик**: `web3`, Bridge Polymarket → Predict.fun (USDT через BSC)

## Сервисы
| Контейнер | Файл | Роль |
|---|---|---|
| `collect-polymarket` | `collect/btc_orderbook.py` | Стакан Polymarket → analyzer |
| `collect-predict` | `collect/predict_orderbook.py` | Стакан Predict.fun → analyzer |
| `analyzer` | `analyzer/main.py` | Поиск арбитражных окон, расчёт edge |
| `trader` | `trader/main.py` | Исполнение ордеров (FastAPI :9000) |
| `balancer` | `balancer/main.py` | Ребалансировка кошельков |
| `claimer` | `claimer/` | Клейм выплат после резолюции |
| `bot` | `bot/` | Telegram-уведомления |

## Команды

```bash
# Запуск всего
docker compose up -d --build

# Логи конкретного сервиса
docker compose logs -f collect-predict
docker compose logs -f collect-polymarket
docker compose logs -f analyzer
docker compose logs -f trader

# Проверка рынка (15-мин) — смотреть первыми
docker compose logs -f collect-predict | grep -E "Активный|рынок|WARN|ERROR"
docker compose logs -f collect-polymarket | grep -E "Новое окно|ERROR"

# Ребилд одного сервиса
docker compose up -d --build trader
```

## Важные правила кода

1. **Библиотека Polymarket** — `py-clob-client-v2` (не `py-clob-client`). CLOB v2 убрал поля `nonce`, `feeRateBps`, `taker` из ордеров.
2. **Методы ClobClient** — `create_or_derive_api_key()`, `cancel_orders([id])` (в v2 нет `cancel()` и `create_or_derive_api_creds()`).
3. **Размер слота** — `SLOT_SEC`/`SLOT_MIN` определяет длину окна. Ветка `backup/2026-04-28` = 15 мин, `main` = 1 час.
4. **Буфер экспайри** — для 15-мин рынков: `PREDICT_MIN_EXPIRY_BUFFER_SEC=120`, `PREDICT_UNWIND_BUDGET_SEC=120` (в docker-compose).
5. **Ghost-fill защита** — не убирать `_late_fill_watcher` и `_check_inflight_on_startup`. Predict.fun может подтвердить BSC-транзакцию через несколько минут после отмены.
6. **Балансировщик** — всегда отслеживать депозит по `amount_base_unit` (на адрес может прийти несколько транзакций).

## Как обновлять SESSION_STATE.md

После значимых изменений запускай:
```
Обнови docs/SESSION_STATE.md: [краткое описание что изменилось]
```
Или я обновляю его сам в конце сессии с новыми задачами/решениями.
