# Session State — последнее обновление: 2026-04-29

## Текущая ветка
`backup/2026-04-28` — ветка под **15-минутные рынки**.

## Что уже сделано

### Восстановлены 2 потерянных коммита (cherry-pick из main)
- `a7371f9` — балансировщик: отслеживание нужной tx по `amount_base_unit`, cooldown 1h при timeout бриджа
- `1951ef0` — CLOB v2 миграция: `py-clob-client-v2`, правильные методы API

### Переведено на 15-минутный режим
| Файл | Что изменено |
|---|---|
| `collect/btc_orderbook.py` | `SLOT_SEC = 900` (было 3600); slug включает минуты (`2-15pm-et`) |
| `collect/predict_orderbook.py` | `SLOT_MIN = 15`; `current_et_slot()` округляет до 15 мин; два slug-формата (новый + старый `btc-usd-up-down-YYYY-MM-DD-HH-MM`); парсер slug поддерживает минуты; фильтр по длительности рынка (±50% от 15 мин) |
| `trader/main.py` | `PREDICT_UNWIND_BUDGET_SEC` вынесен в env var (default 360s, для 15-мин ставим 120s) |
| `docker-compose.yml` | `PREDICT_MIN_EXPIRY_BUFFER_SEC=120`, `PREDICT_UNWIND_BUDGET_SEC=120` |

## Текущий статус
Бот запущен через Docker Compose. Проверяется, находит ли `collect-predict` и `collect-polymarket` 15-минутные рынки.

## Неизвестные вещи (надо проверить в логах)

1. **Точный slug-формат** predict.fun для 15-мин рынков. Код пробует оба:
   - `bitcoin-up-or-down-april-29-2026-2-15pm-et` (новый)
   - `btc-usd-up-down-2026-04-29-14-15` (старый)
   - Fallback: `/search` API

2. **Slug-формат Polymarket** для 15-мин рынков. Генерируется `bitcoin-up-or-down-april-29-2026-2-15pm-et` — нужно убедиться что Gamma API его отдаёт.

## Как проверить
```bash
docker compose logs -f collect-predict | grep -E "Активный|рынок|WARN|slug|market_id"
docker compose logs -f collect-polymarket | grep -E "Новое окно|slug|ERROR"
```

## Следующие шаги
- [ ] По логам понять, какой slug-формат реально используется на обоих площадках
- [ ] При необходимости — скорректировать `slot_to_slug` / `_slot_ts_to_poly_slug`
- [ ] Убедиться что analyzer видит тики от обоих коллекторов и находит edge
- [ ] Первый тестовый трейд в dry_run режиме (`TRADER_DRY_RUN=true`)

## Ключевые файлы
- `collect/btc_orderbook.py` — Polymarket стакан, определение слота
- `collect/predict_orderbook.py` — Predict.fun стакан, поиск рынка
- `analyzer/main.py` — поиск арбитражных окон
- `trader/main.py` — исполнение (~6650 строк), главная логика
- `trader/config.py` — runtime настройки через `/data/settings.json`
- `docker-compose.yml` — env vars для всех сервисов
