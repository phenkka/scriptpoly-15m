# Session State — последнее обновление: 2026-04-30

## Текущая ветка
`main` — ветка **phenkka/scriptpoly-15m** (основной деплой-репо на сервере)

---

## Что сделано в этой сессии

### 1. Исправлен таргетный репо
Все предыдущие коммиты уходили в `vanesssw/scriptpoly` вместо `phenkka/scriptpoly-15m`.
Все фиксы переприменены в правильный репо (`phenkka/scriptpoly-15m`, ветка `main`).

### 2. Исправлен трейдер: два бага в `_late_fill_watcher` (`trader/main.py`)
**Коммит:** `0c79f90`

**Баг 1** — `token_id` в `predict_late_watch.json` — это токен **Polymarket**, а не Predict.  
При вызове `_predict_max_shares_for_market(token_id=poly_token_id)` он никогда не совпадал  
с позициями Predict → `_cur_bal` всегда 0 → дельта всегда 0 → орфанные позиции не детектились.  
**Фикс:** передавать `token_id=None` (берём максимум по всем исходам рынка).

**Баг 2** — `pre_balance=0.0` из-за задержки Predict REST API.  
После BSC-подтверждения (bsc_fill_on_new_head) Predict API не успевает за ~30 с.  
Следующий ордер в том же рынке снимает `pre_balance=0` → через 30 мин `cur_bal=10.2` → ложный `TEST_UNHEDGED_DELTA`.  
**Фикс:** добавлен `_predict_mkt_confirmed_shares: dict[int, float]` — накапливает подтверждённые BSC-филлы.  
В `_late_fill_watcher` используется `effective_pre = max(saved_pre_bal, known_floor)`.

Счётчик обновляется в трёх местах:
- `bsc_fill_on_new_head`
- `ghost_fill_watch_bsc`
- `ghost_fill_watch_bsc_on_head`

### 3. Исправлены права `/data/`
Все контейнеры работают как `app` (uid=100, gid=101).  
Папка `/opt/scriptpoly-15m/data/` была создана от `polybot` (uid=1000) → контейнеры не могли писать.  
**Фикс:** `sudo chown -R 100:101 /opt/scriptpoly-15m/data/`

### 4. Балансировщик: попытка перехода на native USDC (`balancer/main.py`)
Пользователь сказал "Polymarket перешёл на native USDC" → сменили токен с pUSD на `0x3c499c...`.  
Проверили on-chain балансы кошелька `0x656f6C34...`:
```
native USDC (0x3c499c): 0.00
pUSD        (0xc011..): 233.09  ← здесь реальные деньги
USDC.e      (0x2791..):  34.01
```
Балансировщик видел `POLYMARKET USDC=0.00$` → **вернули токен на pUSD**.

**Коммит:** `79481f2` — revert to pUSD

---

## Текущий статус

### Что работает
- Трейдер запущен, `_late_fill_watcher` исправлен
- Права `/data/` починены, `settings.json` пишется
- Балансировщик видит pUSD (ждёт реконфигурации docker-compose)

### Проблема: docker-compose.yml на сервере расходится с репо

На сервере `/opt/scriptpoly-15m/docker-compose.yml` имеет **локальные правки**:
- container_name с суффиксом `-15m` (нужно)
- `POLYGON_USDC_ADDRESS: 0x3c499c...` (неверно — надо `POLYGON_PUSD_ADDRESS: 0xc011...`)
- `git pull` падает с `error: Your local changes... would be overwritten`

**Что нужно сделать:**
1. Взять серверный docker-compose как основу (там правильные container_name `-15m`)
2. Поменять `POLYGON_USDC_ADDRESS` → `POLYGON_PUSD_ADDRESS: 0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb`
3. Закоммитить в репо и задеплоить на сервер

### Как задеплоить на сервер
```bash
# SSH на сервере через polybot (имеет sudo NOPASSWD)
ssh polybot@111.88.147.230

# Pull с правильным SSH ключом (github.com-phenkka → id_ed25520)
cd /opt/scriptpoly-15m
GIT_SSH_COMMAND='ssh -i ~/.ssh/id_ed25520 -o StrictHostKeyChecking=no' git pull

# Ребилд нужного сервиса
sudo docker compose up -d --build balancer
```

---

## Следующие шаги

- [ ] Привести docker-compose.yml в репо в соответствие с серверным (container_name `-15m`)
- [ ] Заменить `POLYGON_USDC_ADDRESS` → `POLYGON_PUSD_ADDRESS: 0xc011...` в docker-compose
- [ ] Задеплоить на сервер → убедиться что `POLYMARKET pUSD=233$` появляется в логах балансировщика
- [ ] Проверить что балансировщик начал перекидывать pUSD → Predict (порог 70$, target 100$)
- [ ] Понять почему Predict баланс упал до 3.85$ (балансировщик делал equalize → отправил USDT с Predict на bridge)

---

## Ключевые файлы
| Файл | Роль |
|---|---|
| `trader/main.py` | Исполнение ордеров, `_late_fill_watcher`, `_predict_mkt_confirmed_shares` |
| `balancer/main.py` | Ребалансировка кошельков Poly↔Predict |
| `docker-compose.yml` | Конфиг контейнеров (на сервере расходится с репо!) |
| `collect/btc_orderbook.py` | Стакан Polymarket, SLOT_SEC=900 |
| `collect/predict_orderbook.py` | Стакан Predict.fun, SLOT_MIN=15 |

## Сервер
- Хост: `111.88.147.230`, пользователь: `polybot` (sudo NOPASSWD)
- Проект: `/opt/scriptpoly-15m/`
- SSH ключ для github pull: `~/.ssh/id_ed25520` (Host alias: `github.com-phenkka`)
- Данные: `/opt/scriptpoly-15m/data/` (владелец uid=100, gid=101 — app user внутри контейнеров)
