# scriptpoly

Arbitrage bot between [Polymarket](https://polymarket.com) and [Predict.fun](https://predict.fun).

Finds opportunities where the sum of complementary outcome prices on both platforms is less than 1.0, buys both sides, and locks in a guaranteed profit.

---

## Services

| Service | Description |
|---|---|
| `collect_polymarket` | Streams live quotes from Polymarket CLOB |
| `collect_predict` | Streams live quotes from Predict.fun |
| `analyzer` | Detects arbitrage opportunities and forwards them to the trader |
| `trader` | Executes orders on both platforms, manages hedging and incidents |
| `balancer` | Monitors and rebalances USDC/USDT across wallets |
| `claimer` | Claims resolved positions on both platforms |
| `bot` | Telegram bot for notifications and settings management |

---

## Quick Start

```bash
# Run everything
docker compose up -d --build

# Run a single service (e.g. during development)
docker compose up --build collect_polymarket
docker compose up --build collect_predict
```

---

## Useful Commands

### Logs

```bash
# Follow all logs
docker compose logs -f

# Follow a specific service
docker compose logs -f analyzer
docker compose logs -f trader
docker compose logs -f collect_predict
docker compose logs -f collect_polymarket
```

### Monitoring

```bash
# Live arbitrage opportunities detected by analyzer
docker compose logs -f analyzer | grep ARBITRAGE

# Executed trades
docker compose logs -f trader | grep TRADE

# Incidents (unhedged positions, API lag, etc.)
docker compose logs -f trader | grep INCIDENT
```

### Restart & Deploy

```bash
# Rebuild and restart a single service (e.g. after code change)
docker compose up -d --build trader

# Restart without rebuild
docker compose restart trader

# Stop everything
docker compose down
```

### Server Deploy

```bash
git push
ssh polybot "cd ~/polybot/scriptpoly && git pull && docker compose up -d --build trader"
```
