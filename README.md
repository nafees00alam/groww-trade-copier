# Groww F&O Trade Copier

Copy F&O and commodity trades from your master Groww account to multiple followers — instantly, automatically.

**One master trades. Everyone follows.**

## What It Does

You place a trade on your Groww account. Within seconds, the same trade appears on all your follower accounts — with configurable lot sizing (same, fixed, or multiplied). Cancellations and modifications sync too.

## Highlights

- **Real-time** — WebSocket feed, not polling. Sub-second detection.
- **Auto-recovery** — Token expiry auto-reauth, feed auto-reconnect. Zero manual intervention.
- **Web dashboard** — Orders, positions, P&L, market indices, copy log. Dark theme. No build step.
- **OI Analytics** — PCR, max pain, buildup signals, unusual activity, trade suggestions.
- **Greeks Scanner** — Option chain Greeks (delta, gamma, theta, vega, IV) with trade signals.
- **Scalp Alerts** — 1-minute scanner with EMA cross, OR breakout, S/R bounce, RSI signals.
- **Chart Images** — Candlestick charts with S/R, ATM, BB, Fib, EMA/SMA, PDH/PDL overlays.
- **Telegram auth** — Send `/login`, click link, you're in. No passwords.
- **Dry run** — Test everything without placing real orders.
- **TOTP** — Auto-generates Groww TOTP codes. No manual entry.

## Quick Start

```bash
git clone https://github.com/nafees00alam/groww-trade-copier.git
cd groww-trade-copier
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config.example.json config.json  # edit with your credentials
python app.py                       # open http://localhost:8002
```

## Architecture

```
app.py                  # FastAPI server + copier + scanners + Telegram bot
copier.py               # Standalone CLI copier (headless polling mode)
dashboard.html          # Trade copier dashboard (SPA)
oi.html                 # OI Analytics dashboard (SPA)
config.json             # Master/follower credentials + settings
```

### Key Systems

| System | Description |
|--------|-------------|
| **Trade Copier** | Detects master orders via WebSocket feed, copies to followers with lot sizing |
| **Greeks Scanner** | Scans option chain every 2 min, generates signals at 75+ confidence after 3 scans |
| **Scalp Scanner** | 1-min internal scan, 5 consecutive confirmations, 15-min cooldown per underlying |
| **OI Analytics** | PCR, max pain, OI buildup, unusual activity detection |
| **Hourly Scan** | Automated scan at each hour boundary during market hours (9:15-15:30 IST) |
| **Auto-Reauth** | Detects expired Groww tokens, re-authenticates automatically with 30s cooldown |
| **Feed Reconnect** | WebSocket feed auto-reconnects with reauth on disconnect (up to 50 retries) |

### Scan Signal Types

| Emoji | Type | Description |
|-------|------|-------------|
| :chart_with_upwards_trend: | Manual scan | Triggered via `/scan` command |
| :clock1: | Hourly scan | Automated at each hour boundary |
| :dart: | Greeks signal | 75+ confidence after 3+ scans |
| :zap: | Scalp alert | 5 consecutive 1-min confirmations |

## Docs

See the [Wiki](https://github.com/nafees00alam/groww-trade-copier/wiki) for setup guide, configuration, API reference, deployment, and architecture details.

## Disclaimer

Educational purposes only. Automated trading involves financial risk. Test with dry run mode first.

## License

MIT
