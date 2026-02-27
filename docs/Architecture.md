# Architecture

## Overview

Single-file FastAPI application (`app.py`) that combines:
- **Trade copier** — WebSocket feed + polling fallback
- **REST API** — Dashboard data, account info, scanner controls
- **Telegram bot** — Auth, scan commands, alerts
- **Background scanners** — Greeks, OI analytics, scalp, hourly

## Components

```
┌─────────────────────────────────────────────┐
│                  app.py                      │
├─────────────┬─────────────┬─────────────────┤
│  FastAPI     │  Telegram   │  Background     │
│  REST API    │  Bot        │  Tasks          │
│  WebSocket   │  (aiogram)  │                 │
├─────────────┤             ├─────────────────┤
│  Dashboard   │  /login     │  Trade Copier   │
│  OI Page     │  /scan      │  Greeks Scanner │
│              │  /start     │  OI Analytics   │
│              │  /stop      │  Scalp Scanner  │
│              │             │  Hourly Scan    │
└─────────────┴─────────────┴─────────────────┘
        │                          │
        ▼                          ▼
┌──────────────┐          ┌──────────────────┐
│  GrowwAPI    │          │  GrowwFeed       │
│  (REST)      │          │  (WebSocket)     │
│  Orders      │          │  Order updates   │
│  Positions   │          │  Index values    │
│  Option chain│          │  LTP stream      │
└──────────────┘          └──────────────────┘
```

## Data Flow — Trade Copying

1. **Master places order** on Groww
2. **GrowwFeed** (WebSocket) detects order update in <1s
3. **Order processor** checks if it's a new/modified/cancelled FNO order
4. **For each follower**: adjusts lot size, places matching order via GrowwAPI
5. **Copy log** updated, broadcast to dashboard via WebSocket

Fallback: if WebSocket feed is down, polling runs every N seconds.

## Data Flow — Greeks Scanner

1. Every 2 minutes, for each underlying (NIFTY, BANKNIFTY, SENSEX):
2. Fetch option chain via `GrowwAPI.get_option_chain()`
3. Extract Greeks (delta, gamma, theta, vega, IV) for ATM ± 5 strikes
4. Fetch TradingView chart data (RSI, MACD, EMA, BB across 5 timeframes)
5. Generate chart image (candlestick + overlays)
6. After 3+ scans: generate signals if confidence ≥ 75
7. Send to Telegram with chart image

## Auto-Recovery

See [[Auto Recovery]] for details on token reauth and feed reconnect.
