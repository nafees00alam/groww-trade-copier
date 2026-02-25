# Groww F&O Trade Copier

Auto-copy F&O and commodity trades from a master Groww account to multiple follower accounts in real-time. Comes with a web dashboard, OI analytics, and Telegram bot authentication.

## Features

- **Instant trade copying** — WebSocket feed detects master orders in real-time, copies to all followers
- **Configurable lot sizing** — Same as master, fixed quantity, or multiplier per follower
- **Web dashboard** — Live status, orders, positions, P&L, copy log, market indices
- **OI Analytics** — Open Interest analysis with PCR, max pain, buildup signals, unusual activity, and trade suggestions (dedicated `/oi` page)
- **Telegram bot auth** — `/login` command generates a one-time link, no passwords needed
- **Admin panel** — Manage followers via Telegram bot (`/admin`)
- **Dry run mode** — Test without placing real orders
- **Market indices** — Live NIFTY, BANKNIFTY, SENSEX and 25+ indices with OHLC data
- **Sound alerts** — Audio notifications for successful copies and errors
- **TOTP support** — Authenticate Groww accounts via TOTP (auto-generates codes)

## Screenshots

The dashboard shows live copier status, market indices, account info, orders, positions, trades, and copy log — all in a dark theme.

## Prerequisites

- Python 3.10+
- A [Groww](https://groww.in) account with F&O enabled
- Groww API credentials (API key + TOTP, or API key + secret)
- (Optional) A Telegram bot token from [@BotFather](https://t.me/BotFather)

## Setup

1. **Clone the repo**

   ```bash
   git clone https://github.com/nafees00alam/groww-trade-copier.git
   cd groww-trade-copier
   ```

2. **Create a virtual environment and install dependencies**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   # .venv\Scripts\activate   # Windows
   pip install -r requirements.txt
   ```

3. **Configure**

   ```bash
   cp config.example.json config.json
   ```

   Edit `config.json` with your credentials:

   | Field | Description |
   |-------|-------------|
   | `telegram_bot_token` | Bot token from BotFather (optional, set `"YOUR_BOT_TOKEN"` to disable) |
   | `jwt_secret` | Random string for session tokens |
   | `master.api_key` | Groww JWT token (from TOTP setup) or API key |
   | `master.totp_secret` | Base32 TOTP secret (the string below the QR code) |
   | `master.use_totp` | `true` for TOTP auth, `false` for API key + secret |
   | `followers[].api_key` | Each follower's Groww credentials |
   | `followers[].lot_multiplier` | Trade size multiplier (1 = same as master) |

4. **Run**

   ```bash
   python app.py
   ```

   Open `http://localhost:8002` in your browser.

## Authentication

Without a Telegram bot configured, you can access the dashboard by generating a JWT manually or configuring a bot token:

1. Create a bot via [@BotFather](https://t.me/BotFather) on Telegram
2. Set the token in `config.json`
3. Send `/login` to your bot — it returns a one-time login link
4. Sessions persist for 7 days

The master account (first `telegram_id` configured) gets full access. Followers can only see their own orders, positions, and credentials.

## Architecture

```
┌──────────────┐     WebSocket Feed      ┌──────────────────┐
│  Groww API   │◄────────────────────────►│     app.py       │
│  (Master)    │     Order Detection      │                  │
└──────────────┘                          │  FastAPI Server  │
                                          │  + Copier Engine │
┌──────────────┐     Place Orders         │  + Telegram Bot  │
│  Groww API   │◄─────────────────────────│  + OI Analytics  │
│  (Followers) │                          │                  │
└──────────────┘                          └────────┬─────────┘
                                                   │
                                          ┌────────┴─────────┐
                                          │   Dashboard UI   │
                                          │  (WebSocket +    │
                                          │   REST API)      │
                                          └──────────────────┘
```

- **Order detection**: WebSocket feed from Groww for instant detection (falls back to polling)
- **Copy engine**: Detects new/modified/cancelled master orders, replicates to followers with configurable lot sizing
- **Dashboard**: Single-page HTML app, no build step, communicates via REST + WebSocket
- **OI Analytics**: Polls option chain data, computes PCR, max pain, buildup signals, unusual activity. Data pushed to `/oi` page via WebSocket.

## Lot Modes

| Mode | Behavior |
|------|----------|
| `same` | Copy exact quantity from master |
| `fixed` | Always trade a fixed number of lots |
| `multiplier` | Multiply master's quantity (e.g., 2x) |

Configure per follower in the dashboard settings or `config.json`.

## CLI Mode

For headless operation without the web dashboard:

```bash
python copier.py
```

This runs the polling-based copier in the terminal with console output.

## Deployment

### systemd (Linux)

```ini
[Unit]
Description=Groww F&O Trade Copier
After=network.target

[Service]
User=your-user
WorkingDirectory=/path/to/groww-trade-copier
ExecStart=/path/to/groww-trade-copier/.venv/bin/python app.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable groww-copier
sudo systemctl start groww-copier
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Dashboard |
| GET | `/oi` | OI Analytics page |
| GET | `/api/status` | Copier status |
| GET | `/api/account/{name}/orders` | Account orders |
| GET | `/api/account/{name}/positions` | Account positions |
| GET | `/api/account/{name}/margin` | Account margin |
| GET | `/api/indices` | Market indices |
| GET | `/api/copy-log` | Copy log entries |
| GET | `/api/oi/status` | OI analytics status |
| GET | `/api/oi/analysis` | Full OI analysis |
| POST | `/api/copier/start` | Start copier |
| POST | `/api/copier/stop` | Stop copier |
| POST | `/api/oi/start` | Start OI polling |
| POST | `/api/oi/stop` | Stop OI polling |
| GET | `/api/settings` | Get configuration |
| PUT | `/api/settings/master` | Update master config |
| WS | `/ws` | Live updates |

## Disclaimer

This software is for educational purposes. Use at your own risk. Automated trading involves financial risk. The authors are not responsible for any losses incurred. Always test with dry run mode first.

## License

MIT
