# Setup Guide

## Prerequisites

- Python 3.10+
- A Groww trading account (master)
- One or more follower Groww accounts
- A Telegram bot token (for auth and scan alerts)

## Installation

```bash
git clone https://github.com/nafees00alam/groww-trade-copier.git
cd groww-trade-copier
python -m venv .venv
source .venv/bin/activate   # Linux/Mac
# .venv\Scripts\activate    # Windows
pip install -r requirements.txt
```

## Configuration

Copy the example config and fill in your credentials:

```bash
cp config.example.json config.json
```

See [[Configuration]] for detailed field descriptions.

## First Run

```bash
python app.py
```

This starts:
- FastAPI server on port 8002
- Telegram bot polling
- WebSocket feed for order detection
- All enabled scanners (Greeks, OI, scalp)

Open `http://localhost:8002` for the dashboard.

## Telegram Login

1. Send `/login` to your bot
2. Click the login link
3. You're authenticated in the dashboard — no passwords needed
