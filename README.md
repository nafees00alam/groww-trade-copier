# Groww F&O Trade Copier

Copy F&O and commodity trades from your master Groww account to multiple followers — instantly, automatically.

**One master trades. Everyone follows.**

## What It Does

You place a trade on your Groww account. Within seconds, the same trade appears on all your follower accounts — with configurable lot sizing (same, fixed, or multiplied). Cancellations and modifications sync too.

## Highlights

- **Real-time** — WebSocket feed, not polling. Sub-second detection.
- **Web dashboard** — Orders, positions, P&L, market indices, copy log. Dark theme. No build step.
- **OI Analytics** — PCR, max pain, buildup signals, unusual activity, trade suggestions.
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

## Docs

See the [Wiki](https://github.com/nafees00alam/groww-trade-copier/wiki) for setup guide, configuration, API reference, deployment, and architecture details.

## Disclaimer

Educational purposes only. Automated trading involves financial risk. Test with dry run mode first.

## License

MIT
