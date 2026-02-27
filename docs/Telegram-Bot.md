# Telegram Bot

The built-in Telegram bot handles authentication and scan commands.

## Commands

| Command | Description |
|---------|-------------|
| `/start` | Welcome message |
| `/login` | Get a one-time dashboard login link |
| `/scan` | Trigger manual scan for all underlyings |

## Login Flow

1. User sends `/login` to the bot
2. Bot generates a one-time token (expires in 5 minutes)
3. Bot replies with a clickable link: `http://your-server:8002/auth/{token}`
4. Clicking the link authenticates the browser session
5. Dashboard loads with full access

No passwords, no registration. Identity is tied to Telegram user ID.

## Scan Alerts

The bot sends scan results and alerts to configured Telegram chat IDs:

- **Manual scan** (📊) — triggered by `/scan` command
- **Hourly scan** (🕐) — automatic at each hour boundary during market hours
- **Greeks signal** (🎯) — high-confidence trade signal after 3+ scans
- **Scalp alert** (⚡) — short-term scalp opportunity with 5x confirmation

Each scan message includes:
- Direction (BULLISH/BEARISH) with confidence score
- Greeks summary (PCR, IV skew, max pain, delta exposure)
- TradingView multi-timeframe scores
- Candlestick chart image with technical overlays
- Quick Trade buttons for immediate order placement

## Configuration

Set your bot token and alert chat IDs in `config.json`:

```json
{
  "telegram_bot_token": "123456:ABC...",
  "alert_telegram_ids": [123456789]
}
```
