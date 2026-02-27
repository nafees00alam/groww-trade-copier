# Configuration

All settings live in `config.json`.

## Master Account

```json
{
  "master": {
    "name": "Master",
    "api_key": "your-groww-api-key",
    "secret": "your-groww-secret",
    "use_totp": true,
    "totp_secret": "your-totp-base32-secret",
    "enabled": true
  }
}
```

| Field | Description |
|-------|-------------|
| `name` | Display name |
| `api_key` | Groww API key |
| `secret` | Groww API secret (used if `use_totp` is false) |
| `use_totp` | If true, generates TOTP codes automatically |
| `totp_secret` | Base32 TOTP secret for auto-generating codes |

## Followers

```json
{
  "followers": [
    {
      "name": "Follower1",
      "api_key": "...",
      "secret": "...",
      "use_totp": true,
      "totp_secret": "...",
      "enabled": true,
      "lot_mode": "same",
      "lot_fixed": 1,
      "lot_multiplier": 1.0
    }
  ]
}
```

### Lot Modes

| Mode | Description |
|------|-------------|
| `same` | Copy exact same quantity as master |
| `fixed` | Always use `lot_fixed` quantity |
| `multiplier` | Multiply master qty by `lot_multiplier` |

## Other Settings

| Field | Default | Description |
|-------|---------|-------------|
| `poll_interval_seconds` | 3 | Polling fallback interval (WebSocket is primary) |
