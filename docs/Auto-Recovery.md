# Auto Recovery

The system automatically recovers from Groww API token expiry and WebSocket disconnections without manual intervention.

## Token Auto-Reauth

Groww API tokens expire periodically. When any API call returns `"Authentication failed. Your API token has either expired or is invalid."`, the system:

1. **Detects** the auth error via `is_auth_error()`
2. **Re-authenticates** via `reauth_master()` — generates fresh TOTP, gets new token
3. **Retries** the failed operation with the new token
4. **Cooldown**: 30-second minimum between reauth attempts (thread-safe lock)

### Protected Functions

| Function | Used By |
|----------|---------|
| `scan_greeks_for_underlying()` | Greeks scanner, scalp scanner |
| `fetch_option_chain()` | OI analytics |
| `fetch_master_orders()` | Trade copier polling |

### Flow

```
API call → Auth error?
  ├── No → Return result normally
  └── Yes → reauth_master()
              ├── Cooldown active? → Skip (use existing client)
              └── Get new token → Update state.master_client
                    ├── Success → Retry original call
                    └── Failure → Log error, return empty
```

## WebSocket Feed Reconnect

The GrowwFeed WebSocket runs in a dedicated thread. If it disconnects:

1. Detects disconnect (when `feed.consume()` returns or throws)
2. Waits 10 seconds
3. Re-authenticates master (gets fresh token)
4. Creates new GrowwFeed with fresh client
5. Re-subscribes to order updates + index values
6. Resumes consuming

Up to 50 reconnect attempts across a trading day. If reauth fails, waits 60 seconds before retrying.

## What This Replaces

Previously, token expiry required manual service restart (`systemctl restart groww-copier` or `kill -9`). The system would spam auth errors in logs until restarted. Now it self-heals automatically.
