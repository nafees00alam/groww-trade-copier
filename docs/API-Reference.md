# API Reference

Base URL: `http://localhost:8002`

## Dashboard

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Trade copier dashboard |
| GET | `/oi` | OI Analytics page |
| WebSocket | `/ws` | Real-time updates (copy log, indices, status) |

## Copier

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/status` | Copier status, uptime, stats |
| POST | `/api/start` | Start trade copier |
| POST | `/api/stop` | Stop trade copier |
| POST | `/api/dry-run` | Toggle dry run mode |
| GET | `/api/copy-log` | Recent copy log entries |

## Accounts

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/account/{name}/orders` | Get account orders |
| GET | `/api/account/{name}/positions` | Get account positions |
| GET | `/api/account/{name}/margin` | Get account margin |
| GET | `/api/followers` | List all followers with config |
| POST | `/api/followers` | Add a follower |
| PUT | `/api/followers/{name}` | Update a follower |
| DELETE | `/api/followers/{name}` | Delete a follower |
| PUT | `/api/master` | Update master config |

## Market Data

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/indices` | All index values from feed |
| GET | `/api/indices?pinned=NIFTY,BANKNIFTY` | Pinned indices with OHLC |

## Scanners

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/oi/start` | Start OI analytics scanner |
| POST | `/api/oi/stop` | Stop OI analytics scanner |
| GET | `/api/oi/config` | Get OI scanner config |
| POST | `/api/oi/config` | Update OI scanner config |
| GET | `/api/oi/data` | Latest OI snapshots |
| POST | `/api/greeks/start` | Start Greeks scanner |
| POST | `/api/greeks/stop` | Stop Greeks scanner |
| GET | `/api/greeks/config` | Get Greeks scanner config |
| POST | `/api/greeks/config` | Update Greeks scanner config |

## Auth

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/auth/{token}` | Telegram login callback |
