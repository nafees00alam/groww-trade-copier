# Scanners & Signals

## Greeks Scanner

Runs every 2 minutes during market hours (9:15 AM – 3:30 PM IST).

**Underlyings:** NIFTY (Monday expiry), BANKNIFTY (monthly), SENSEX (Thursday)

### How It Works

1. Fetches option chain for nearest expiry
2. Extracts Greeks for ATM ± 5 strikes: delta, gamma, theta, vega, IV
3. Fetches TradingView indicators across 5 timeframes (W, D, 1H, 15m, 5m)
4. Generates candlestick chart image with overlays
5. After 3+ scans: runs 6-layer signal confirmation
6. Signals with confidence ≥ 75 are sent to Telegram

### Signal Layers

| Layer | What It Checks |
|-------|---------------|
| Greeks momentum | Delta/gamma shift direction over 3+ scans |
| IV skew | Call vs put IV imbalance |
| OI buildup | Put vs call OI accumulation |
| Price trend | Underlying price direction from LTP history |
| TradingView score | Multi-timeframe technical analysis |
| Chart alignment | All layers must agree on direction |

### Chart Overlays

- Candlestick (15m, yfinance)
- Support/Resistance (from OI data)
- ATM strike level
- Bollinger Bands
- Fibonacci retracements
- EMA20 / SMA50
- PDH / PDL / PDC (previous day)
- ORH / ORL (opening range)
- De-collided labels

## Scalp Scanner

Runs every 60 seconds. Sends alerts for short-term scalp opportunities.

### Conditions Detected

| Signal | Direction | Trigger |
|--------|-----------|---------|
| EMA Cross | Bullish/Bearish | EMA20 crosses SMA50 with ≥0.1% spread |
| OR Breakout | Bullish/Bearish | Price sustaining above/below opening range |
| PDH/PDL Break | Bullish/Bearish | Price sustaining above PDH or below PDL |
| RSI Bounce | Bullish | RSI 5m recovering from oversold (30-40) |
| RSI Reversal | Bearish | RSI 5m cooling from overbought (60-70) |
| S/R Bounce | Bullish/Bearish | Price bouncing at support or rejecting at resistance |

### Confirmation

- 5 consecutive 1-minute confirmations needed before firing
- Maximum 1 alert per underlying per 15 minutes
- Direction lock prevents contradictory signals
- Warmup period on startup (first scan is silent)

## Hourly Scan

- Fires at each hour boundary during market hours
- Waits for next hour boundary on restart (no immediate fire)
- Uses same Greeks + TradingView analysis as manual scan
- Sends results to all configured Telegram alert IDs

## Signal Message Types

| Emoji | Header | When |
|-------|--------|------|
| 📊 | Manual scan | User triggers `/scan` |
| 🕐 | Hourly scan | Automatic at hour boundary |
| 🎯 | Greeks signal | 75+ confidence after 3+ scans |
| ⚡ | Scalp alert | 5 consecutive 1-min confirmations |
