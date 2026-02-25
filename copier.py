"""
Groww F&O Trade Copier
- Polls master account for new orders
- Copies them to multiple follower accounts
- Configurable lot multiplier per follower
"""

import json
import time
import math
import logging
from pathlib import Path
from dataclasses import dataclass, field

import pyotp
from growwapi import GrowwAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("copier")

CONFIG_PATH = Path(__file__).parent / "config.json"
STATE_FILE = Path(__file__).parent / "copied_orders.json"


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class AccountConfig:
    name: str
    api_key: str
    secret: str
    use_totp: bool = False
    totp_secret: str = ""
    lot_multiplier: float = 1.0
    enabled: bool = True


@dataclass
class CopierState:
    """Tracks which master orders have already been copied."""
    copied_order_ids: set = field(default_factory=set)

    def save(self):
        STATE_FILE.write_text(json.dumps(list(self.copied_order_ids), indent=2))

    @classmethod
    def load(cls):
        if STATE_FILE.exists():
            ids = set(json.loads(STATE_FILE.read_text()))
            return cls(copied_order_ids=ids)
        return cls()


# ── Auth ──────────────────────────────────────────────────────────────────────

def authenticate(acc: AccountConfig) -> GrowwAPI:
    """Authenticate and return a GrowwAPI client."""
    if acc.use_totp and acc.totp_secret:
        totp = pyotp.TOTP(acc.totp_secret).now()
        token = GrowwAPI.get_access_token(api_key=acc.api_key, totp=totp)
    else:
        token = GrowwAPI.get_access_token(api_key=acc.api_key, secret=acc.secret)
    log.info(f"Authenticated: {acc.name}")
    return GrowwAPI(token)


# ── Core logic ────────────────────────────────────────────────────────────────

def fetch_master_orders(client: GrowwAPI) -> list[dict]:
    """Get today's FNO orders from master account."""
    try:
        raw = client.get_order_list()
        if not raw:
            return []
        # API returns {'order_list': [...]}
        orders = raw.get("order_list", []) if isinstance(raw, dict) else raw
        return [o for o in orders if o.get("segment", "").upper() in ("FNO", "COMMODITY")]
    except Exception as e:
        log.error(f"Failed to fetch master orders: {e}")
        return []


def copy_order_to_follower(
    follower_client: GrowwAPI,
    follower: AccountConfig,
    order: dict,
):
    """Place a copied order on a follower account."""
    # Calculate quantity with lot multiplier
    original_qty = int(order.get("quantity", 0))
    copied_qty = max(1, math.floor(original_qty * follower.lot_multiplier))

    order_type = order.get("order_type", "LIMIT").upper()
    txn_type = order.get("transaction_type", "BUY").upper()

    # Map order type strings to SDK constants
    type_map = {
        "MARKET": follower_client.ORDER_TYPE_MARKET,
        "LIMIT": follower_client.ORDER_TYPE_LIMIT,
        "SL": follower_client.ORDER_TYPE_STOP_LOSS,
        "SL-M": follower_client.ORDER_TYPE_STOP_LOSS_MARKET,
        "STOP_LOSS": follower_client.ORDER_TYPE_STOP_LOSS,
        "STOP_LOSS_MARKET": follower_client.ORDER_TYPE_STOP_LOSS_MARKET,
    }
    txn_map = {
        "BUY": follower_client.TRANSACTION_TYPE_BUY,
        "SELL": follower_client.TRANSACTION_TYPE_SELL,
    }
    segment_map = {
        "FNO": follower_client.SEGMENT_FNO,
        "COMMODITY": follower_client.SEGMENT_COMMODITY,
        "CASH": follower_client.SEGMENT_CASH,
    }
    product_map = {
        "MIS": follower_client.PRODUCT_MIS,
        "CNC": follower_client.PRODUCT_CNC,
        "NRML": follower_client.PRODUCT_NRML,
    }

    segment_str = order.get("segment", "FNO").upper()
    product_str = order.get("product", "MIS").upper()
    exchange_str = order.get("exchange", "NSE").upper()

    exchange_map = {
        "NSE": follower_client.EXCHANGE_NSE,
        "BSE": follower_client.EXCHANGE_BSE,
        "MCX": follower_client.EXCHANGE_MCX,
    }

    kwargs = dict(
        trading_symbol=order["trading_symbol"],
        quantity=copied_qty,
        validity=follower_client.VALIDITY_DAY,
        exchange=exchange_map.get(exchange_str, follower_client.EXCHANGE_NSE),
        segment=segment_map.get(segment_str, follower_client.SEGMENT_FNO),
        product=product_map.get(product_str, follower_client.PRODUCT_MIS),
        order_type=type_map.get(order_type, follower_client.ORDER_TYPE_LIMIT),
        transaction_type=txn_map.get(txn_type, follower_client.TRANSACTION_TYPE_BUY),
    )

    # Add price fields if present
    price = order.get("price")
    if price and float(price) > 0:
        kwargs["price"] = float(price)

    trigger = order.get("trigger_price")
    if trigger and float(trigger) > 0:
        kwargs["trigger_price"] = float(trigger)

    try:
        resp = follower_client.place_order(**kwargs)
        log.info(
            f"COPIED → {follower.name}: {txn_type} {copied_qty}x "
            f"{order['trading_symbol']} @ {price or 'MKT'} | "
            f"Order ID: {resp.get('groww_order_id', 'N/A')}"
        )
        return resp
    except Exception as e:
        log.error(f"FAILED → {follower.name}: {order['trading_symbol']} | {e}")
        return None


def handle_modifications(
    master_client: GrowwAPI,
    follower_clients: dict[str, tuple[GrowwAPI, AccountConfig]],
    state: CopierState,
):
    """
    Detect cancelled orders on master and cancel corresponding orders on followers.
    This is a basic implementation - you can extend it for modifications too.
    """
    # TODO: Track order_id mappings (master → follower) for cancel sync
    pass


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    # Load config
    config = json.loads(CONFIG_PATH.read_text())
    poll_interval = config.get("poll_interval_seconds", 3)

    master_cfg = AccountConfig(**config["master"])
    follower_cfgs = [AccountConfig(**f) for f in config["followers"] if f.get("enabled", True)]

    if not follower_cfgs:
        log.error("No enabled followers in config. Exiting.")
        return

    # Authenticate all accounts
    log.info("=" * 50)
    log.info("GROWW F&O TRADE COPIER")
    log.info("=" * 50)

    master_client = authenticate(master_cfg)
    follower_clients: dict[str, tuple[GrowwAPI, AccountConfig]] = {}
    for fc in follower_cfgs:
        try:
            client = authenticate(fc)
            follower_clients[fc.name] = (client, fc)
        except Exception as e:
            log.error(f"Failed to authenticate {fc.name}: {e}")

    if not follower_clients:
        log.error("No followers authenticated. Exiting.")
        return

    log.info(f"Master: {master_cfg.name}")
    for name, (_, fc) in follower_clients.items():
        log.info(f"Follower: {name} (lot multiplier: {fc.lot_multiplier}x)")
    log.info(f"Poll interval: {poll_interval}s")
    log.info("-" * 50)

    # Load state
    state = CopierState.load()
    log.info(f"Loaded state: {len(state.copied_order_ids)} previously copied orders")

    # Main polling loop
    try:
        while True:
            orders = fetch_master_orders(master_client)

            new_orders = [
                o for o in orders
                if o.get("groww_order_id") and o["groww_order_id"] not in state.copied_order_ids
                and o.get("order_status", "").upper() in ("EXECUTED", "OPEN", "PENDING")
            ]

            if new_orders:
                log.info(f"Found {len(new_orders)} new order(s) to copy")

            for order in new_orders:
                oid = order["groww_order_id"]
                symbol = order.get("trading_symbol", "???")
                log.info(
                    f"NEW ORDER: {order.get('transaction_type')} "
                    f"{order.get('quantity')}x {symbol} "
                    f"@ {order.get('price', 'MKT')} [{order.get('order_status')}]"
                )

                for name, (client, fc) in follower_clients.items():
                    copy_order_to_follower(client, fc, order)

                state.copied_order_ids.add(oid)
                state.save()

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        log.info("\nStopping copier...")
        state.save()
        log.info("State saved. Bye!")


if __name__ == "__main__":
    main()
