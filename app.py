"""
Groww F&O Trade Copier — Web Dashboard
FastAPI app with background copier + REST API + WebSocket + Telegram auth
"""

import json
import time
import math
import uuid
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict

import threading
import pyotp
import jwt
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request, Cookie
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from growwapi import GrowwAPI, GrowwFeed


from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("copier")

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
STATE_FILE = BASE_DIR / "copied_orders.json"
DASHBOARD_PATH = BASE_DIR / "dashboard.html"
OI_PATH = BASE_DIR / "oi.html"


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class AccountConfig:
    name: str
    api_key: str
    secret: str
    use_totp: bool = False
    totp_secret: str = ""
    lot_mode: str = "same"  # "same", "fixed", "multiplier"
    lot_multiplier: float = 1.0
    lot_fixed: int = 1
    enabled: bool = True
    telegram_id: int = 0


@dataclass
class CopierState:
    copied_order_ids: set = field(default_factory=set)

    def save(self):
        STATE_FILE.write_text(json.dumps(list(self.copied_order_ids), indent=2))

    @classmethod
    def load(cls):
        if STATE_FILE.exists():
            ids = set(json.loads(STATE_FILE.read_text()))
            return cls(copied_order_ids=ids)
        return cls()


@dataclass
class OISnapshot:
    underlying: str
    expiry: str
    timestamp: str
    underlying_ltp: float
    strikes: dict  # strike -> {ce_oi, pe_oi, ce_volume, pe_volume, ce_ltp, pe_ltp, ce_iv, pe_iv}
    total_ce_oi: int = 0
    total_pe_oi: int = 0


@dataclass
class OIAnalysis:
    underlying: str
    expiry: str
    timestamp: str
    underlying_ltp: float
    pcr: float = 0.0
    max_pain: float = 0.0
    sentiment: str = "Neutral"
    support_level: float = 0.0
    resistance_level: float = 0.0
    buildup_signals: list = field(default_factory=list)
    unusual_activity: list = field(default_factory=list)
    trade_suggestions: list = field(default_factory=list)


# ── Shared app state ──────────────────────────────────────────────────────────

class AppState:
    def __init__(self):
        self.running = False
        self.dry_run = False
        self.start_time: float | None = None
        self.poll_interval = 3
        self.copy_log: list[dict] = []
        self.total_copied = 0
        self.total_failed = 0
        self.copier_task: asyncio.Task | None = None

        self.master_cfg: AccountConfig | None = None
        self.master_client: GrowwAPI | None = None
        self.follower_cfgs: list[AccountConfig] = []
        self.follower_clients: dict[str, tuple[GrowwAPI, AccountConfig]] = {}

        self.copier_state = CopierState.load()
        self.ws_clients: set[WebSocket] = set()

        self.master_feed: GrowwFeed | None = None
        self.feed_mode = True  # True = WebSocket feed, False = polling fallback
        self._loop: asyncio.AbstractEventLoop | None = None  # set at startup for thread-safe broadcast
        self.index_data: dict = {}  # latest index values from feed

        # Map master_order_id → {follower_name: follower_order_id} for cancel/modify sync
        self.order_map: dict[str, dict[str, str]] = {}
        # Last known qty/price per master order — to detect modifications
        self._last_order_snapshot: dict[str, dict] = {}

        # OI Analytics
        self.oi_snapshots: dict[str, list[OISnapshot]] = {}  # underlying -> [prev, current]
        self.oi_analysis: dict[str, OIAnalysis] = {}
        self.oi_task: asyncio.Task | None = None
        self.oi_running = False
        self.oi_last_poll: str | None = None

        # Greeks Scanner
        self.greeks_scanner_running = False
        self.greeks_scanner_task: asyncio.Task | None = None
        self.greeks_history: dict[str, list[dict]] = {}  # underlying → rolling window of last 5 snapshots
        self.ltp_history: dict[str, list[tuple[float, float]]] = {}  # index_token → [(timestamp, ltp)]
        self.signal_cooldown: dict[str, float] = {}  # symbol → last_signal_time
        self.bot_instance: Bot | None = None  # Telegram bot for signal delivery
        self.hourly_scan_running = False
        self.hourly_scan_task: asyncio.Task | None = None


    def add_log(self, event: str, symbol: str = "", follower: str = "",
                status: str = "info", details: str = ""):
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "symbol": symbol,
            "follower": follower,
            "status": status,
            "details": details,
        }
        self.copy_log.append(entry)
        if len(self.copy_log) > 200:
            self.copy_log = self.copy_log[-100:]
        # Schedule broadcast safely — may be called from feed thread or async context
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.broadcast(entry))
        except RuntimeError:
            # Called from non-async thread (e.g. feed callback) — schedule on main loop
            if self._loop:
                asyncio.run_coroutine_threadsafe(self.broadcast(entry), self._loop)

    async def broadcast(self, data: dict):
        dead = set()
        for ws in self.ws_clients:
            try:
                await ws.send_json(data)
            except Exception:
                dead.add(ws)
        self.ws_clients -= dead


state = AppState()


# ── Telegram Auth & JWT ──────────────────────────────────────────────────────

# In-memory login token store: token_str → {telegram_id, name, role, expires}
_login_tokens: dict[str, dict] = {}


def _read_config() -> dict:
    return json.loads(CONFIG_PATH.read_text())


def _write_config(config: dict):
    CONFIG_PATH.write_text(json.dumps(config, indent=4))


def _is_master(telegram_id: int) -> bool:
    return _read_config().get("master", {}).get("telegram_id") == telegram_id


def _get_jwt_secret() -> str:
    return _read_config().get("jwt_secret", "fallback-secret")


def _get_dashboard_url() -> str:
    return _read_config().get("dashboard_url", "http://localhost:8000")


def resolve_telegram_user(telegram_id: int) -> dict | None:
    """Look up telegram_id in config → return {name, role, account_name} or None."""
    config = _read_config()
    master = config.get("master", {})
    if master.get("telegram_id") == telegram_id:
        return {"name": master.get("name", "Master"), "role": "master",
                "account_name": master.get("name", "Master")}
    for f in config.get("followers", []):
        if f.get("telegram_id") == telegram_id:
            return {"name": f.get("name"), "role": "follower",
                    "account_name": f.get("name")}
    return None


def create_login_token(telegram_id: int, name: str, role: str) -> str:
    token = str(uuid.uuid4())
    _login_tokens[token] = {
        "telegram_id": telegram_id,
        "name": name,
        "role": role,
        "expires": time.time() + 300,  # 5 minutes
    }
    return token


def consume_login_token(token: str) -> dict | None:
    data = _login_tokens.pop(token, None)
    if not data:
        return None
    if time.time() > data["expires"]:
        return None
    return data


def create_jwt(telegram_id: int, name: str, role: str) -> str:
    payload = {
        "sub": str(telegram_id),
        "name": name,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(days=7),
    }
    return jwt.encode(payload, _get_jwt_secret(), algorithm="HS256")


def decode_jwt(token: str) -> dict | None:
    try:
        return jwt.decode(token, _get_jwt_secret(), algorithms=["HS256"])
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None


def get_current_user(request: Request) -> dict | None:
    """Extract user from JWT cookie. Returns {sub, name, role} or None."""
    token = request.cookies.get("session")
    if not token:
        return None
    return decode_jwt(token)


# ── Auth Middleware ───────────────────────────────────────────────────────────

# Paths that don't require authentication
_PUBLIC_PATHS = {"/auth", "/api/me", "/api/logout", "/", "/oi", "/ws"}


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        # Public paths and static assets
        if path in _PUBLIC_PATHS or not path.startswith("/api/"):
            return await call_next(request)

        user = get_current_user(request)
        if not user:
            return JSONResponse({"detail": "Not authenticated"}, status_code=401)

        # Inject user into request state for downstream use
        request.state.user = user
        return await call_next(request)


# ── Telegram Bot Setup ───────────────────────────────────────────────────────

tg_router = Router()


@tg_router.message(Command("start"))
async def cmd_start(message: Message):
    if _is_master(message.from_user.id):
        await message.answer(
            "Welcome, Master! 🎯\n\n"
            "Use /admin to manage users and access the dashboard.",
            reply_markup=admin_panel_kb(),
        )
    else:
        user_info = resolve_telegram_user(message.from_user.id)
        if user_info:
            await message.answer(
                f"Welcome, {user_info['name']}!\n\n"
                "Use /login to access the dashboard."
            )
        else:
            await message.answer("You don't have access. Ask the admin to add you.")


@tg_router.message(Command("scan"))
async def cmd_scan(message: Message):
    """On-demand Greeks scan for followers — show market snapshot without waiting for signals."""
    telegram_id = message.from_user.id
    user_info = resolve_telegram_user(telegram_id)
    if not user_info:
        await message.answer("Access denied. Your Telegram ID is not configured.")
        return
    if user_info["role"] == "master":
        await message.answer("Use the dashboard for scans. This command is for followers.")
        return

    if not state.master_client:
        await message.answer("Master account not connected. Scan unavailable.")
        return

    await message.answer("📡 Scanning chart + Greeks... please wait.")

    for underlying in GREEKS_UNDERLYINGS:
        try:
            # Fetch Greeks and chart in parallel
            snapshot, chart = await asyncio.gather(
                asyncio.to_thread(scan_greeks_for_underlying, state.master_client, underlying),
                asyncio.to_thread(_get_chart_analysis, underlying),
            )
            if not snapshot:
                await message.answer(f"{underlying}: No data available.")
                continue

            hist = state.greeks_history.get(underlying, [])
            text = _format_scan_summary(underlying, snapshot, hist, chart=chart)
            await message.answer(text, parse_mode="HTML")
        except Exception as e:
            log.error(f"Scan command error for {underlying}: {e}")
            await message.answer(f"{underlying}: Scan failed — {e}")


@tg_router.message(Command("login"))
async def cmd_login(message: Message):
    telegram_id = message.from_user.id
    user_info = resolve_telegram_user(telegram_id)
    if not user_info:
        await message.answer("Access denied. Your Telegram ID is not configured.")
        return

    token = create_login_token(telegram_id, user_info["name"], user_info["role"])
    url = f"{_get_dashboard_url()}/auth?token={token}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Login to Dashboard", url=url)],
    ])
    await message.answer(
        f"Hi {user_info['name']}! Tap the button below to login.\n\n"
        f"⏳ Expires in 5 minutes.",
        reply_markup=kb,
    )



# ── FSM States for admin flows ──
class AddUserFSM(StatesGroup):
    waiting_telegram_id = State()
    waiting_name = State()


# ── Admin Panel ──

def admin_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Users", callback_data="admin:users"),
         InlineKeyboardButton(text="➕ Add User", callback_data="admin:add")],
        [InlineKeyboardButton(text="🔑 Login Dashboard", callback_data="admin:login")],
    ])


@tg_router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not _is_master(message.from_user.id):
        await message.answer("Admin only.")
        return
    await state.clear()
    await message.answer("⚙️ <b>Admin Panel</b>", parse_mode="HTML", reply_markup=admin_panel_kb())


@tg_router.callback_query(F.data == "admin:login")
async def cb_admin_login(cb: CallbackQuery):
    if not _is_master(cb.from_user.id):
        await cb.answer("Admin only.", show_alert=True)
        return
    user_info = resolve_telegram_user(cb.from_user.id)
    token = create_login_token(cb.from_user.id, user_info["name"], user_info["role"])
    url = f"{_get_dashboard_url()}/auth?token={token}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Login to Dashboard", url=url)],
    ])
    await cb.message.answer(
        "Tap the button below to open the dashboard.\n\n⏳ Expires in 5 minutes.",
        reply_markup=kb,
    )
    await cb.answer()


@tg_router.callback_query(F.data == "admin:users")
async def cb_admin_users(cb: CallbackQuery):
    if not _is_master(cb.from_user.id):
        await cb.answer("Admin only.", show_alert=True)
        return

    config = _read_config()
    master = config.get("master", {})
    lines = [
        f"👤 <b>Master</b>: {master.get('name', 'N/A')} — <code>{master.get('telegram_id', 'N/A')}</code>",
        "",
        "👥 <b>Followers</b>",
    ]
    followers = config.get("followers", [])
    buttons = []
    if not followers:
        lines.append("  (none)")
    for f in followers:
        status = "✅" if f.get("enabled", True) else "❌"
        api = "🔑" if f.get("api_key") else "⚠️ no key"
        tid = f.get("telegram_id", 0)
        lines.append(
            f"  {status} {f.get('name', 'N/A')} — <code>{tid}</code> "
            f"({f.get('lot_multiplier', 1)}x) {api}"
        )
        buttons.append([InlineKeyboardButton(
            text=f"❌ Remove {f.get('name', tid)}",
            callback_data=f"admin:remove:{tid}",
        )])

    buttons.append([InlineKeyboardButton(text="➕ Add User", callback_data="admin:add")])
    buttons.append([InlineKeyboardButton(text="« Back", callback_data="admin:back")])

    await cb.message.edit_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await cb.answer()


@tg_router.callback_query(F.data.startswith("admin:remove:"))
async def cb_admin_remove(cb: CallbackQuery):
    if not _is_master(cb.from_user.id):
        await cb.answer("Admin only.", show_alert=True)
        return

    tid = int(cb.data.split(":")[2])
    config = _read_config()
    followers = config.get("followers", [])
    match = [f for f in followers if f.get("telegram_id") == tid]
    if not match:
        await cb.answer("User not found.", show_alert=True)
        return

    name = match[0].get("name", "Unknown")
    # Confirm removal
    await cb.message.edit_text(
        f"Remove <b>{name}</b> (<code>{tid}</code>)?",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Yes, remove", callback_data=f"admin:confirmremove:{tid}"),
             InlineKeyboardButton(text="❌ Cancel", callback_data="admin:users")],
        ]),
    )
    await cb.answer()


@tg_router.callback_query(F.data.startswith("admin:confirmremove:"))
async def cb_admin_confirm_remove(cb: CallbackQuery):
    if not _is_master(cb.from_user.id):
        await cb.answer("Admin only.", show_alert=True)
        return

    tid = int(cb.data.split(":")[2])
    config = _read_config()
    followers = config.get("followers", [])
    match = [f for f in followers if f.get("telegram_id") == tid]
    if not match:
        await cb.answer("User not found.", show_alert=True)
        return

    name = match[0].get("name", "Unknown")
    config["followers"] = [f for f in followers if f.get("telegram_id") != tid]
    _write_config(config)

    await cb.message.edit_text(f"✅ Removed <b>{name}</b> (<code>{tid}</code>)", parse_mode="HTML",
                                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                    [InlineKeyboardButton(text="« Back to Users", callback_data="admin:users")],
                                ]))
    await cb.answer()


@tg_router.callback_query(F.data == "admin:add")
async def cb_admin_add(cb: CallbackQuery, state: FSMContext):
    if not _is_master(cb.from_user.id):
        await cb.answer("Admin only.", show_alert=True)
        return

    await state.set_state(AddUserFSM.waiting_telegram_id)
    await cb.message.edit_text(
        "📝 Send the <b>Telegram ID</b> of the new follower:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin:cancel")],
        ]),
    )
    await cb.answer()


@tg_router.callback_query(F.data == "admin:cancel")
async def cb_admin_cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("⚙️ <b>Admin Panel</b>", parse_mode="HTML", reply_markup=admin_panel_kb())
    await cb.answer()


@tg_router.callback_query(F.data == "admin:back")
async def cb_admin_back(cb: CallbackQuery):
    await cb.message.edit_text("⚙️ <b>Admin Panel</b>", parse_mode="HTML", reply_markup=admin_panel_kb())
    await cb.answer()


@tg_router.message(AddUserFSM.waiting_telegram_id)
async def fsm_add_tid(message: Message, state: FSMContext):
    if not _is_master(message.from_user.id):
        return

    try:
        tid = int(message.text.strip())
    except ValueError:
        await message.answer("❌ That's not a valid Telegram ID. Send a number:")
        return

    config = _read_config()
    if config.get("master", {}).get("telegram_id") == tid:
        await message.answer("❌ That's the master account. Send a different ID:")
        return
    for f in config.get("followers", []):
        if f.get("telegram_id") == tid:
            await message.answer(f"❌ {f.get('name')} ({tid}) already exists. Send a different ID:")
            return

    await state.update_data(telegram_id=tid)
    await state.set_state(AddUserFSM.waiting_name)
    await message.answer("👤 Now send a <b>name</b> for this follower:", parse_mode="HTML")


@tg_router.message(AddUserFSM.waiting_name)
async def fsm_add_name(message: Message, state: FSMContext):
    if not _is_master(message.from_user.id):
        return

    name = message.text.strip()
    data = await state.get_data()
    tid = data["telegram_id"]

    config = _read_config()
    config.setdefault("followers", []).append({
        "telegram_id": tid,
        "name": name,
        "api_key": "",
        "secret": "",
        "use_totp": False,
        "totp_secret": "",
        "lot_mode": "same",
        "lot_multiplier": 1.0,
        "lot_fixed": 1,
        "enabled": True,
    })
    _write_config(config)
    await state.clear()

    await message.answer(
        f"✅ Added follower: <b>{name}</b> (<code>{tid}</code>)\n"
        f"Lot multiplier: 1x\n\n"
        f"They can now /login and set up their Groww API key from the dashboard Settings.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 View Users", callback_data="admin:users")],
        ]),
    )


# ── Groww Auth ───────────────────────────────────────────────────────────────

def authenticate(acc: AccountConfig) -> GrowwAPI:
    if acc.use_totp and acc.totp_secret:
        totp = pyotp.TOTP(acc.totp_secret).now()
        token = GrowwAPI.get_access_token(api_key=acc.api_key, totp=totp)
    else:
        token = GrowwAPI.get_access_token(api_key=acc.api_key, secret=acc.secret)
    log.info(f"Authenticated: {acc.name}")
    return GrowwAPI(token)


def load_config_and_auth():
    config = json.loads(CONFIG_PATH.read_text())
    state.poll_interval = config.get("poll_interval_seconds", 3)
    state.master_cfg = AccountConfig(**config["master"])
    state.follower_cfgs = [AccountConfig(**f) for f in config["followers"]]

    # Auth master
    try:
        state.master_client = authenticate(state.master_cfg)
    except Exception as e:
        log.error(f"Master auth failed: {e}")
        state.master_client = None

    # Auth followers
    state.follower_clients.clear()
    for fc in state.follower_cfgs:
        if not fc.enabled:
            continue
        try:
            client = authenticate(fc)
            state.follower_clients[fc.name] = (client, fc)
        except Exception as e:
            log.error(f"Follower {fc.name} auth failed: {e}")

    # Setup WebSocket feed for master order updates
    setup_master_feed()


# ── WebSocket Feed for instant order detection ────────────────────────────────

def setup_master_feed():
    """Connect to Groww WebSocket feed for instant master order updates.

    GrowwFeed/NatsClient creates its own asyncio event loop internally,
    so we must run the entire init + consume in a separate thread to avoid
    conflicting with uvicorn's running event loop.
    """
    if not state.master_client:
        log.warning("Master not authenticated, skipping feed setup")
        state.feed_mode = False
        return

    def _run_feed():
        """Runs in a separate thread — safe to create a new event loop."""
        try:
            feed = GrowwFeed(state.master_client)
            state.master_feed = feed
            feed.subscribe_fno_order_updates(
                on_data_received=on_master_order_update
            )

            # Subscribe to major Indian indices
            index_instruments = [
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTY"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "BANKNIFTY"},
                {"exchange": "BSE", "segment": "CASH", "exchange_token": "1"},  # SENSEX
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "FINNIFTY"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYJR"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTY100"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTY500"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYMIDCAP"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYMIDCAP150"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYSMALL"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYSMALLCAP250"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYAUTO"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYIT"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYPHARMA"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYFMCG"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYMETAL"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYREALTY"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYPVTBANK"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYPSUBANK"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYMEDIA"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYCDTY"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYMIDSELECT"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTYTOTALMCAP"},
                {"exchange": "NSE", "segment": "CASH", "exchange_token": "INDIAVIX"},
                {"exchange": "BSE", "segment": "CASH", "exchange_token": "14"},  # BANKEX
                {"exchange": "BSE", "segment": "CASH", "exchange_token": "2"},   # BSE100
                {"exchange": "BSE", "segment": "CASH", "exchange_token": "18"},  # BSEMIDCAP
                {"exchange": "BSE", "segment": "CASH", "exchange_token": "19"},  # BSESMLCAP
            ]
            feed.subscribe_index_value(
                instrument_list=index_instruments,
                on_data_received=on_index_update,
            )
            log.info(f"Subscribed to {len(index_instruments)} indices")

            log.info("WebSocket feed connected — instant order detection active")
            state.feed_mode = True
            feed.consume()  # blocking — stays in this thread
        except Exception as e:
            log.error(f"Feed thread error: {e}")
            state.master_feed = None
            state.feed_mode = False

    try:
        feed_thread = threading.Thread(target=_run_feed, daemon=True)
        feed_thread.start()
        # Give it a moment to connect before we check
        time.sleep(2)
        if state.feed_mode:
            log.info("Feed thread started successfully")
        else:
            log.warning("Feed did not connect, falling back to polling")
    except Exception as e:
        log.error(f"Feed setup failed, falling back to polling: {e}")
        state.master_feed = None
        state.feed_mode = False


# Index name mapping for display
INDEX_NAMES = {
    "NIFTY": "NIFTY 50", "BANKNIFTY": "NIFTY Bank", "1": "SENSEX",
    "FINNIFTY": "Fin Nifty", "NIFTYJR": "Nifty Next 50", "NIFTY100": "NIFTY 100",
    "NIFTY500": "NIFTY 500", "NIFTYMIDCAP": "Nifty Midcap 100",
    "NIFTYMIDCAP150": "Nifty Midcap 150", "NIFTYSMALL": "Nifty Smallcap 100",
    "NIFTYSMALLCAP250": "Nifty Smallcap 250", "NIFTYAUTO": "NIFTY Auto",
    "NIFTYIT": "NIFTY IT", "NIFTYPHARMA": "NIFTY Pharma", "NIFTYFMCG": "NIFTY FMCG",
    "NIFTYMETAL": "NIFTY Metal", "NIFTYREALTY": "NIFTY Realty",
    "NIFTYPVTBANK": "NIFTY Pvt Bank", "NIFTYPSUBANK": "NIFTY PSU Bank",
    "NIFTYMEDIA": "NIFTY Media", "NIFTYCDTY": "NIFTY Commodities",
    "NIFTYMIDSELECT": "Nifty Midcap Select", "NIFTYTOTALMCAP": "Nifty Total Market",
    "INDIAVIX": "India VIX", "14": "BSE Bankex", "2": "BSE 100",
    "18": "BSE Midcap", "19": "BSE Smallcap",
    "MIDCAP50": "Nifty Midcap 50",
}


def on_index_update(meta: dict):
    """Callback fired when index values update via feed.

    get_index_value() returns nested dict:
    {'NSE': {'CASH': {'NIFTY': {...data...}, 'BANKNIFTY': {...}}}, 'BSE': {'CASH': {'1': {...}}}}
    """
    if not state.master_feed:
        return
    try:
        raw = state.master_feed.get_index_value()
        if not raw:
            return
        for exchange, segments in raw.items():
            if not isinstance(segments, dict):
                continue
            for segment, instruments in segments.items():
                if not isinstance(instruments, dict):
                    continue
                for token, data in instruments.items():
                    if not data or not isinstance(data, dict):
                        continue
                    name = INDEX_NAMES.get(token, token)
                    state.index_data[token] = {
                        "name": name,
                        "exchange": exchange,
                        "token": token,
                        **data,
                    }
                    # Track LTP history for underlying price trend confirmation
                    ltp_val = float(data.get("value", data.get("ltp", data.get("lastTradedPrice", 0))) or 0)
                    if ltp_val > 0:
                        now_ts = time.time()
                        hist = state.ltp_history.setdefault(token, [])
                        hist.append((now_ts, ltp_val))
                        # Trim entries older than 600s
                        cutoff = now_ts - 600
                        while hist and hist[0][0] < cutoff:
                            hist.pop(0)
    except Exception as e:
        log.warning(f"Index update error: {e}")


def sync_cancel_to_followers(master_oid: str, symbol: str, segment: str):
    """Cancel all follower orders that were copied from a master order."""
    follower_orders = state.order_map.get(master_oid, {})
    if not follower_orders:
        state.add_log(
            f"Cancel sync: no mapped follower orders for {symbol}",
            symbol=symbol, status="warning",
            details=f"Master order {master_oid} — may have been a DRY_RUN",
        )
        return

    segment_map_keys = {"FNO": "SEGMENT_FNO", "COMMODITY": "SEGMENT_COMMODITY", "CASH": "SEGMENT_CASH"}

    for fname, foid in follower_orders.items():
        if foid == "DRY_RUN":
            state.add_log(
                f"Cancel skipped (was DRY_RUN): {fname}",
                symbol=symbol, follower=fname, status="info",
            )
            continue
        pair = state.follower_clients.get(fname)
        if not pair:
            continue
        client, fc = pair
        seg = getattr(client, segment_map_keys.get(segment, "SEGMENT_FNO"), client.SEGMENT_FNO)
        try:
            client.cancel_order(foid, segment=seg)
            state.add_log(
                f"Auto-cancelled on {fname}",
                symbol=symbol, follower=fname, status="warning",
                details=f"Follower order {foid} cancelled (master cancelled)",
            )
            log.info(f"CANCEL SYNC → {fname}: cancelled {foid} (master {master_oid})")
        except Exception as e:
            state.add_log(
                f"Cancel failed on {fname}",
                symbol=symbol, follower=fname, status="error",
                details=str(e),
            )
            log.error(f"CANCEL SYNC FAILED → {fname}: {foid} | {e}")


def sync_modify_to_followers(master_oid: str, order_data: dict, symbol: str):
    """Modify all follower orders to match master's updated price/qty."""
    follower_orders = state.order_map.get(master_oid, {})
    if not follower_orders:
        state.add_log(
            f"Modify sync: no mapped follower orders for {symbol}",
            symbol=symbol, status="warning",
        )
        return

    order = normalize_feed_order(order_data)
    order_type = order.get("order_type", "LIMIT").upper()
    segment_str = order.get("segment", "FNO").upper()

    for fname, foid in follower_orders.items():
        if foid == "DRY_RUN":
            state.add_log(
                f"Modify skipped (was DRY_RUN): {fname}",
                symbol=symbol, follower=fname, status="info",
            )
            continue
        pair = state.follower_clients.get(fname)
        if not pair:
            continue
        client, fc = pair

        new_qty = int(order.get("quantity", 0))
        copied_qty = calc_follower_qty(fc, new_qty)

        type_map = {
            "MARKET": client.ORDER_TYPE_MARKET, "LIMIT": client.ORDER_TYPE_LIMIT,
            "SL": client.ORDER_TYPE_STOP_LOSS, "SL-M": client.ORDER_TYPE_STOP_LOSS_MARKET,
            "STOP_LOSS": client.ORDER_TYPE_STOP_LOSS, "STOP_LOSS_MARKET": client.ORDER_TYPE_STOP_LOSS_MARKET,
        }
        segment_map = {
            "FNO": client.SEGMENT_FNO, "COMMODITY": client.SEGMENT_COMMODITY, "CASH": client.SEGMENT_CASH,
        }

        kwargs = dict(
            groww_order_id=foid,
            order_type=type_map.get(order_type, client.ORDER_TYPE_LIMIT),
            segment=segment_map.get(segment_str, client.SEGMENT_FNO),
            quantity=copied_qty,
        )

        price = order.get("price")
        if price and float(price) > 0:
            kwargs["price"] = float(price)
        trigger = order.get("trigger_price")
        if trigger and float(trigger) > 0:
            kwargs["trigger_price"] = float(trigger)

        if state.dry_run:
            log.info(f"DRY RUN MODIFY → {fname}: {foid} qty={copied_qty} price={price}")
            state.add_log(
                f"Modify (DRY RUN) on {fname}",
                symbol=symbol, follower=fname, status="info",
                details=f"qty={copied_qty}, price={price}",
            )
            continue

        try:
            client.modify_order(**kwargs)
            state.add_log(
                f"Auto-modified on {fname}",
                symbol=symbol, follower=fname, status="success",
                details=f"qty={copied_qty}, price={price}",
            )
            log.info(f"MODIFY SYNC → {fname}: modified {foid} qty={copied_qty} price={price}")
        except Exception as e:
            state.add_log(
                f"Modify failed on {fname}",
                symbol=symbol, follower=fname, status="error",
                details=str(e),
            )
            log.error(f"MODIFY SYNC FAILED → {fname}: {foid} | {e}")


def on_master_order_update(meta: dict):
    """Callback fired instantly when master places/modifies/cancels an order.

    Full lifecycle sync:
    - ACKED/OPEN/PENDING → place new order on followers
    - CANCELLED/CANCELLATION_REQUESTED → cancel on followers
    - Already-copied order with changed price/qty → modify on followers
    - EXECUTED → log only (order already placed, exchange filled it)
    - FAILED/REJECTED → log warning
    """
    if not state.running:
        return

    try:
        order_data = state.master_feed.get_fno_order_update()
        if not order_data:
            return

        log.info(f"FEED: Order update received: {order_data}")

        oid = order_data.get("growwOrderId") or order_data.get("groww_order_id")
        if not oid:
            return

        symbol = (order_data.get("contractId") or order_data.get("trading_symbol")
                  or order_data.get("tradingSymbol") or "unknown")
        status = (order_data.get("order_status") or order_data.get("orderStatus") or "").upper()
        segment = (order_data.get("segment") or "FNO").upper()

        # ── CANCEL SYNC ──────────────────────────────────────────────────
        if status in ("CANCELLED", "CANCELLATION_REQUESTED"):
            state.add_log(
                f"Master {status}: {symbol}",
                symbol=symbol, status="warning",
                details="Syncing cancel to followers...",
            )
            sync_cancel_to_followers(oid, symbol, segment)
            return

        # ── MODIFICATION SYNC ────────────────────────────────────────────
        # If we already copied this order, check if price/qty changed
        if oid in state.copier_state.copied_order_ids:
            if oid in state.order_map:
                order = normalize_feed_order(order_data)
                prev = state._last_order_snapshot.get(oid, {})
                new_qty = order.get("quantity", 0)
                new_price = order.get("price", 0)
                old_qty = prev.get("quantity", 0)
                old_price = prev.get("price", 0)

                if new_qty != old_qty or abs(new_price - old_price) > 0.001:
                    state.add_log(
                        f"Master MODIFIED: {symbol}",
                        symbol=symbol, status="info",
                        details=f"qty: {old_qty}→{new_qty}, price: {old_price}→{new_price}",
                    )
                    sync_modify_to_followers(oid, order_data, symbol)
                    # Update snapshot
                    state._last_order_snapshot[oid] = {"quantity": new_qty, "price": new_price}
            return

        # ── NEW ORDER ────────────────────────────────────────────────────
        if status not in ("ACKED", "EXECUTED", "OPEN", "PENDING"):
            remark = order_data.get("remark") or order_data.get("statusMessage") or ""
            state.add_log(
                f"Order {status}: {symbol}",
                symbol=symbol,
                status="warning" if status in ("FAILED", "REJECTED") else "info",
                details=remark,
            )
            return

        # Normalize field names (feed may use camelCase)
        order = normalize_feed_order(order_data)
        symbol = order.get("trading_symbol", "???")
        txn = order.get("transaction_type", "")
        qty = order.get("quantity", 0)
        price = order.get("price", 0)

        state.add_log(
            f"INSTANT: {txn} {qty}x {symbol} [{status}]",
            symbol=symbol, status="info",
            details="via WebSocket feed",
        )

        # Copy to all followers and track order ID mapping
        follower_map = {}
        for name, (client, fc) in state.follower_clients.items():
            resp = copy_order_to_follower(client, fc, order)
            if resp:
                state.total_copied += 1
                follower_oid = resp.get("groww_order_id", "DRY_RUN")
                follower_map[name] = follower_oid
                state.add_log(
                    f"Copied to {name}",
                    symbol=symbol, follower=name, status="success",
                    details=f"Order ID: {follower_oid}",
                )
            else:
                state.total_failed += 1
                state.add_log(
                    f"Failed copy to {name}",
                    symbol=symbol, follower=name, status="error",
                )

        # Store mapping for cancel/modify sync
        if follower_map:
            state.order_map[oid] = follower_map

        # Store snapshot for modification detection
        state._last_order_snapshot[oid] = {"quantity": qty, "price": price}

        state.copier_state.copied_order_ids.add(oid)
        state.copier_state.save()

    except Exception as e:
        log.error(f"Feed order handler error: {e}")
        state.add_log(f"Feed error: {e}", status="error")


def normalize_feed_order(data: dict) -> dict:
    """Normalize camelCase feed data to snake_case matching order format.

    Real feed format (from testing):
    {'qty': 20, 'price': '5', 'growwOrderId': 'GLTFO...', 'orderStatus': 'ACKED',
     'duration': 'DAY', 'segment': 'FNO', 'product': 'NRML', 'orderType': 'LIMIT',
     'contractId': 'SENSEX26FEB82000PE', 'triggerPrice': '0', 'filledQty': 0,
     'remainingQty': 0, 'avgFillPrice': '0', 'exchangeOrderId': '', 'exchange': 'BSE',
     'remark': '', 'transactionType': 'BUY'}
    """
    # Feed sends prices in paise as strings (e.g. '15000' = ₹150.00)
    price_raw = float(data.get("price") or data.get("avgFillPrice") or 0) / 100
    trigger_raw = float(data.get("triggerPrice") or data.get("trigger_price") or 0) / 100
    return {
        "groww_order_id": data.get("growwOrderId") or data.get("groww_order_id"),
        "trading_symbol": data.get("contractId") or data.get("trading_symbol") or data.get("tradingSymbol"),
        "order_status": data.get("orderStatus") or data.get("order_status"),
        "quantity": data.get("qty") or data.get("quantity") or 0,
        "price": price_raw,
        "trigger_price": trigger_raw,
        "order_type": data.get("orderType") or data.get("order_type") or "MARKET",
        "transaction_type": data.get("transactionType") or data.get("transaction_type") or "BUY",
        "exchange": data.get("exchange") or "NSE",
        "segment": data.get("segment") or "FNO",
        "product": data.get("product") or data.get("productType") or "NRML",
    }


# ── Copier logic (async) ─────────────────────────────────────────────────────

def unwrap_orders(raw) -> list[dict]:
    """Unwrap {'order_list': [...]} or return list as-is."""
    if isinstance(raw, dict):
        return raw.get("order_list", [])
    return raw if isinstance(raw, list) else []


def unwrap_positions(raw) -> list[dict]:
    """Unwrap {'positions': [...]} or return list as-is."""
    if isinstance(raw, dict):
        return raw.get("positions", [])
    return raw if isinstance(raw, list) else []


def fetch_master_orders(client: GrowwAPI) -> list[dict]:
    try:
        raw = client.get_order_list()
        orders = unwrap_orders(raw)
        return [o for o in orders if o.get("segment", "").upper() in ("FNO", "COMMODITY")]
    except Exception as e:
        log.error(f"Failed to fetch master orders: {e}")
        return []


def calc_follower_qty(follower: AccountConfig, master_qty: int) -> int:
    if follower.lot_mode == "fixed":
        return max(1, follower.lot_fixed)
    elif follower.lot_mode == "multiplier":
        return max(1, math.floor(master_qty * follower.lot_multiplier))
    else:  # "same"
        return max(1, master_qty)


def copy_order_to_follower(follower_client: GrowwAPI, follower: AccountConfig, order: dict):
    original_qty = int(order.get("quantity", 0))
    copied_qty = calc_follower_qty(follower, original_qty)
    order_type = order.get("order_type", "LIMIT").upper()
    txn_type = order.get("transaction_type", "BUY").upper()

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

    price = order.get("price")
    if price and float(price) > 0:
        kwargs["price"] = float(price)
    trigger = order.get("trigger_price")
    if trigger and float(trigger) > 0:
        kwargs["trigger_price"] = float(trigger)

    if state.dry_run:
        log.info(
            f"DRY RUN → {follower.name}: {txn_type} {copied_qty}x "
            f"{order['trading_symbol']} @ {price or 'MKT'} "
            f"[{exchange_str}/{segment_str}/{product_str}]"
        )
        return {"groww_order_id": "DRY_RUN", "dry_run": True}

    try:
        resp = follower_client.place_order(**kwargs)
        log.info(f"COPIED → {follower.name}: {txn_type} {copied_qty}x {order['trading_symbol']}")
        return resp
    except Exception as e:
        log.error(f"FAILED → {follower.name}: {order['trading_symbol']} | {e}")
        return None


async def copier_loop():
    if state.feed_mode:
        state.add_log("Copier started (WebSocket feed — instant)", status="success",
                       details="Orders copied instantly via feed callback")
        # Feed callback handles copying; loop just keeps alive + periodic sync
        while state.running:
            # Periodic sync: catch any orders the feed might have missed
            await poll_and_copy()
            await asyncio.sleep(30)  # Light polling every 30s as safety net
    else:
        state.add_log("Copier started (polling mode)", status="success",
                       details=f"Polling every {state.poll_interval}s")
        while state.running:
            await poll_and_copy()
            await asyncio.sleep(state.poll_interval)

    state.add_log("Copier stopped", status="warning")


async def poll_and_copy():
    """Poll master orders and copy new ones (used as fallback or periodic sync)."""
    if not state.master_client:
        return

    orders = await asyncio.to_thread(fetch_master_orders, state.master_client)
    new_orders = [
        o for o in orders
        if o.get("groww_order_id") and o["groww_order_id"] not in state.copier_state.copied_order_ids
        and o.get("order_status", "").upper() in ("ACKED", "EXECUTED", "OPEN", "PENDING")
    ]

    for order in new_orders:
        oid = order["groww_order_id"]
        symbol = order.get("trading_symbol", "???")
        txn = order.get("transaction_type", "")
        qty = order.get("quantity", 0)

        state.add_log(
            f"New master order: {txn} {qty}x {symbol}",
            symbol=symbol, status="info",
            details="via polling sync" if state.feed_mode else "via polling",
        )

        for name, (client, fc) in state.follower_clients.items():
            resp = await asyncio.to_thread(copy_order_to_follower, client, fc, order)
            if resp:
                state.total_copied += 1
                state.add_log(
                    f"Copied to {name}",
                    symbol=symbol, follower=name, status="success",
                    details=f"Order ID: {resp.get('groww_order_id', 'N/A')}",
                )
            else:
                state.total_failed += 1
                state.add_log(
                    f"Failed copy to {name}",
                    symbol=symbol, follower=name, status="error",
                )

        state.copier_state.copied_order_ids.add(oid)
        state.copier_state.save()


# ── OI Analytics Engine ──────────────────────────────────────────────────────

def _get_oi_config() -> dict:
    config = _read_config()
    return config.get("oi_analytics", {
        "enabled": False, "instruments": [],
        "poll_interval_seconds": 180, "unusual_oi_threshold": 2.0,
    })


def fetch_option_chain(client: GrowwAPI, underlying: str, exchange: str = "NSE") -> OISnapshot | None:
    """Fetch option chain for underlying, pick nearest expiry, return OISnapshot."""
    try:
        expiries = client.get_expiries(exchange, underlying)
        if not expiries:
            log.warning(f"OI: No expiries for {underlying}")
            return None

        # expiries is a list of date strings; pick nearest future
        from datetime import date
        today = date.today()
        today_str = today.strftime("%Y-%m-%d")

        # Filter to future/current expiries
        valid = [e for e in expiries if e >= today_str]
        if not valid:
            valid = expiries[-1:]  # fallback to last

        # On expiry day (Thursday), prefer next week if available
        chosen = valid[0]
        if chosen == today_str and len(valid) > 1:
            chosen = valid[1]

        chain = client.get_option_chain(exchange, underlying, chosen)
        if not chain:
            log.warning(f"OI: Empty chain for {underlying} exp={chosen}")
            return None

        strikes = {}
        total_ce_oi = 0
        total_pe_oi = 0
        ltp = 0.0

        # Parse option chain — format varies by SDK version
        # Typical: list of dicts with strike_price, call/put sub-dicts
        if isinstance(chain, list):
            for row in chain:
                strike = float(row.get("strike_price", row.get("strikePrice", 0)))
                if strike == 0:
                    continue
                call = row.get("call", row.get("CE", {})) or {}
                put = row.get("put", row.get("PE", {})) or {}
                ce_oi = int(call.get("open_interest", call.get("openInterest", call.get("oi", 0))) or 0)
                pe_oi = int(put.get("open_interest", put.get("openInterest", put.get("oi", 0))) or 0)
                if ce_oi == 0 and pe_oi == 0:
                    continue
                strikes[strike] = {
                    "ce_oi": ce_oi,
                    "pe_oi": pe_oi,
                    "ce_volume": int(call.get("volume", call.get("tradedVolume", 0)) or 0),
                    "pe_volume": int(put.get("volume", put.get("tradedVolume", 0)) or 0),
                    "ce_ltp": float(call.get("last_price", call.get("lastPrice", call.get("ltp", 0))) or 0),
                    "pe_ltp": float(put.get("last_price", put.get("lastPrice", put.get("ltp", 0))) or 0),
                    "ce_iv": float(call.get("implied_volatility", call.get("impliedVolatility", call.get("iv", 0))) or 0),
                    "pe_iv": float(put.get("implied_volatility", put.get("impliedVolatility", put.get("iv", 0))) or 0),
                }
                total_ce_oi += ce_oi
                total_pe_oi += pe_oi
                # Try to get underlying LTP from the chain
                if not ltp:
                    ltp = float(row.get("underlying_value", row.get("underlyingValue", 0)) or 0)
        elif isinstance(chain, dict):
            # Some SDKs return dict with 'data' or 'records' key
            records = chain.get("data", chain.get("records", chain.get("options", [])))
            if isinstance(records, list):
                for row in records:
                    strike = float(row.get("strike_price", row.get("strikePrice", 0)))
                    if strike == 0:
                        continue
                    call = row.get("call", row.get("CE", {})) or {}
                    put = row.get("put", row.get("PE", {})) or {}
                    ce_oi = int(call.get("open_interest", call.get("openInterest", call.get("oi", 0))) or 0)
                    pe_oi = int(put.get("open_interest", put.get("openInterest", put.get("oi", 0))) or 0)
                    if ce_oi == 0 and pe_oi == 0:
                        continue
                    strikes[strike] = {
                        "ce_oi": ce_oi, "pe_oi": pe_oi,
                        "ce_volume": int(call.get("volume", call.get("tradedVolume", 0)) or 0),
                        "pe_volume": int(put.get("volume", put.get("tradedVolume", 0)) or 0),
                        "ce_ltp": float(call.get("last_price", call.get("lastPrice", call.get("ltp", 0))) or 0),
                        "pe_ltp": float(put.get("last_price", put.get("lastPrice", put.get("ltp", 0))) or 0),
                        "ce_iv": float(call.get("implied_volatility", call.get("impliedVolatility", call.get("iv", 0))) or 0),
                        "pe_iv": float(put.get("implied_volatility", put.get("impliedVolatility", put.get("iv", 0))) or 0),
                    }
                    total_ce_oi += ce_oi
                    total_pe_oi += pe_oi
                    if not ltp:
                        ltp = float(row.get("underlying_value", row.get("underlyingValue", 0)) or 0)

        if not strikes:
            log.warning(f"OI: No valid strikes for {underlying}")
            return None

        # Fallback: get LTP from feed/index data
        if not ltp:
            idx = state.index_data.get(underlying, {})
            ltp = float(idx.get("ltp", idx.get("lastTradedPrice", idx.get("value", 0))) or 0)

        return OISnapshot(
            underlying=underlying, expiry=chosen,
            timestamp=datetime.now(timezone.utc).isoformat(),
            underlying_ltp=ltp, strikes=strikes,
            total_ce_oi=total_ce_oi, total_pe_oi=total_pe_oi,
        )
    except Exception as e:
        log.error(f"OI: fetch_option_chain({underlying}) failed: {e}")
        return None


def calculate_pcr(snapshot: OISnapshot) -> float:
    if snapshot.total_ce_oi == 0:
        return 0.0
    return round(snapshot.total_pe_oi / snapshot.total_ce_oi, 3)


def calculate_max_pain(snapshot: OISnapshot) -> float:
    """Find strike where total option writer loss is minimized."""
    strikes = sorted(snapshot.strikes.keys())
    if not strikes:
        return 0.0
    min_loss = float('inf')
    max_pain_strike = strikes[0]
    for test_strike in strikes:
        total_loss = 0.0
        for s, data in snapshot.strikes.items():
            # CE writers lose if expiry above strike
            if test_strike > s:
                total_loss += (test_strike - s) * data["ce_oi"]
            # PE writers lose if expiry below strike
            if test_strike < s:
                total_loss += (s - test_strike) * data["pe_oi"]
        if total_loss < min_loss:
            min_loss = total_loss
            max_pain_strike = test_strike
    return max_pain_strike


def detect_buildup(prev: OISnapshot | None, curr: OISnapshot) -> list[dict]:
    """Compare OI and LTP changes to detect buildup patterns near ATM."""
    if not prev:
        return []
    signals = []
    ltp = curr.underlying_ltp
    all_strikes = sorted(curr.strikes.keys())
    if not all_strikes or ltp == 0:
        return []

    # Find ATM strike
    atm = min(all_strikes, key=lambda s: abs(s - ltp))
    atm_idx = all_strikes.index(atm)
    nearby = all_strikes[max(0, atm_idx - 10):atm_idx + 11]

    for strike in nearby:
        curr_data = curr.strikes.get(strike, {})
        prev_data = prev.strikes.get(strike, {})
        if not curr_data or not prev_data:
            continue

        for opt_type, oi_key, ltp_key in [("CE", "ce_oi", "ce_ltp"), ("PE", "pe_oi", "pe_ltp")]:
            curr_oi = curr_data.get(oi_key, 0)
            prev_oi = prev_data.get(oi_key, 0)
            curr_ltp_val = curr_data.get(ltp_key, 0)
            prev_ltp_val = prev_data.get(ltp_key, 0)
            oi_change = curr_oi - prev_oi
            ltp_change = curr_ltp_val - prev_ltp_val

            if oi_change == 0:
                continue

            if ltp_change > 0 and oi_change > 0:
                signal = "Long Buildup"
            elif ltp_change < 0 and oi_change > 0:
                signal = "Short Buildup"
            elif ltp_change < 0 and oi_change < 0:
                signal = "Long Unwinding"
            elif ltp_change > 0 and oi_change < 0:
                signal = "Short Covering"
            else:
                continue

            oi_pct = round((oi_change / prev_oi * 100), 1) if prev_oi else 0
            signals.append({
                "strike": strike,
                "type": opt_type,
                "signal": signal,
                "oi_change": oi_change,
                "oi_pct": oi_pct,
                "ltp": curr_ltp_val,
                "ltp_change": round(ltp_change, 2),
                "current_oi": curr_oi,
            })

    # Sort by absolute OI change, return top 20
    signals.sort(key=lambda x: abs(x["oi_change"]), reverse=True)
    return signals[:20]


def detect_unusual_activity(prev: OISnapshot | None, curr: OISnapshot, threshold: float = 2.0) -> list[dict]:
    """Flag strikes where OI change > threshold * avg OI change."""
    if not prev:
        return []
    changes = []
    for strike in curr.strikes:
        curr_data = curr.strikes[strike]
        prev_data = prev.strikes.get(strike, {})
        if not prev_data:
            continue
        for opt_type, oi_key in [("CE", "ce_oi"), ("PE", "pe_oi")]:
            change = abs(curr_data.get(oi_key, 0) - prev_data.get(oi_key, 0))
            if change > 0:
                changes.append((strike, opt_type, change, curr_data, prev_data))

    if not changes:
        return []

    avg_change = sum(c[2] for c in changes) / len(changes)
    if avg_change == 0:
        return []

    unusual = []
    for strike, opt_type, change, curr_data, prev_data in changes:
        if change > threshold * avg_change:
            oi_key = "ce_oi" if opt_type == "CE" else "pe_oi"
            ltp_key = "ce_ltp" if opt_type == "CE" else "pe_ltp"
            prev_oi = prev_data.get(oi_key, 0)
            curr_oi = curr_data.get(oi_key, 0)
            direction = "Added" if curr_oi > prev_oi else "Reduced"
            unusual.append({
                "strike": strike,
                "type": opt_type,
                "oi_change": curr_oi - prev_oi,
                "oi_pct": round(((curr_oi - prev_oi) / prev_oi * 100), 1) if prev_oi else 0,
                "current_oi": curr_oi,
                "ltp": curr_data.get(ltp_key, 0),
                "direction": direction,
                "ratio": round(change / avg_change, 1),
            })

    unusual.sort(key=lambda x: abs(x["oi_change"]), reverse=True)
    return unusual[:10]


def generate_suggestions(analysis: OIAnalysis) -> list[dict]:
    """Rule-based trade suggestion engine."""
    suggestions = []
    ltp = analysis.underlying_ltp
    pcr = analysis.pcr
    max_pain = analysis.max_pain
    support = analysis.support_level
    resistance = analysis.resistance_level

    if ltp == 0:
        return suggestions

    # PCR-based sentiment
    if pcr > 1.2 and max_pain > ltp:
        suggestions.append({
            "action": f"BUY {analysis.underlying} CE near {support:.0f}",
            "rationale": f"PCR {pcr:.2f} (bullish) + Max Pain {max_pain:.0f} above LTP {ltp:.0f}. Put writers supporting the market.",
            "confidence": "High" if pcr > 1.5 else "Medium",
            "direction": "Bullish",
        })
    elif pcr < 0.7 and max_pain < ltp:
        suggestions.append({
            "action": f"BUY {analysis.underlying} PE near {resistance:.0f}",
            "rationale": f"PCR {pcr:.2f} (bearish) + Max Pain {max_pain:.0f} below LTP {ltp:.0f}. Call writers capping upside.",
            "confidence": "High" if pcr < 0.5 else "Medium",
            "direction": "Bearish",
        })
    elif 0.7 <= pcr <= 1.2:
        suggestions.append({
            "action": f"Sell {analysis.underlying} Straddle/Strangle near ATM",
            "rationale": f"PCR {pcr:.2f} (neutral range). Max Pain at {max_pain:.0f}, market may stay range-bound between {support:.0f}-{resistance:.0f}.",
            "confidence": "Medium",
            "direction": "Neutral",
        })

    # Buildup-based suggestions
    for sig in analysis.buildup_signals[:5]:
        if sig["signal"] == "Short Buildup" and sig["type"] == "CE":
            suggestions.append({
                "action": f"Resistance at {sig['strike']:.0f} ({sig['type']})",
                "rationale": f"Heavy call writing ({sig['oi_change']:+,} OI) at {sig['strike']:.0f} CE — resistance forming.",
                "confidence": "Medium",
                "direction": "Bearish",
            })
            break
    for sig in analysis.buildup_signals[:5]:
        if sig["signal"] == "Short Buildup" and sig["type"] == "PE":
            suggestions.append({
                "action": f"Support at {sig['strike']:.0f} ({sig['type']})",
                "rationale": f"Heavy put writing ({sig['oi_change']:+,} OI) at {sig['strike']:.0f} PE — support forming.",
                "confidence": "Medium",
                "direction": "Bullish",
            })
            break

    return suggestions[:5]


def find_support_resistance(snapshot: OISnapshot) -> tuple[float, float]:
    """Find support (highest PE OI below ATM) and resistance (highest CE OI above ATM)."""
    ltp = snapshot.underlying_ltp
    if not ltp or not snapshot.strikes:
        return 0.0, 0.0

    support = 0.0
    resistance = 0.0
    max_pe_oi = 0
    max_ce_oi = 0

    for strike, data in snapshot.strikes.items():
        if strike < ltp and data["pe_oi"] > max_pe_oi:
            max_pe_oi = data["pe_oi"]
            support = strike
        if strike > ltp and data["ce_oi"] > max_ce_oi:
            max_ce_oi = data["ce_oi"]
            resistance = strike

    return support, resistance


def run_oi_analysis(underlying: str, snapshot: OISnapshot) -> OIAnalysis:
    """Orchestrate full OI analysis for an underlying."""
    prev_snapshots = state.oi_snapshots.get(underlying, [])
    prev = prev_snapshots[-1] if len(prev_snapshots) >= 1 and prev_snapshots[-1] is not snapshot else (
        prev_snapshots[-2] if len(prev_snapshots) >= 2 else None
    )

    pcr = calculate_pcr(snapshot)
    max_pain = calculate_max_pain(snapshot)
    support, resistance = find_support_resistance(snapshot)
    buildup = detect_buildup(prev, snapshot)
    unusual = detect_unusual_activity(prev, snapshot, _get_oi_config().get("unusual_oi_threshold", 2.0))

    # Determine sentiment
    if pcr > 1.2:
        sentiment = "Bullish"
    elif pcr < 0.7:
        sentiment = "Bearish"
    else:
        sentiment = "Neutral"

    analysis = OIAnalysis(
        underlying=underlying, expiry=snapshot.expiry,
        timestamp=snapshot.timestamp, underlying_ltp=snapshot.underlying_ltp,
        pcr=pcr, max_pain=max_pain, sentiment=sentiment,
        support_level=support, resistance_level=resistance,
        buildup_signals=buildup, unusual_activity=unusual,
    )
    analysis.trade_suggestions = generate_suggestions(analysis)
    return analysis


async def oi_polling_loop():
    """Background task: poll option chains and run analysis."""
    log.info("OI Analytics polling loop started")
    while state.oi_running:
        oi_cfg = _get_oi_config()
        instruments = oi_cfg.get("instruments", [])
        if not instruments or not state.master_client:
            await asyncio.sleep(10)
            continue

        for underlying in instruments:
            if not state.oi_running:
                break
            try:
                snapshot = await asyncio.to_thread(fetch_option_chain, state.master_client, underlying)
                if snapshot:
                    # Rotate snapshots: keep [prev, current]
                    if underlying not in state.oi_snapshots:
                        state.oi_snapshots[underlying] = []
                    state.oi_snapshots[underlying].append(snapshot)
                    if len(state.oi_snapshots[underlying]) > 2:
                        state.oi_snapshots[underlying] = state.oi_snapshots[underlying][-2:]

                    analysis = run_oi_analysis(underlying, snapshot)
                    state.oi_analysis[underlying] = analysis
                    log.info(f"OI: {underlying} PCR={analysis.pcr:.2f} MaxPain={analysis.max_pain:.0f} LTP={analysis.underlying_ltp:.0f}")
                else:
                    log.warning(f"OI: No snapshot for {underlying}")
            except Exception as e:
                log.error(f"OI: Error analyzing {underlying}: {e}")

            await asyncio.sleep(2)  # gap between instruments

        state.oi_last_poll = datetime.now(timezone.utc).isoformat()
        # Broadcast full OI analysis to connected clients
        oi_payload = {}
        for underlying, analysis in state.oi_analysis.items():
            has_prev = underlying in state.oi_snapshots and len(state.oi_snapshots[underlying]) >= 2
            oi_payload[underlying] = {**asdict(analysis), "has_prev_data": has_prev}
        await state.broadcast({"type": "oi_update", "data": oi_payload})

        interval = oi_cfg.get("poll_interval_seconds", 180)
        await asyncio.sleep(interval)

    log.info("OI Analytics polling loop stopped")


# ── Greeks Scanner & Signal Engine ────────────────────────────────────────────

# Underlying configs — Greeks API only works for SENSEX currently
# NIFTY/BANKNIFTY return null Greeks from Groww API
GREEKS_UNDERLYINGS = {
    "SENSEX": {"exchange": "BSE", "index_token": "1", "step": 100, "lot": 20},
}

IST = timezone(timedelta(hours=5, minutes=30))


def _get_nearest_weekly_expiry(underlying: str) -> str:
    """Return nearest weekly expiry date as YYYY-MM-DD.

    All index weekly options expire on Thursday.
    If today is Thursday and past 15:30 IST, use next Thursday.
    """
    now_ist = datetime.now(IST)
    today = now_ist.date()

    expiry_weekday = 3  # Thursday for all

    days_ahead = (expiry_weekday - today.weekday()) % 7
    if days_ahead == 0:
        # Today is expiry — use today if before 15:30, else next week
        if now_ist.hour >= 16:
            days_ahead = 7
    expiry = today + timedelta(days=days_ahead)
    return expiry.strftime("%Y-%m-%d")


def _build_trading_symbol(underlying: str, expiry_str: str, strike: int, opt_type: str) -> str:
    """Build Groww trading symbol like SENSEX26FEB82400CE.

    Format: {UNDERLYING}{DD}{MMM}{STRIKE}{CE/PE}
    where DD is day-of-month (no leading zero for single digits), MMM is month abbreviation.
    Example: SENSEX26FEB82400CE = SENSEX, 26th FEB, strike 82400, CE
    """
    exp = datetime.strptime(expiry_str, "%Y-%m-%d").date()
    dd = str(exp.day)                 # "26" (no leading zero: "5" not "05")
    mmm = exp.strftime("%b").upper()  # "FEB"
    return f"{underlying}{dd}{mmm}{strike}{opt_type}"


def _get_price_trend(index_token: str, minutes: int = 5) -> tuple[str, float]:
    """Get underlying price trend from LTP history. Returns (direction, change_pct)."""
    hist = state.ltp_history.get(index_token, [])
    if len(hist) < 2:
        return "FLAT", 0.0
    now_ts = time.time()
    cutoff = now_ts - minutes * 60
    recent = [(ts, ltp) for ts, ltp in hist if ts >= cutoff]
    if len(recent) < 2:
        return "FLAT", 0.0
    first_ltp = recent[0][1]
    last_ltp = recent[-1][1]
    if first_ltp == 0:
        return "FLAT", 0.0
    change_pct = (last_ltp - first_ltp) / first_ltp * 100
    if change_pct > 0.05:
        return "UP", round(change_pct, 3)
    elif change_pct < -0.05:
        return "DOWN", round(change_pct, 3)
    return "FLAT", round(change_pct, 3)


def _check_delta_trend(strike: int, history: list[dict], key: str) -> tuple[bool, int]:
    """Check if delta is trending consistently across scans.
    Returns (is_trending_3plus, consecutive_count)."""
    if len(history) < 3:
        return False, 0
    values = []
    for snap in history:
        sd = snap.get(strike, {})
        if sd:
            values.append(sd.get(key, 0))
    if len(values) < 3:
        return False, 0
    # Count consecutive increases (for CE) or consecutive magnitude increases (for PE)
    consec = 1
    for i in range(1, len(values)):
        if key == "pe_delta":
            # PE delta is negative, increasing magnitude means more negative
            if abs(values[i]) > abs(values[i - 1]) + 0.005:
                consec += 1
            else:
                consec = 1
        else:
            if values[i] > values[i - 1] + 0.005:
                consec += 1
            else:
                consec = 1
    return consec >= 3, consec


def _check_oi_consistency(strike: int, history: list[dict], key: str) -> bool:
    """Check if OI has been building for 2+ of last 3 scans."""
    if len(history) < 3:
        return False
    recent = history[-3:]
    building_count = 0
    for i in range(1, len(recent)):
        prev_oi = recent[i - 1].get(strike, {}).get(key, 0)
        curr_oi = recent[i].get(strike, {}).get(key, 0)
        if curr_oi > prev_oi > 0:
            building_count += 1
    return building_count >= 2


def _oi_velocity(strike: int, history: list[dict], key: str) -> tuple[float, bool]:
    """Calculate OI change velocity. Returns (change_pct, is_spike_20pct)."""
    if len(history) < 2:
        return 0.0, False
    prev_oi = history[-2].get(strike, {}).get(key, 0)
    curr_oi = history[-1].get(strike, {}).get(key, 0)
    if prev_oi <= 0:
        return 0.0, False
    change_pct = (curr_oi - prev_oi) / prev_oi * 100
    return round(change_pct, 1), change_pct > 20


def _volume_oi_ratio(strike_data: dict, pfx: str) -> float:
    """Calculate volume/OI ratio for freshness detection."""
    oi = strike_data.get(f"{pfx}oi", 0)
    vol = strike_data.get(f"{pfx}volume", 0)
    if oi <= 0:
        return 0.0
    return round(vol / oi, 3)


def _snapshot_to_oi(underlying: str, snapshot: dict, underlying_ltp: float) -> OISnapshot:
    """Convert Greeks scanner snapshot dict to OISnapshot for max pain / S&R calculations."""
    strikes = {}
    total_ce_oi = 0
    total_pe_oi = 0
    for strike, data in snapshot.items():
        strikes[strike] = {
            "ce_oi": data.get("ce_oi", 0),
            "pe_oi": data.get("pe_oi", 0),
            "ce_volume": data.get("ce_volume", 0),
            "pe_volume": data.get("pe_volume", 0),
            "ce_ltp": data.get("ce_ltp", 0),
            "pe_ltp": data.get("pe_ltp", 0),
            "ce_iv": data.get("ce_iv", 0),
            "pe_iv": data.get("pe_iv", 0),
        }
        total_ce_oi += data.get("ce_oi", 0)
        total_pe_oi += data.get("pe_oi", 0)
    return OISnapshot(
        underlying=underlying,
        expiry="",
        timestamp=datetime.now(timezone.utc).isoformat(),
        underlying_ltp=underlying_ltp,
        strikes=strikes,
        total_ce_oi=total_ce_oi,
        total_pe_oi=total_pe_oi,
    )


def _get_atm_strike(underlying: str, cfg: dict) -> int | None:
    """Get ATM strike from live index feed data."""
    idx_data = state.index_data.get(cfg["index_token"], {})
    # BSE indices use "value", NSE uses "ltp" or "lastTradedPrice"
    ltp = float(idx_data.get("value", idx_data.get("ltp", idx_data.get("lastTradedPrice", 0))) or 0)
    if ltp == 0:
        return None
    step = cfg["step"]
    return round(ltp / step) * step


def scan_greeks_for_underlying(client: GrowwAPI, underlying: str) -> dict:
    """Scan Greeks for strikes around ATM using get_option_chain (single API call).

    Returns {strike_int: {ce_delta, ce_gamma, ..., pe_delta, ..., ce_ltp, pe_ltp,
                          ce_oi, pe_oi, ce_volume, pe_volume, ce_symbol, pe_symbol, expiry}}.
    """
    cfg = GREEKS_UNDERLYINGS[underlying]
    exchange = cfg["exchange"]
    step = cfg["step"]
    expiry = _get_nearest_weekly_expiry(underlying)

    try:
        chain = client.get_option_chain(exchange, underlying, expiry)
    except Exception as e:
        log.error(f"Greeks: option chain fetch failed for {underlying}: {e}")
        return {}

    if not chain or not isinstance(chain, dict):
        log.warning(f"Greeks: Empty chain for {underlying}")
        return {}

    underlying_ltp = float(chain.get("underlying_ltp", 0) or 0)
    strikes_raw = chain.get("strikes", {})
    if not strikes_raw:
        log.warning(f"Greeks: No strikes in chain for {underlying}")
        return {}

    # Determine ATM from chain's underlying_ltp (more accurate than feed)
    atm = round(underlying_ltp / step) * step if underlying_ltp else None
    if atm is None:
        atm = _get_atm_strike(underlying, cfg)
    if atm is None:
        log.warning(f"Greeks: No ATM for {underlying}")
        return {}

    # Filter to ATM ± 5 strikes
    snapshot = {}
    for strike_str, data in strikes_raw.items():
        try:
            strike_val = int(float(strike_str))
        except (ValueError, TypeError):
            continue

        if abs(strike_val - atm) > 5 * step:
            continue

        ce = data.get("CE", {}) or {}
        pe = data.get("PE", {}) or {}
        ce_g = ce.get("greeks", {}) or {}
        pe_g = pe.get("greeks", {}) or {}

        def _g(g, key):
            v = g.get(key)
            return float(v) if v is not None else 0.0

        snapshot[strike_val] = {
            "ce_delta": _g(ce_g, "delta"),
            "ce_gamma": _g(ce_g, "gamma"),
            "ce_theta": _g(ce_g, "theta"),
            "ce_vega": _g(ce_g, "vega"),
            "ce_iv": _g(ce_g, "iv"),
            "pe_delta": _g(pe_g, "delta"),
            "pe_gamma": _g(pe_g, "gamma"),
            "pe_theta": _g(pe_g, "theta"),
            "pe_vega": _g(pe_g, "vega"),
            "pe_iv": _g(pe_g, "iv"),
            "ce_ltp": float(ce.get("ltp", 0) or 0),
            "pe_ltp": float(pe.get("ltp", 0) or 0),
            "ce_oi": int(ce.get("open_interest", 0) or 0),
            "pe_oi": int(pe.get("open_interest", 0) or 0),
            "ce_volume": int(ce.get("volume", 0) or 0),
            "pe_volume": int(pe.get("volume", 0) or 0),
            "ce_symbol": ce.get("trading_symbol", _build_trading_symbol(underlying, expiry, strike_val, "CE")),
            "pe_symbol": pe.get("trading_symbol", _build_trading_symbol(underlying, expiry, strike_val, "PE")),
            "expiry": expiry,
        }

    greeks_count = sum(1 for s in snapshot.values() if s["ce_delta"] != 0 or s["pe_delta"] != 0)
    log.info(f"Greeks scan {underlying}: {len(snapshot)} strikes, {greeks_count} with data, ATM={atm}, expiry={expiry}, ltp={underlying_ltp:.0f}")
    return snapshot


def _score_bullish_ce(strike_data: dict, prev_data: dict | None, all_data: dict, atm: int, underlying_ltp: float,
                      history: list[dict] | None = None, trend_dir: str = "FLAT",
                      max_pain: float = 0.0, support: float = 0.0, resistance: float = 0.0) -> tuple[int, list[str]]:
    """Score bullish CE buy signal. Returns (score, reasons).
    Strict scoring with 6-layer confirmation — only genuinely strong setups should pass 75+."""
    score = 0
    reasons = []

    d = strike_data
    p = prev_data or {}
    strike = d.get("strike", 0)
    history = history or []

    # HARD FILTER: only ATM or 1 strike ITM — no deep OTM noise
    if strike < atm - 100 or strike > atm + 200:
        return 0, []

    # HARD FILTER: must have meaningful volume (avoid illiquid strikes)
    if d.get("ce_volume", 0) < 500:
        return 0, []

    # CE delta sweet spot 0.40-0.60 (tight range, truly near ATM)
    if 0.40 <= d["ce_delta"] <= 0.60:
        score += 10
        reasons.append(f"Delta {d['ce_delta']:.2f}")
    else:
        return 0, []  # outside sweet spot = not a buy signal

    # RISING delta from previous scan (momentum confirmation)
    if p and d["ce_delta"] > p.get("ce_delta", 0) + 0.03:
        score += 15
        reasons.append(f"Delta rising (+{d['ce_delta'] - p.get('ce_delta', 0):.3f})")

    # Low IV < 20% (genuinely cheap premium, not just slightly below average)
    if 0 < d["ce_iv"] < 18:
        score += 15
        reasons.append(f"Low IV {d['ce_iv']:.1f}%")
    elif 0 < d["ce_iv"] < 20:
        score += 8
        reasons.append(f"Moderate IV {d['ce_iv']:.1f}%")

    # PCR strongly bullish (PE writers dominating = strong support)
    total_pe_oi = sum(v.get("pe_oi", 0) for v in all_data.values())
    total_ce_oi = sum(v.get("ce_oi", 0) for v in all_data.values())
    if total_ce_oi > 0:
        pcr = total_pe_oi / total_ce_oi
        if pcr > 1.3:
            score += 20
            reasons.append(f"PCR {pcr:.2f} (strong put support)")
        elif pcr > 1.15:
            score += 10
            reasons.append(f"PCR {pcr:.2f}")

    # Heavy PE OI at this strike = strong support level
    if d.get("pe_oi", 0) > d.get("ce_oi", 0) * 2 and d.get("pe_oi", 0) > 100000:
        score += 15
        reasons.append(f"PE OI support {d['pe_oi']:,}")

    # IV dropping from previous scan (premium cheapening = good entry)
    if p and p.get("ce_iv", 0) > 0 and d["ce_iv"] < p.get("ce_iv", 0) - 0.5:
        score += 10
        reasons.append(f"IV dropping ({p.get('ce_iv', 0):.1f}→{d['ce_iv']:.1f})")

    # OI buildup on CE side (fresh buying, not just existing positions)
    if p and d.get("ce_oi", 0) > p.get("ce_oi", 0) * 1.1 and d.get("ce_oi", 0) > 50000:
        score += 10
        reasons.append("CE OI buildup")

    # ── Enhancement 1: Multi-scan delta trend ──
    if history:
        trending, consec = _check_delta_trend(strike, history, "ce_delta")
        if trending:
            score += 20
            reasons.append(f"Delta trending {consec} scans")
        if _check_oi_consistency(strike, history, "ce_oi"):
            score += 10
            reasons.append("OI building consistently")

    # ── Enhancement 2: Underlying price trend ──
    if trend_dir == "UP":
        score += 15
        reasons.append("Underlying trending UP")
    elif trend_dir == "DOWN":
        score -= 20
        reasons.append("Against trend (underlying DOWN)")

    # ── Enhancement 3: OI velocity ──
    if history and len(history) >= 2:
        oi_chg, is_spike = _oi_velocity(strike, history, "ce_oi")
        if is_spike:
            score += 15
            reasons.append(f"OI spike +{oi_chg:.0f}%")
        vol_oi = _volume_oi_ratio(d, "ce_")
        if vol_oi > 0.5:
            score += 10
            reasons.append(f"Fresh positions (V/OI {vol_oi:.2f})")
        elif vol_oi < 0.1 and d.get("ce_oi", 0) > 100000:
            score -= 5

    # ── Enhancement 4: Max pain ──
    if max_pain > 0 and underlying_ltp > 0:
        if underlying_ltp < max_pain:
            score += 10
            reasons.append(f"Below max pain {max_pain:.0f}")
        elif underlying_ltp > max_pain + 200:
            score -= 10
            reasons.append(f"Well above max pain")

    # ── Enhancement 5: IV skew ──
    ce_iv = d.get("ce_iv", 0)
    pe_iv = d.get("pe_iv", 0)
    if ce_iv > 0 and pe_iv > 0:
        skew = pe_iv / ce_iv
        if skew > 1.3:
            score += 12
            reasons.append(f"Fear skew (PE/CE IV {skew:.2f})")
        elif skew < 0.8:
            score -= 8

    # ── Enhancement 6: Support/resistance structure ──
    if support > 0 and underlying_ltp > 0:
        dist_pct = abs(underlying_ltp - support) / underlying_ltp * 100
        if dist_pct <= 0.5:
            score += 15
            reasons.append(f"Near support {support:.0f}")
        elif dist_pct > 2.0:
            score -= 5

    return score, reasons


def _score_bearish_pe(strike_data: dict, prev_data: dict | None, all_data: dict, atm: int, underlying_ltp: float,
                      history: list[dict] | None = None, trend_dir: str = "FLAT",
                      max_pain: float = 0.0, support: float = 0.0, resistance: float = 0.0) -> tuple[int, list[str]]:
    """Score bearish PE buy signal. Returns (score, reasons).
    Strict scoring with 6-layer confirmation — only genuinely strong setups should pass 75+."""
    score = 0
    reasons = []

    d = strike_data
    strike = d.get("strike", 0)
    history = history or []

    # HARD FILTER: only ATM or 1 strike ITM
    if strike > atm + 100 or strike < atm - 200:
        return 0, []

    # HARD FILTER: must have meaningful volume
    if d.get("pe_volume", 0) < 500:
        return 0, []

    # PE delta sweet spot 0.40-0.60 magnitude
    if 0.40 <= abs(d["pe_delta"]) <= 0.60:
        score += 10
        reasons.append(f"PE Delta {d['pe_delta']:.2f}")
    else:
        return 0, []

    # RISING PE delta (magnitude) from previous scan
    if prev_data and abs(d["pe_delta"]) > abs(prev_data.get("pe_delta", 0)) + 0.03:
        score += 15
        reasons.append(f"PE delta rising")

    # Low PE IV
    if 0 < d["pe_iv"] < 18:
        score += 15
        reasons.append(f"Low IV {d['pe_iv']:.1f}%")
    elif 0 < d["pe_iv"] < 20:
        score += 8
        reasons.append(f"Moderate IV {d['pe_iv']:.1f}%")

    # PCR strongly bearish (CE writers dominating = resistance)
    total_ce_oi = sum(v.get("ce_oi", 0) for v in all_data.values())
    total_pe_oi = sum(v.get("pe_oi", 0) for v in all_data.values())
    if total_pe_oi > 0:
        pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 999
        if pcr < 0.7:
            score += 20
            reasons.append(f"PCR {pcr:.2f} (bearish)")
        elif pcr < 0.85:
            score += 10
            reasons.append(f"PCR {pcr:.2f}")

    # Heavy CE OI at this strike = strong resistance
    if d.get("ce_oi", 0) > d.get("pe_oi", 0) * 2 and d.get("ce_oi", 0) > 100000:
        score += 15
        reasons.append(f"CE OI resistance {d['ce_oi']:,}")

    # IV dropping from previous scan
    if prev_data and prev_data.get("pe_iv", 0) > 0 and d["pe_iv"] < prev_data.get("pe_iv", 0) - 0.5:
        score += 10
        reasons.append(f"IV dropping")

    # OI buildup on PE side
    if prev_data and d.get("pe_oi", 0) > prev_data.get("pe_oi", 0) * 1.1 and d.get("pe_oi", 0) > 50000:
        score += 10
        reasons.append("PE OI buildup")

    # ── Enhancement 1: Multi-scan delta trend ──
    if history:
        trending, consec = _check_delta_trend(strike, history, "pe_delta")
        if trending:
            score += 20
            reasons.append(f"PE delta trending {consec} scans")
        if _check_oi_consistency(strike, history, "pe_oi"):
            score += 10
            reasons.append("PE OI building consistently")

    # ── Enhancement 2: Underlying price trend (mirror of CE) ──
    if trend_dir == "DOWN":
        score += 15
        reasons.append("Underlying trending DOWN")
    elif trend_dir == "UP":
        score -= 20
        reasons.append("Against trend (underlying UP)")

    # ── Enhancement 3: OI velocity ──
    if history and len(history) >= 2:
        oi_chg, is_spike = _oi_velocity(strike, history, "pe_oi")
        if is_spike:
            score += 15
            reasons.append(f"PE OI spike +{oi_chg:.0f}%")
        vol_oi = _volume_oi_ratio(d, "pe_")
        if vol_oi > 0.5:
            score += 10
            reasons.append(f"Fresh positions (V/OI {vol_oi:.2f})")
        elif vol_oi < 0.1 and d.get("pe_oi", 0) > 100000:
            score -= 5

    # ── Enhancement 4: Max pain ──
    if max_pain > 0 and underlying_ltp > 0:
        if underlying_ltp > max_pain:
            score += 10
            reasons.append(f"Above max pain {max_pain:.0f}")
        elif underlying_ltp < max_pain - 200:
            score -= 10
            reasons.append(f"Well below max pain")

    # ── Enhancement 5: IV skew ──
    ce_iv = d.get("ce_iv", 0)
    pe_iv = d.get("pe_iv", 0)
    if ce_iv > 0 and pe_iv > 0:
        skew = ce_iv / pe_iv
        if skew > 1.3:
            score += 12
            reasons.append(f"Call fear skew (CE/PE IV {skew:.2f})")
        elif skew < 0.8:
            score -= 8

    # ── Enhancement 6: Resistance structure ──
    if resistance > 0 and underlying_ltp > 0:
        dist_pct = abs(resistance - underlying_ltp) / underlying_ltp * 100
        if dist_pct <= 0.5:
            score += 15
            reasons.append(f"Near resistance {resistance:.0f}")
        elif dist_pct > 2.0:
            score -= 5

    return score, reasons


def _score_sell_signal(strike_data: dict, opt_type: str) -> tuple[int, list[str]]:
    """Score premium sell signal. Returns (score, reasons).
    Very strict — selling naked options is risky, need high conviction."""
    score = 0
    reasons = []

    pfx = "ce_" if opt_type == "CE" else "pe_"
    d = strike_data
    iv = d[f"{pfx}iv"]
    theta = d[f"{pfx}theta"]
    gamma = d[f"{pfx}gamma"]
    delta = abs(d[f"{pfx}delta"])
    vega = abs(d[f"{pfx}vega"])
    oi = d.get(f"{pfx}oi", 0)
    volume = d.get(f"{pfx}volume", 0)

    # HARD FILTER: must be OTM (delta < 0.30) — never sell ATM/ITM
    if delta >= 0.30:
        return 0, []

    # HARD FILTER: must have decent OI (liquid, not trapped)
    if oi < 50000:
        return 0, []

    # High IV (>28%) + high theta decay — overpriced premium
    if iv > 30 and abs(theta) > 100:
        score += 25
        reasons.append(f"High IV {iv:.1f}% + theta {theta:.0f}")
    elif iv > 25 and abs(theta) > 80:
        score += 15
        reasons.append(f"Elevated IV {iv:.1f}%")

    # Very low delta (deep OTM, likely expires worthless)
    if delta < 0.15:
        score += 20
        reasons.append(f"Deep OTM delta {delta:.2f}")
    elif delta < 0.20:
        score += 10
        reasons.append(f"OTM delta {delta:.2f}")

    # Low gamma (stable, won't whip against you)
    if abs(gamma) < 0.0002:
        score += 10
        reasons.append(f"Low gamma {abs(gamma):.4f}")

    # High OI (many writers = consensus safe zone)
    if oi > 200000:
        score += 15
        reasons.append(f"Heavy OI {oi:,}")
    elif oi > 100000:
        score += 8

    # Low vega (less vol risk)
    if vega < 3:
        score += 10
        reasons.append(f"Low vega {vega:.1f}")

    return score, reasons


# TradingView symbol mapping for chart analysis
_TV_SYMBOLS = {
    "SENSEX": {"symbol": "SENSEX", "exchange": "BSE", "screener": "india"},
    "NIFTY": {"symbol": "NIFTY", "exchange": "NSE", "screener": "india"},
    "BANKNIFTY": {"symbol": "BANKNIFTY", "exchange": "NSE", "screener": "india"},
}

_TV_TIMEFRAMES = [
    ("|1W", "W"),
    ("|1D", "D"),
    ("|60", "1H"),
    ("|15", "15m"),
    ("|5", "5m"),
]

# Indicators to fetch from TradingView scanner API
_TV_INDICATORS = [
    "Recommend.All", "Recommend.Other", "Recommend.MA",
    "RSI", "MACD.macd", "MACD.signal", "EMA20", "SMA50", "SMA200",
    "BB.upper", "BB.lower", "ATR", "ADX", "Stoch.K", "Stoch.D",
    "CCI20", "close", "volume", "AO",
]

_tv_cache: dict[str, tuple[float, dict]] = {}  # underlying -> (timestamp, data)
_TV_CACHE_TTL = 300  # 5 minutes
import threading
_tv_lock = threading.Lock()


def _tv_rec_label(val: float | None) -> str:
    """Convert TradingView Rec.All numeric to label."""
    if val is None:
        return "NEUTRAL"
    if val >= 0.5:
        return "STRONG_BUY"
    if val >= 0.1:
        return "BUY"
    if val <= -0.5:
        return "STRONG_SELL"
    if val <= -0.1:
        return "SELL"
    return "NEUTRAL"


def _get_chart_analysis(underlying: str) -> dict:
    """Fetch ALL TradingView timeframes in a SINGLE API call + cache.
    Uses lock to prevent duplicate concurrent fetches.
    Returns {timeframe: {rec, rsi, macd, ...}} or {} on failure."""
    import time as _time
    import requests as _req

    # Check cache first (no lock needed for read)
    cached = _tv_cache.get(underlying)
    if cached and (_time.time() - cached[0]) < _TV_CACHE_TTL:
        return cached[1]

    # Acquire lock so only one caller fetches at a time
    with _tv_lock:
        # Re-check cache (another thread may have populated it while we waited)
        cached = _tv_cache.get(underlying)
        if cached and (_time.time() - cached[0]) < _TV_CACHE_TTL:
            return cached[1]

        tv = _TV_SYMBOLS.get(underlying)
        if not tv:
            return {}

        # Build columns for ALL timeframes in one request
        columns = []
        col_map = []  # (tf_label, indicator_name, col_index)
        for interval_str, label in _TV_TIMEFRAMES:
            for ind in _TV_INDICATORS:
                columns.append(f"{ind}{interval_str}")
                col_map.append((label, ind))

        ticker = f"{tv['exchange']}:{tv['symbol']}"
        payload = {
            "symbols": {"tickers": [ticker]},
            "columns": columns,
        }
        try:
            resp = _req.post(
                f"https://scanner.tradingview.com/{tv['screener'].lower()}/scan",
                json=payload,
                headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0"},
                timeout=15,
            )
            if resp.status_code != 200:
                log.warning(f"TV scanner {tv['symbol']} HTTP {resp.status_code}")
                return {}
            data = resp.json().get("data", [])
            if not data:
                return {}
        except Exception as e:
            log.warning(f"TV scanner {tv['symbol']} failed: {e}")
            return {}

        vals = data[0]["d"]
        # Parse values back into per-timeframe dicts
        tf_data: dict[str, dict] = {}
        for i, (label, ind_name) in enumerate(col_map):
            if label not in tf_data:
                tf_data[label] = {}
            tf_data[label][ind_name] = vals[i]

        result = {}
        for label, raw in tf_data.items():
            rec_val = raw.get("Recommend.All")
            result[label] = {
                "rec": _tv_rec_label(rec_val),
                "rec_score": round(rec_val or 0, 2),
                "buy": 0, "sell": 0, "neutral": 0,
                "rsi": round(raw.get("RSI", 0) or 0, 1),
                "macd": round(raw.get("MACD.macd", 0) or 0, 1),
                "macd_signal": round(raw.get("MACD.signal", 0) or 0, 1),
                "ema20": round(raw.get("EMA20", 0) or 0, 1),
                "sma50": round(raw.get("SMA50", 0) or 0, 1),
                "sma200": round(raw.get("SMA200", 0) or 0, 1),
                "bb_upper": round(raw.get("BB.upper", 0) or 0, 1),
                "bb_lower": round(raw.get("BB.lower", 0) or 0, 1),
                "atr": round(raw.get("ATR", 0) or 0, 1),
                "adx": round(raw.get("ADX", 0) or 0, 1),
                "stoch_k": round(raw.get("Stoch.K", 0) or 0, 1),
                "stoch_d": round(raw.get("Stoch.D", 0) or 0, 1),
                "cci": round(raw.get("CCI20", 0) or 0, 1),
                "close": round(raw.get("close", 0) or 0, 2),
                "volume": round(raw.get("volume", 0) or 0, 0),
                "ao": round(raw.get("AO", 0) or 0, 1),
            }

        # Cache result
        if result:
            _tv_cache[underlying] = (_time.time(), result)
            log.info(f"TV chart {underlying}: {len(result)} timeframes fetched OK")
        return result


def _chart_rec_emoji(rec: str) -> str:
    """Map TradingView recommendation to emoji."""
    return {
        "STRONG_BUY": "🟢🟢",
        "BUY": "🟢",
        "NEUTRAL": "⚪",
        "SELL": "🔴",
        "STRONG_SELL": "🔴🔴",
    }.get(rec, "⚪")


def _chart_bias_score(chart: dict) -> tuple[str, int]:
    """Derive overall chart bias and a numeric score (-100 to +100).
    Positive = bullish (CE), Negative = bearish (PE)."""
    if not chart:
        return "NEUTRAL", 0
    total = 0
    weights = {"W": 3, "D": 2, "1H": 1.5, "15m": 1, "5m": 0.5}
    for tf, w in weights.items():
        data = chart.get(tf)
        if not data:
            continue
        rec = data["rec"]
        if rec == "STRONG_BUY":
            total += 30 * w
        elif rec == "BUY":
            total += 15 * w
        elif rec == "SELL":
            total -= 15 * w
        elif rec == "STRONG_SELL":
            total -= 30 * w
    # Clamp to -100..+100
    score = max(-100, min(100, int(total)))
    if score >= 30:
        return "BULLISH", score
    elif score <= -30:
        return "BEARISH", score
    return "NEUTRAL", score


def _detect_breakout(chart: dict, underlying_ltp: float, support: float, resistance: float,
                     snapshot: dict, history: list[dict]) -> list[str]:
    """Detect breakout and false breakout signals from chart + Greeks data.
    Returns list of signal strings."""
    signals = []
    if not chart:
        return signals

    h1 = chart.get("1H", {})
    m15 = chart.get("15m", {})
    m5 = chart.get("5m", {})
    daily = chart.get("D", {})

    close = m5.get("close", 0) or m15.get("close", 0) or underlying_ltp
    if close == 0:
        return signals

    bb_upper = h1.get("bb_upper", 0)
    bb_lower = h1.get("bb_lower", 0)
    atr_1h = h1.get("atr", 0)
    adx_1h = h1.get("adx", 0)
    rsi_15m = m15.get("rsi", 50)
    rsi_5m = m5.get("rsi", 50)
    rsi_1h = h1.get("rsi", 50)
    vol_5m = m5.get("volume", 0)
    macd_5m = m5.get("macd", 0)
    macd_sig_5m = m5.get("macd_signal", 0)
    cci_15m = m15.get("cci", 0)
    stoch_k = m15.get("stoch_k", 50)
    sma50_d = daily.get("sma50", 0)
    ema20_1h = h1.get("ema20", 0)

    # ── Breakout Detection ──

    # 1. Resistance breakout — price above resistance with momentum
    if resistance > 0 and close > resistance:
        strength_factors = 0
        reasons = []
        if adx_1h > 25:
            strength_factors += 1
            reasons.append(f"ADX {adx_1h:.0f}")
        if rsi_15m > 60:
            strength_factors += 1
            reasons.append(f"RSI {rsi_15m:.0f}")
        if macd_5m > macd_sig_5m:
            strength_factors += 1
            reasons.append("MACD cross ↑")
        if cci_15m > 100:
            strength_factors += 1
            reasons.append(f"CCI {cci_15m:.0f}")
        if stoch_k > 80:
            strength_factors += 1
            reasons.append(f"Stoch {stoch_k:.0f}")

        if strength_factors >= 2:
            signals.append(f"🚀 <b>BREAKOUT ↑</b> above R {resistance:,.0f} ({', '.join(reasons)})")

    # 2. Support breakdown — price below support with momentum
    if support > 0 and close < support:
        strength_factors = 0
        reasons = []
        if adx_1h > 25:
            strength_factors += 1
            reasons.append(f"ADX {adx_1h:.0f}")
        if rsi_15m < 40:
            strength_factors += 1
            reasons.append(f"RSI {rsi_15m:.0f}")
        if macd_5m < macd_sig_5m:
            strength_factors += 1
            reasons.append("MACD cross ↓")
        if cci_15m < -100:
            strength_factors += 1
            reasons.append(f"CCI {cci_15m:.0f}")
        if stoch_k < 20:
            strength_factors += 1
            reasons.append(f"Stoch {stoch_k:.0f}")

        if strength_factors >= 2:
            signals.append(f"💥 <b>BREAKDOWN ↓</b> below S {support:,.0f} ({', '.join(reasons)})")

    # 3. Bollinger Band breakout
    if bb_upper > 0 and close > bb_upper and rsi_15m > 60:
        signals.append(f"📈 BB breakout ↑ (close {close:,.0f} &gt; upper {bb_upper:,.0f})")
    elif bb_lower > 0 and close < bb_lower and rsi_15m < 40:
        signals.append(f"📉 BB breakdown ↓ (close {close:,.0f} &lt; lower {bb_lower:,.0f})")

    # ── False Breakout Detection ──

    # 4. Price crossed resistance but momentum dying — false breakout up
    if resistance > 0 and close > resistance and close < resistance + atr_1h * 0.3:
        false_signs = 0
        reasons = []
        if rsi_15m < 50:
            false_signs += 1
            reasons.append(f"RSI weak {rsi_15m:.0f}")
        if rsi_5m < rsi_15m:
            false_signs += 1
            reasons.append("RSI fading")
        if macd_5m < macd_sig_5m:
            false_signs += 1
            reasons.append("MACD cross ↓")
        if adx_1h < 20:
            false_signs += 1
            reasons.append(f"ADX low {adx_1h:.0f}")
        if stoch_k > 80 and stoch_k < m15.get("stoch_d", 50):
            false_signs += 1
            reasons.append("Stoch diverging")

        if false_signs >= 2:
            signals.append(f"🚫 <b>FALSE BREAKOUT ↑</b> near R {resistance:,.0f} ({', '.join(reasons)})")

    # 5. Price crossed support but momentum dying — false breakdown
    if support > 0 and close < support and close > support - atr_1h * 0.3:
        false_signs = 0
        reasons = []
        if rsi_15m > 50:
            false_signs += 1
            reasons.append(f"RSI holding {rsi_15m:.0f}")
        if rsi_5m > rsi_15m:
            false_signs += 1
            reasons.append("RSI recovering")
        if macd_5m > macd_sig_5m:
            false_signs += 1
            reasons.append("MACD cross ↑")
        if adx_1h < 20:
            false_signs += 1
            reasons.append(f"ADX low {adx_1h:.0f}")

        if false_signs >= 2:
            signals.append(f"🚫 <b>FALSE BREAKDOWN ↓</b> near S {support:,.0f} ({', '.join(reasons)})")

    # ── Greeks-confirmed breakout (OI spike at level) ──
    if snapshot and history and len(history) >= 2:
        prev = history[-2]
        for strike, data in snapshot.items():
            # Huge PE OI buildup at support = support holding (false breakdown)
            if support > 0 and abs(strike - support) <= 100:
                pe_oi = data.get("pe_oi", 0)
                prev_pe_oi = prev.get(strike, {}).get("pe_oi", 0)
                if prev_pe_oi > 0 and pe_oi > prev_pe_oi * 1.3:
                    signals.append(f"🛡️ PE OI surge +{((pe_oi/prev_pe_oi)-1)*100:.0f}% at S {strike} — support defended")
                    break

            # Huge CE OI buildup at resistance = resistance holding (false breakout)
            if resistance > 0 and abs(strike - resistance) <= 100:
                ce_oi = data.get("ce_oi", 0)
                prev_ce_oi = prev.get(strike, {}).get("ce_oi", 0)
                if prev_ce_oi > 0 and ce_oi > prev_ce_oi * 1.3:
                    signals.append(f"🧱 CE OI surge +{((ce_oi/prev_ce_oi)-1)*100:.0f}% at R {strike} — resistance holding")
                    break

    return signals


def _format_scan_summary(underlying: str, snapshot: dict, history: list[dict],
                         chart: dict | None = None) -> str:
    """Format a scan with chart + Greeks trade idea — strike, entry, SL, TP."""
    cfg = GREEKS_UNDERLYINGS[underlying]
    idx_data = state.index_data.get(cfg["index_token"], {})
    underlying_ltp = float(idx_data.get("value", idx_data.get("ltp", idx_data.get("lastTradedPrice", 0))) or 0)
    lot = cfg["lot"]

    # ATM strike
    step = cfg["step"]
    atm = round(underlying_ltp / step) * step if underlying_ltp else None
    if atm is None:
        atm = _get_atm_strike(underlying, cfg)
    if atm is None:
        return f"📊 <b>{underlying} Scan</b>\n\nNo ATM data available."

    # Max pain, support/resistance
    max_pain = 0.0
    support = 0.0
    resistance = 0.0
    if underlying_ltp > 0:
        oi_snap = _snapshot_to_oi(underlying, snapshot, underlying_ltp)
        max_pain = calculate_max_pain(oi_snap)
        support, resistance = find_support_resistance(oi_snap)

    # Price trend
    trend_dir, trend_pct = _get_price_trend(cfg["index_token"])
    trend_arrow = "↑" if trend_dir == "UP" else ("↓" if trend_dir == "DOWN" else "→")

    # Chart analysis
    if chart is None:
        chart = {}
    chart_bias, chart_score = _chart_bias_score(chart)

    prev_snapshot = history[-2] if len(history) >= 2 else None

    # Score all strikes, find best CE and PE with full data
    best_ce_score, best_ce_strike, best_ce_reasons, best_ce_data = 0, atm, [], {}
    best_pe_score, best_pe_strike, best_pe_reasons, best_pe_data = 0, atm, [], {}

    for strike, data in snapshot.items():
        data["strike"] = strike
        prev_data = prev_snapshot.get(strike) if prev_snapshot else None

        ce_score, ce_reasons = _score_bullish_ce(
            data, prev_data, snapshot, atm, underlying_ltp,
            history=history, trend_dir=trend_dir,
            max_pain=max_pain, support=support, resistance=resistance)
        if ce_score > best_ce_score:
            best_ce_score, best_ce_strike, best_ce_reasons, best_ce_data = ce_score, strike, ce_reasons, data

        pe_score, pe_reasons = _score_bearish_pe(
            data, prev_data, snapshot, atm, underlying_ltp,
            history=history, trend_dir=trend_dir,
            max_pain=max_pain, support=support, resistance=resistance)
        if pe_score > best_pe_score:
            best_pe_score, best_pe_strike, best_pe_reasons, best_pe_data = pe_score, strike, pe_reasons, data

    # Apply chart alignment — HARD VETO for strong bias, else moderate adjustment
    if chart_score <= -50:
        # Strong bearish chart: kill CE, heavily boost PE
        best_ce_score = 0
        best_pe_score = min(100, best_pe_score + abs(chart_score) // 3)
    elif chart_score >= 50:
        # Strong bullish chart: kill PE, heavily boost CE
        best_pe_score = 0
        best_ce_score = min(100, best_ce_score + chart_score // 3)
    elif chart_score > 0:
        best_ce_score = min(100, best_ce_score + chart_score // 4)
        best_pe_score = max(0, best_pe_score - chart_score // 6)
    elif chart_score < 0:
        best_pe_score = min(100, best_pe_score + abs(chart_score) // 4)
        best_ce_score = max(0, best_ce_score - abs(chart_score) // 6)

    # Pick the stronger direction for trade idea
    if best_ce_score >= best_pe_score and best_ce_score > 0:
        pick = "CE"
        pick_score = best_ce_score
        pick_strike = best_ce_strike
        pick_reasons = best_ce_reasons
        pick_entry = best_ce_data.get("ce_ltp", 0)
        pick_symbol = best_ce_data.get("ce_symbol", f"{underlying}{pick_strike}CE")
        pick_delta = best_ce_data.get("ce_delta", 0)
        pick_iv = best_ce_data.get("ce_iv", 0)
        pick_oi = best_ce_data.get("ce_oi", 0)
    else:
        pick = "PE"
        pick_score = best_pe_score
        pick_strike = best_pe_strike
        pick_reasons = best_pe_reasons
        pick_entry = best_pe_data.get("pe_ltp", 0)
        pick_symbol = best_pe_data.get("pe_symbol", f"{underlying}{pick_strike}PE")
        pick_delta = best_pe_data.get("pe_delta", 0)
        pick_iv = best_pe_data.get("pe_iv", 0)
        pick_oi = best_pe_data.get("pe_oi", 0)

    # Check chart-greeks alignment
    greeks_bullish = pick == "CE"
    chart_bullish = chart_score > 0
    aligned = (greeks_bullish and chart_bullish) or (not greeks_bullish and not chart_bullish)
    if chart and aligned:
        pick_score = min(100, pick_score + 5)  # Small bonus for agreement

    # Calculate SL and TP based on entry
    if pick_entry > 0:
        sl = round(pick_entry * 0.75, 2)   # 25% SL
        tp1 = round(pick_entry * 1.30, 2)  # 30% TP1
        tp2 = round(pick_entry * 1.50, 2)  # 50% TP2
        sl_pct = 25
        risk_per_lot = round((pick_entry - sl) * lot, 0)
        reward_per_lot = round((tp1 - pick_entry) * lot, 0)
    else:
        sl = tp1 = tp2 = 0.0
        sl_pct = 0
        risk_per_lot = reward_per_lot = 0

    scan_count = len(history)
    ltp_fmt = f"{underlying_ltp:,.0f}" if underlying_ltp else "N/A"
    mp_fmt = f"{max_pain:,.0f}" if max_pain else "N/A"
    s_fmt = f"{support:,.0f}" if support else "N/A"
    r_fmt = f"{resistance:,.0f}" if resistance else "N/A"

    pick_emoji = "🟢" if pick == "CE" else "🔴"
    reason_str = " + ".join(pick_reasons[:3]) if pick_reasons else "Low conviction"

    # Strength tier with visual indicators
    if pick_score >= 85:
        strength = "MUST TAKE"
        str_icon = "🔥🔥🔥"
        str_bar = "🟩🟩🟩🟩🟩"
        str_tag = "💎"
    elif pick_score >= 75:
        strength = "STRONG"
        str_icon = "🔥🔥"
        str_bar = "🟩🟩🟩🟩⬜"
        str_tag = "💪"
    elif pick_score >= 50:
        strength = "MODERATE"
        str_icon = "🔥"
        str_bar = "🟨🟨🟨⬜⬜"
        str_tag = "👀"
    else:
        strength = "WEAK"
        str_icon = "💤"
        str_bar = "🟥⬜⬜⬜⬜"
        str_tag = "⚠️"

    # Also show the other direction's score
    alt = "PE" if pick == "CE" else "CE"
    alt_score = best_pe_score if pick == "CE" else best_ce_score
    alt_strike = best_pe_strike if pick == "CE" else best_ce_strike

    now_ist = datetime.now(IST).strftime("%H:%M")

    # ── Build message ──
    text = (
        f"📊 <b>{underlying} Scan</b> — {now_ist} IST\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"LTP: {ltp_fmt} | {trend_arrow} {trend_dir} ({trend_pct:+.2f}%)\n"
        f"Max Pain: {mp_fmt} | S: {s_fmt} | R: {r_fmt}\n\n"
    )

    # Chart section
    if chart:
        text += "📈 <b>Chart (TradingView)</b>\n"
        for tf_label in ["W", "D", "1H", "15m", "5m"]:
            td = chart.get(tf_label)
            if td:
                rec_emoji = _chart_rec_emoji(td["rec"])
                rec_short = td["rec"].replace("STRONG_", "STR ")
                text += f"  {tf_label:>3}: {rec_emoji} {rec_short} (RSI {td['rsi']})\n"
        # Overall chart verdict
        bias_emoji = "🟢" if chart_score > 0 else ("🔴" if chart_score < 0 else "⚪")
        text += f"  {bias_emoji} Chart: <b>{chart_bias}</b> ({chart_score:+d})\n"
        # Alignment check
        if aligned:
            text += "  ✅ Chart + Greeks ALIGNED\n\n"
        else:
            text += "  ⚠️ Chart vs Greeks DIVERGING\n\n"
    else:
        text += "📈 Chart: unavailable\n\n"

    # Breakout / False Breakout detection
    breakout_signals = _detect_breakout(chart, underlying_ltp, support, resistance, snapshot, history)
    if breakout_signals:
        for bs in breakout_signals:
            text += f"{bs}\n"
        text += "\n"

    # Trade idea
    if pick_entry > 0:
        text += (
            f"{str_tag} {str_icon} <b>{strength}</b> {str_icon} {str_tag}\n"
            f"{str_bar} {pick_score}/100\n\n"
            f"{pick_emoji} <b>BUY {pick_symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"Strike: {pick_strike} {pick} | Entry: ₹{pick_entry:.2f}\n"
            f"🛑 SL: ₹{sl:.2f} (-{sl_pct}%)\n"
            f"🎯 TP1: ₹{tp1:.2f} (+30%) | TP2: ₹{tp2:.2f} (+50%)\n"
            f"💰 Risk: ₹{risk_per_lot:,.0f} | Reward: ₹{reward_per_lot:,.0f} /lot\n\n"
            f"📐 Δ {pick_delta:+.2f} | IV {pick_iv:.1f}% | OI {pick_oi:,}\n"
            f"📝 {reason_str}\n\n"
        )
    else:
        text += f"{str_tag} {pick_emoji} {pick} Bias @ {pick_strike} — no premium data\n\n"

    text += (
        f"↔️ Alt: {alt} {alt_score}/100 @ {alt_strike}\n"
        f"🔄 Scans: {scan_count}/5"
    )
    return text


async def hourly_scan_loop(bot_instance):
    """Background task: send trade idea scan to all followers every hour during market hours."""
    log.info("Hourly scan loop started")

    while state.hourly_scan_running:
        now_ist = datetime.now(IST)

        # Only during market hours 9:15 - 15:30 IST, weekdays
        if now_ist.weekday() >= 5:
            await asyncio.sleep(300)
            continue

        market_start = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
        market_end = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
        if now_ist < market_start or now_ist > market_end:
            await asyncio.sleep(60)
            continue

        if not state.master_client:
            await asyncio.sleep(60)
            continue

        config = _read_config()
        follower_tids = [f.get("telegram_id") for f in config.get("followers", []) if f.get("telegram_id")]

        if not follower_tids:
            await asyncio.sleep(3600)
            continue

        for underlying in GREEKS_UNDERLYINGS:
            if not state.hourly_scan_running:
                break
            try:
                log.info(f"Hourly scan: {underlying}")
                snapshot, chart = await asyncio.gather(
                    asyncio.to_thread(scan_greeks_for_underlying, state.master_client, underlying),
                    asyncio.to_thread(_get_chart_analysis, underlying),
                )
                if not snapshot:
                    continue

                hist = state.greeks_history.get(underlying, [])
                text = _format_scan_summary(underlying, snapshot, hist, chart=chart)

                for tid in follower_tids:
                    try:
                        await bot_instance.send_message(tid, text, parse_mode="HTML")
                    except Exception as e:
                        log.warning(f"Hourly scan send to {tid} failed: {e}")

            except Exception as e:
                log.error(f"Hourly scan error for {underlying}: {e}")

            await asyncio.sleep(2)

        # Wait ~1 hour (align to next hour boundary + 5 min buffer)
        now_ist = datetime.now(IST)
        next_hour = (now_ist + timedelta(hours=1)).replace(minute=5, second=0, microsecond=0)
        wait_secs = max(60, (next_hour - now_ist).total_seconds())
        log.info(f"Hourly scan done, next in {wait_secs:.0f}s")
        await asyncio.sleep(wait_secs)

    log.info("Hourly scan loop stopped")


def generate_greeks_signals(underlying: str, snapshot: dict, history: list[dict] | None = None,
                            chart_score: int = 0) -> list[dict]:
    """Generate trade signals from Greeks snapshot. Returns list of signal dicts.
    Requires 3+ scans in history for momentum confirmation (6-layer signal confirmation).
    No conflicting CE+PE buy signals — pick the stronger direction only.
    chart_score: -100..+100 from TradingView. Vetoes signals opposing the chart."""
    cfg = GREEKS_UNDERLYINGS[underlying]
    atm = _get_atm_strike(underlying, cfg)
    if atm is None:
        return []

    history = history or []

    # REQUIRE 3+ history snapshots for multi-scan confirmation — silent until then
    if len(history) < 3:
        log.info(f"Greeks signals: skipping {underlying} (only {len(history)} scans, need 3)")
        return []

    prev_snapshot = history[-2] if len(history) >= 2 else None

    idx_data = state.index_data.get(cfg["index_token"], {})
    underlying_ltp = float(idx_data.get("value", idx_data.get("ltp", idx_data.get("lastTradedPrice", 0))) or 0)

    # Compute max pain and support/resistance once (Enhancement 4 & 6)
    max_pain = 0.0
    support = 0.0
    resistance = 0.0
    if underlying_ltp > 0:
        oi_snap = _snapshot_to_oi(underlying, snapshot, underlying_ltp)
        max_pain = calculate_max_pain(oi_snap)
        support, resistance = find_support_resistance(oi_snap)

    # Get underlying price trend (Enhancement 2)
    trend_dir, trend_pct = _get_price_trend(cfg["index_token"])

    ce_buys = []
    pe_buys = []
    sells = []

    for strike, data in snapshot.items():
        data["strike"] = strike  # inject strike for scoring filters
        prev_data = prev_snapshot.get(strike) if prev_snapshot else None

        # Bullish CE buy — threshold 75
        ce_score, ce_reasons = _score_bullish_ce(
            data, prev_data, snapshot, atm, underlying_ltp,
            history=history, trend_dir=trend_dir,
            max_pain=max_pain, support=support, resistance=resistance)
        if ce_score >= 75 and data["ce_ltp"] > 0:
            ce_buys.append({
                "underlying": underlying,
                "direction": "BUY",
                "opt_type": "CE",
                "strike": strike,
                "symbol": data["ce_symbol"],
                "expiry": data["expiry"],
                "entry": data["ce_ltp"],
                "confidence": min(ce_score, 100),
                "lot_size": cfg["lot"],
                "delta": data["ce_delta"],
                "gamma": data["ce_gamma"],
                "theta": data["ce_theta"],
                "vega": data["ce_vega"],
                "iv": data["ce_iv"],
                "oi": data.get("ce_oi", 0),
                "volume": data.get("ce_volume", 0),
                "reasons": ce_reasons,
            })

        # Bearish PE buy — threshold 75
        pe_score, pe_reasons = _score_bearish_pe(
            data, prev_data, snapshot, atm, underlying_ltp,
            history=history, trend_dir=trend_dir,
            max_pain=max_pain, support=support, resistance=resistance)
        if pe_score >= 75 and data["pe_ltp"] > 0:
            pe_buys.append({
                "underlying": underlying,
                "direction": "BUY",
                "opt_type": "PE",
                "strike": strike,
                "symbol": data["pe_symbol"],
                "expiry": data["expiry"],
                "entry": data["pe_ltp"],
                "confidence": min(pe_score, 100),
                "lot_size": cfg["lot"],
                "delta": data["pe_delta"],
                "gamma": data["pe_gamma"],
                "theta": data["pe_theta"],
                "vega": data["pe_vega"],
                "iv": data["pe_iv"],
                "oi": data.get("pe_oi", 0),
                "volume": data.get("pe_volume", 0),
                "reasons": pe_reasons,
            })

        # CE sell — threshold 70
        ce_sell_score, ce_sell_reasons = _score_sell_signal(data, "CE")
        if ce_sell_score >= 70 and data["ce_ltp"] > 0:
            sells.append({
                "underlying": underlying,
                "direction": "SELL",
                "opt_type": "CE",
                "strike": strike,
                "symbol": data["ce_symbol"],
                "expiry": data["expiry"],
                "entry": data["ce_ltp"],
                "confidence": min(ce_sell_score, 100),
                "lot_size": cfg["lot"],
                "delta": data["ce_delta"],
                "gamma": data["ce_gamma"],
                "theta": data["ce_theta"],
                "vega": data["ce_vega"],
                "iv": data["ce_iv"],
                "oi": data.get("ce_oi", 0),
                "volume": data.get("ce_volume", 0),
                "reasons": ce_sell_reasons,
            })

        # PE sell — threshold 70
        pe_sell_score, pe_sell_reasons = _score_sell_signal(data, "PE")
        if pe_sell_score >= 70 and data["pe_ltp"] > 0:
            sells.append({
                "underlying": underlying,
                "direction": "SELL",
                "opt_type": "PE",
                "strike": strike,
                "symbol": data["pe_symbol"],
                "expiry": data["expiry"],
                "entry": data["pe_ltp"],
                "confidence": min(pe_sell_score, 100),
                "lot_size": cfg["lot"],
                "delta": data["pe_delta"],
                "gamma": data["pe_gamma"],
                "theta": data["pe_theta"],
                "vega": data["pe_vega"],
                "iv": data["pe_iv"],
                "oi": data.get("pe_oi", 0),
                "volume": data.get("pe_volume", 0),
                "reasons": pe_sell_reasons,
            })

    # CHART VETO: block signals that go against a strong chart bias
    # If chart is strongly bearish (< -50), kill CE buys
    # If chart is strongly bullish (> 50), kill PE buys
    if chart_score <= -50 and ce_buys:
        log.info(f"Chart veto: killing {len(ce_buys)} CE buys (chart_score={chart_score})")
        ce_buys = []
    if chart_score >= 50 and pe_buys:
        log.info(f"Chart veto: killing {len(pe_buys)} PE buys (chart_score={chart_score})")
        pe_buys = []

    # Moderate chart bias: penalize opposing direction
    if chart_score <= -30:
        for s in ce_buys:
            s["confidence"] = max(0, s["confidence"] - 15)
            s["reasons"].append("Chart bearish penalty")
        for s in pe_buys:
            s["confidence"] = min(100, s["confidence"] + 10)
            s["reasons"].append("Chart bearish boost")
    elif chart_score >= 30:
        for s in pe_buys:
            s["confidence"] = max(0, s["confidence"] - 15)
            s["reasons"].append("Chart bullish penalty")
        for s in ce_buys:
            s["confidence"] = min(100, s["confidence"] + 10)
            s["reasons"].append("Chart bullish boost")

    # Re-filter after adjustments — signals below threshold get dropped
    ce_buys = [s for s in ce_buys if s["confidence"] >= 75]
    pe_buys = [s for s in pe_buys if s["confidence"] >= 75]

    # CONFLICT FILTER: don't send both CE buy and PE buy — pick stronger direction
    signals = []
    best_ce = max(ce_buys, key=lambda s: s["confidence"]) if ce_buys else None
    best_pe = max(pe_buys, key=lambda s: s["confidence"]) if pe_buys else None

    if best_ce and best_pe:
        # Both directions triggered — only send the stronger one
        if best_ce["confidence"] >= best_pe["confidence"]:
            signals.append(best_ce)
            log.info(f"Conflict: CE {best_ce['confidence']}% > PE {best_pe['confidence']}%, picking CE")
        else:
            signals.append(best_pe)
            log.info(f"Conflict: PE {best_pe['confidence']}% > CE {best_ce['confidence']}%, picking PE")
    elif best_ce:
        signals.append(best_ce)
    elif best_pe:
        signals.append(best_pe)

    # Add sell signals (no conflict with buy — different strategy)
    sells.sort(key=lambda s: s["confidence"], reverse=True)
    signals.extend(sells[:1])  # max 1 sell signal per scan

    return signals


def _calc_sl_tp(entry: float, direction: str) -> tuple[float, float]:
    """Calculate SL (20%) and TP (40%) from entry."""
    if direction == "BUY":
        sl = round(entry * 0.80, 2)   # 20% below
        tp = round(entry * 1.40, 2)   # 40% above
    else:  # SELL
        sl = round(entry * 1.20, 2)   # 20% above
        tp = round(entry * 0.60, 2)   # 40% below
    return sl, tp


async def send_signal_to_telegram(bot_instance, signal: dict):
    """Send a Greeks signal to all followers' Telegram."""
    entry = signal["entry"]
    sl, tp = _calc_sl_tp(entry, signal["direction"])
    lot = signal["lot_size"]
    qty = lot  # 1 lot

    if signal["direction"] == "BUY":
        sl_pct = round((entry - sl) / entry * 100)
        tp_pct = round((tp - entry) / entry * 100)
        max_loss = round((entry - sl) * qty, 2)
        max_profit = round((tp - entry) * qty, 2)
    else:
        sl_pct = round((sl - entry) / entry * 100)
        tp_pct = round((entry - tp) / entry * 100)
        max_loss = round((sl - entry) * qty, 2)
        max_profit = round((entry - tp) * qty, 2)

    emoji = "🟢" if signal["direction"] == "BUY" else "🔴"
    reason_str = " + ".join(signal["reasons"][:3]) if signal["reasons"] else "Greeks alignment"

    text = (
        f"📊 <b>{signal['underlying']} Signal ({signal['confidence']}% confidence)</b>\n\n"
        f"{emoji} <b>{signal['direction']} {signal['symbol']}</b>\n"
        f"Entry: ₹{entry:.2f} | Qty: {qty} (1 lot)\n"
        f"SL: ₹{sl:.2f} (-{sl_pct}%) | TP: ₹{tp:.2f} (+{tp_pct}%)\n"
        f"Max Loss: ₹{max_loss:,.0f} | Max Profit: ₹{max_profit:,.0f}\n\n"
        f"Greeks: Δ{signal['delta']:+.2f} Γ{signal['gamma']:.4f} "
        f"Θ{signal['theta']:.0f} V{signal['vega']:.1f} IV{signal['iv']:.1f}%\n"
        f"OI: {signal.get('oi', 0):,} | Vol: {signal.get('volume', 0):,}\n"
        f"Reason: {reason_str}"
    )

    # Encode SL/TP in callback — truncate to fit 64-byte callback limit
    txn = signal["direction"]  # BUY or SELL
    sym_short = signal["symbol"][:20]
    cb_trade = f"sg:t:{sym_short}:{txn}:{qty}:{sl:.1f}:{tp:.1f}"
    cb_skip = f"sg:s:{sym_short}"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Quick Trade", callback_data=cb_trade[:64]),
            InlineKeyboardButton(text="❌ Skip", callback_data=cb_skip[:64]),
        ]
    ])

    # Send to all followers
    config = _read_config()
    sent = 0
    for f in config.get("followers", []):
        tid = f.get("telegram_id")
        if tid:
            try:
                await bot_instance.send_message(tid, text, parse_mode="HTML", reply_markup=kb)
                sent += 1
            except Exception as e:
                log.warning(f"Signal send to {tid} failed: {e}")

    log.info(f"Signal sent to {sent} followers: {signal['direction']} {signal['symbol']} ({signal['confidence']}%)")


async def greeks_scanner_loop(bot_instance):
    """Background task: scan Greeks every 60s during market hours, emit signals."""
    log.info("Greeks scanner loop started")

    while state.greeks_scanner_running:
        now_ist = datetime.now(IST)

        # Only scan during market hours 9:15 - 15:30 IST (weekdays)
        if now_ist.weekday() >= 5:  # Saturday/Sunday
            await asyncio.sleep(60)
            continue

        market_start = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
        market_end = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
        if now_ist < market_start or now_ist > market_end:
            await asyncio.sleep(60)
            continue

        if not state.master_client:
            await asyncio.sleep(30)
            continue

        for underlying in GREEKS_UNDERLYINGS:
            if not state.greeks_scanner_running:
                break

            try:
                log.info(f"Greeks scan: {underlying}")
                snapshot, chart = await asyncio.gather(
                    asyncio.to_thread(scan_greeks_for_underlying, state.master_client, underlying),
                    asyncio.to_thread(_get_chart_analysis, underlying),
                )
                if not snapshot:
                    continue

                _, c_score = _chart_bias_score(chart)
                hist = state.greeks_history.setdefault(underlying, [])
                hist.append(snapshot)
                if len(hist) > 5:
                    hist.pop(0)
                signals = generate_greeks_signals(underlying, snapshot, hist, chart_score=c_score)

                now_ts = time.time()
                for sig in signals:
                    sym = sig["symbol"]
                    # Cooldown: skip if same symbol signaled in last 10 min
                    last_time = state.signal_cooldown.get(sym, 0)
                    if now_ts - last_time < 600:
                        continue

                    state.signal_cooldown[sym] = now_ts
                    state.add_log(
                        f"Signal: {sig['direction']} {sym} ({sig['confidence']}%)",
                        symbol=sym, status="success",
                        details=f"Entry ₹{sig['entry']:.2f} | {', '.join(sig['reasons'][:2])}",
                    )
                    if bot_instance:
                        await send_signal_to_telegram(bot_instance, sig)

            except Exception as e:
                log.error(f"Greeks scan error for {underlying}: {e}")

            await asyncio.sleep(2)  # gap between underlyings

        # Clean up old cooldowns (>10 min)
        now_ts = time.time()
        state.signal_cooldown = {k: v for k, v in state.signal_cooldown.items() if now_ts - v < 600}

        await asyncio.sleep(120)  # scan every 2 minutes

    log.info("Greeks scanner loop stopped")


# ── Quick Trade callback handler ─────────────────────────────────────────────

@tg_router.callback_query(F.data.startswith("sg:t:"))
async def cb_signal_trade(cb: CallbackQuery):
    """Handle Quick Trade button click — place order on follower's account."""
    try:
        parts = cb.data.split(":")
        # sg:t:{symbol}:{txn}:{qty}:{sl}:{tp}
        if len(parts) < 7:
            await cb.answer("Invalid signal data", show_alert=True)
            return

        symbol = parts[2]
        txn_type = parts[3]  # BUY or SELL
        qty = int(parts[4])
        sl_price = float(parts[5])
        tp_price = float(parts[6])

        # Find the follower client for this user
        user_tid = cb.from_user.id
        config = _read_config()
        account_name = None

        # Check if master
        if config.get("master", {}).get("telegram_id") == user_tid:
            # Master can't trade signals on their own account via this button
            # But let's allow it — use first follower or master client
            account_name = config.get("master", {}).get("name")

        # Check followers
        if not account_name:
            for f in config.get("followers", []):
                if f.get("telegram_id") == user_tid:
                    account_name = f.get("name")
                    break

        if not account_name:
            await cb.answer("You don't have a trading account configured", show_alert=True)
            return

        # Get client + config — try live clients first, then authenticate on-the-fly
        client = None
        fc = None
        pair = state.follower_clients.get(account_name)
        if pair:
            client, fc = pair
        elif state.master_cfg and account_name == state.master_cfg.name:
            client = state.master_client
            fc = state.master_cfg
        else:
            # Try on-the-fly auth for disabled followers with API keys
            config_followers = config.get("followers", [])
            fc_data = next((f for f in config_followers if f.get("name") == account_name), None)
            if fc_data and fc_data.get("api_key"):
                try:
                    fc = AccountConfig(**fc_data)
                    client = await asyncio.to_thread(authenticate, fc)
                    state.follower_clients[account_name] = (client, fc)
                    log.info(f"Quick Trade: on-the-fly auth for {account_name}")
                except Exception as e:
                    await cb.answer(f"Auth failed: {e}", show_alert=True)
                    return
            else:
                await cb.answer(f"Account {account_name} not authenticated", show_alert=True)
                return

        if not client or not fc:
            await cb.answer("Trading client not available", show_alert=True)
            return

        await cb.answer("Placing order...", show_alert=False)

        # Build a synthetic order dict — same format as copy trading uses
        exchange_str = "BSE" if "SENSEX" in symbol else "NSE"
        order = {
            "trading_symbol": symbol,
            "quantity": qty,
            "order_type": "MARKET",
            "transaction_type": txn_type,
            "exchange": exchange_str,
            "segment": "FNO",
            "product": "MIS",
            "price": 0,
            "trigger_price": 0,
        }

        # Place main order using same logic as copy trading
        try:
            log.info(f"Quick Trade: placing {txn_type} {qty}x {symbol} for {account_name}")
            resp = await asyncio.to_thread(
                copy_order_to_follower, client, fc, order
            )
            log.info(f"Quick Trade: order response: {resp}")

            if resp is None:
                await cb.message.reply(f"❌ Order failed — check logs")
                state.add_log(
                    f"Quick Trade FAILED: {symbol}",
                    symbol=symbol, follower=account_name, status="error",
                    details="place_order returned None",
                )
                return

            oid = resp.get("groww_order_id", "unknown")

            # Place OCO smart order (SL + TP as one unit — Groww cancels the other automatically)
            opposite_txn = "SELL" if txn_type == "BUY" else "BUY"
            oco_resp = None
            oco_id = "failed"

            exchange_const = client.EXCHANGE_BSE if exchange_str == "BSE" else client.EXCHANGE_NSE
            try:
                oco_resp = await asyncio.to_thread(
                    client.create_smart_order,
                    smart_order_type=client.SMART_ORDER_TYPE_OCO,
                    segment=client.SEGMENT_FNO,
                    trading_symbol=symbol,
                    quantity=qty,
                    product_type=client.PRODUCT_MIS,
                    exchange=exchange_const,
                    duration=client.VALIDITY_DAY,
                    transaction_type=opposite_txn,
                    net_position_quantity=qty if txn_type == "BUY" else -qty,
                    target={
                        "trigger_price": f"{tp_price:.2f}",
                        "order_type": "LIMIT",
                        "price": f"{tp_price:.2f}",
                    },
                    stop_loss={
                        "trigger_price": f"{sl_price:.2f}",
                        "order_type": "STOP_LOSS",
                        "price": f"{sl_price * 0.95:.2f}" if opposite_txn == "SELL" else f"{sl_price * 1.05:.2f}",
                    },
                )
                oco_id = oco_resp.get("smart_order_id", oco_resp.get("id", "unknown")) if oco_resp else "failed"
                log.info(f"OCO placed for {symbol}: {oco_resp}")
            except Exception as oco_err:
                log.warning(f"OCO order failed for {symbol}: {oco_err}, falling back to separate SL")
                # Fallback: place SL only (no TP) — SL-M blocked for BSE options
                sl_limit = sl_price * 0.95 if opposite_txn == "SELL" else sl_price * 1.05
                sl_order = {
                    "trading_symbol": symbol,
                    "quantity": qty,
                    "order_type": "SL",
                    "transaction_type": opposite_txn,
                    "exchange": exchange_str,
                    "segment": "FNO",
                    "product": "MIS",
                    "price": round(sl_limit, 2),
                    "trigger_price": sl_price,
                }
                try:
                    sl_resp = await asyncio.to_thread(
                        copy_order_to_follower, client, fc, sl_order
                    )
                    oco_id = f"SL-only: {sl_resp.get('groww_order_id', 'failed')}" if sl_resp else "SL-failed"
                except Exception:
                    oco_id = "SL-failed"

            result_text = (
                f"✅ <b>Order Placed!</b>\n\n"
                f"Main: {txn_type} {qty}x {symbol}\n"
                f"Order ID: <code>{oid}</code>\n"
                f"OCO: SL ₹{sl_price:.1f} / TP ₹{tp_price:.1f}\n"
                f"OCO ID: <code>{oco_id}</code>"
            )
            await cb.message.reply(result_text, parse_mode="HTML")

            state.add_log(
                f"Quick Trade: {txn_type} {qty}x {symbol}",
                symbol=symbol, follower=account_name, status="success",
                details=f"OID={oid}, OCO={oco_id}",
            )

        except Exception as e:
            log.error(f"Quick Trade exception: {e}", exc_info=True)
            await cb.message.reply(f"❌ Order failed: {e}")
            state.add_log(
                f"Quick Trade FAILED: {symbol}",
                symbol=symbol, follower=account_name, status="error",
                details=str(e),
            )

    except Exception as e:
        log.error(f"Signal trade callback error: {e}")
        await cb.answer(f"Error: {e}", show_alert=True)


@tg_router.callback_query(F.data.startswith("sg:s:"))
async def cb_signal_skip(cb: CallbackQuery):
    """Handle Skip button — just acknowledge."""
    await cb.answer("Signal skipped")
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


# ── Safe API wrappers ─────────────────────────────────────────────────────────

def safe_api_call(func, *args, default=None):
    try:
        result = func(*args)
        return result if result else default
    except Exception as e:
        log.warning(f"API call failed: {e}")
        return default


def get_client_for(name: str) -> GrowwAPI | None:
    if state.master_cfg and name == state.master_cfg.name:
        return state.master_client
    pair = state.follower_clients.get(name)
    return pair[0] if pair else None


def mask(s: str) -> str:
    if not s or len(s) < 8:
        return "••••••••"
    return s[:4] + "•" * (len(s) - 8) + s[-4:]


# ── FastAPI app ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    state._loop = asyncio.get_running_loop()
    load_config_and_auth()
    log.info("Config loaded, accounts authenticated")

    # Start Telegram bot as background task
    config = _read_config()
    bot_token = config.get("telegram_bot_token", "")
    bot_task = None
    dp = None
    bot = None
    if bot_token and bot_token != "YOUR_BOT_TOKEN":
        try:
            bot = Bot(token=bot_token)
            dp = Dispatcher()
            dp.include_router(tg_router)
            bot_task = asyncio.create_task(dp.start_polling(bot))
            state.bot_instance = bot
            log.info("Telegram bot started")
        except Exception as e:
            log.error(f"Telegram bot failed to start: {e}")
    else:
        log.warning("No telegram_bot_token configured, bot disabled")

    # Start OI Analytics if enabled
    oi_cfg = _get_oi_config()
    if oi_cfg.get("enabled") and state.master_client:
        state.oi_running = True
        state.oi_task = asyncio.create_task(oi_polling_loop())
        log.info(f"OI Analytics started for: {oi_cfg.get('instruments', [])}")

    # Start Greeks Scanner if enabled in config
    greeks_cfg = _read_config().get("greeks_scanner", {})
    if greeks_cfg.get("enabled", False) and state.master_client and bot:
        state.greeks_scanner_running = True
        state.greeks_scanner_task = asyncio.create_task(greeks_scanner_loop(bot))
        log.info("Greeks scanner started")

    # Start hourly scan for followers (always on if bot + master connected)
    if state.master_client and bot:
        state.hourly_scan_running = True
        state.hourly_scan_task = asyncio.create_task(hourly_scan_loop(bot))
        log.info("Hourly scan loop started")

    yield

    state.running = False
    if state.copier_task and not state.copier_task.done():
        state.copier_task.cancel()
    state.copier_state.save()
    # Stop OI polling
    state.oi_running = False
    if state.oi_task and not state.oi_task.done():
        state.oi_task.cancel()
    # Stop Greeks scanner
    state.greeks_scanner_running = False
    if state.greeks_scanner_task and not state.greeks_scanner_task.done():
        state.greeks_scanner_task.cancel()
    # Stop hourly scan
    state.hourly_scan_running = False
    if state.hourly_scan_task and not state.hourly_scan_task.done():
        state.hourly_scan_task.cancel()
    if dp and bot:
        await dp.stop_polling()
        await bot.session.close()
    if bot_task:
        bot_task.cancel()


app = FastAPI(title="Groww Trade Copier", lifespan=lifespan)
app.add_middleware(AuthMiddleware)


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_PATH.read_text(encoding="utf-8")


@app.get("/oi", response_class=HTMLResponse)
async def oi_page():
    return OI_PATH.read_text(encoding="utf-8")


# ── Auth Endpoints ───────────────────────────────────────────────────────────

@app.get("/auth")
async def auth_callback(token: str):
    """Validate one-time login token, set JWT cookie, redirect to dashboard."""
    data = consume_login_token(token)
    if not data:
        return HTMLResponse(
            "<html><body style='background:#0B0E11;color:#F6465D;font-family:sans-serif;"
            "display:flex;justify-content:center;align-items:center;height:100vh'>"
            "<h2>Invalid or expired login link. Send /login again.</h2></body></html>",
            status_code=401,
        )
    jwt_token = create_jwt(data["telegram_id"], data["name"], data["role"])
    # Return 200 HTML page that sets cookie then redirects via JS
    # (some browsers drop Set-Cookie on 302 redirects over plain HTTP)
    response = HTMLResponse(
        "<html><body style='background:#0B0E11;color:#EAECEF;font-family:sans-serif;"
        "display:flex;justify-content:center;align-items:center;height:100vh'>"
        "<h2>Logging in...</h2>"
        "<script>setTimeout(function(){window.location.href='/';},500);</script>"
        "</body></html>"
    )
    response.set_cookie(
        key="session",
        value=jwt_token,
        max_age=7 * 24 * 3600,  # 7 days
        httponly=True,
        samesite="lax",
        path="/",
    )
    return response


@app.get("/api/me")
async def api_me(request: Request):
    """Return current user info or 401."""
    user = get_current_user(request)
    if not user:
        return JSONResponse({"detail": "Not authenticated"}, status_code=401)
    return {"telegram_id": user["sub"], "name": user["name"], "role": user["role"]}


@app.get("/api/logout")
async def api_logout():
    """Clear session cookie and redirect to /."""
    response = RedirectResponse(url="/", status_code=302)
    response.delete_cookie("session")
    return response


# ── Status ────────────────────────────────────────────────────────────────────

@app.get("/api/status")
async def api_status(request: Request):
    user = request.state.user
    uptime = time.time() - state.start_time if state.start_time else 0
    result = {
        "running": state.running,
        "dry_run": state.dry_run,
        "feed_mode": state.feed_mode,
        "uptime_seconds": round(uptime),
        "poll_interval": state.poll_interval,
        "total_copied": state.total_copied,
        "total_failed": state.total_failed,
        "master_connected": state.master_client is not None,
        "active_followers": len(state.follower_clients),
    }
    return result


# ── Master endpoints ─────────────────────────────────────────────────────────

@app.get("/api/master/orders")
async def master_orders(request: Request):
    user = request.state.user
    if user["role"] != "master":
        raise HTTPException(403, "Master only")
    if not state.master_client:
        return []
    raw = await asyncio.to_thread(safe_api_call, state.master_client.get_order_list, default={})
    orders = unwrap_orders(raw)
    return [o for o in orders if o.get("segment", "").upper() in ("FNO", "COMMODITY")]


@app.get("/api/master/positions")
async def master_positions(request: Request):
    user = request.state.user
    if user["role"] != "master":
        raise HTTPException(403, "Master only")
    if not state.master_client:
        return []
    raw = await asyncio.to_thread(safe_api_call, state.master_client.get_positions_for_user, default={})
    return unwrap_positions(raw)


# ── Followers ─────────────────────────────────────────────────────────────────

@app.get("/api/followers")
async def followers_list(request: Request):
    user = request.state.user
    if user["role"] != "master":
        raise HTTPException(403, "Master only")
    result = []
    for fc in state.follower_cfgs:
        pair = state.follower_clients.get(fc.name)
        result.append({
            "name": fc.name,
            "lot_mode": fc.lot_mode,
            "lot_multiplier": fc.lot_multiplier,
            "lot_fixed": fc.lot_fixed,
            "enabled": fc.enabled,
            "authenticated": pair is not None,
        })
    return result


@app.get("/api/followers/{name}/positions")
async def follower_positions(name: str, request: Request):
    user = request.state.user
    # Only master can view, or follower can view their own
    if user["role"] != "master" and user["name"] != name:
        raise HTTPException(403, "Forbidden")
    client = get_client_for(name)
    if not client:
        return []
    raw = await asyncio.to_thread(safe_api_call, client.get_positions_for_user, default={})
    return unwrap_positions(raw)


# ── Account endpoints (works for master + followers) ──────────────────────────

def _check_account_access(user: dict, name: str):
    """Ensure user can only access their own account data."""
    if user["name"] != name:
        raise HTTPException(403, "Forbidden — you can only access your own account")


@app.get("/api/account/{name}/margin")
async def account_margin(name: str, request: Request):
    _check_account_access(request.state.user, name)
    client = get_client_for(name)
    if not client:
        return {}
    return await asyncio.to_thread(safe_api_call, client.get_available_margin_details, default={})


@app.get("/api/account/{name}/positions")
async def account_positions(name: str, request: Request):
    _check_account_access(request.state.user, name)
    client = get_client_for(name)
    if not client:
        return []
    raw = await asyncio.to_thread(safe_api_call, client.get_positions_for_user, default={})
    return unwrap_positions(raw)


@app.get("/api/account/{name}/orders")
async def account_orders(name: str, request: Request):
    _check_account_access(request.state.user, name)
    client = get_client_for(name)
    if not client:
        return []
    raw = await asyncio.to_thread(safe_api_call, client.get_order_list, default={})
    return unwrap_orders(raw)


# ── Indices ───────────────────────────────────────────────────────────────────

# Major indices for REST fallback (token, exchange, segment, display_name)
MAJOR_INDICES = [
    ("NIFTY", "NSE", "CASH", "NIFTY 50"),
    ("BANKNIFTY", "NSE", "CASH", "NIFTY Bank"),
    ("SENSEX", "BSE", "CASH", "SENSEX"),
    ("FINNIFTY", "NSE", "CASH", "Fin Nifty"),
    ("NIFTYJR", "NSE", "CASH", "Nifty Next 50"),
    ("NIFTY100", "NSE", "CASH", "NIFTY 100"),
    ("NIFTY500", "NSE", "CASH", "NIFTY 500"),
    ("NIFTYMIDCAP150", "NSE", "CASH", "Nifty Midcap 150"),
    ("NIFTYMIDSELECT", "NSE", "CASH", "Nifty Midcap Select"),
    ("NIFTYSMALLCAP250", "NSE", "CASH", "Nifty Smallcap 250"),
    ("INDIAVIX", "NSE", "CASH", "India VIX"),
    ("NIFTYIT", "NSE", "CASH", "NIFTY IT"),
    ("NIFTYPHARMA", "NSE", "CASH", "NIFTY Pharma"),
    ("NIFTYFMCG", "NSE", "CASH", "NIFTY FMCG"),
    ("NIFTYMETAL", "NSE", "CASH", "NIFTY Metal"),
    ("NIFTYAUTO", "NSE", "CASH", "NIFTY Auto"),
    ("NIFTYREALTY", "NSE", "CASH", "NIFTY Realty"),
    ("NIFTYPVTBANK", "NSE", "CASH", "NIFTY Pvt Bank"),
    ("NIFTYPSUBANK", "NSE", "CASH", "NIFTY PSU Bank"),
    ("NIFTYMEDIA", "NSE", "CASH", "NIFTY Media"),
    ("NIFTYCDTY", "NSE", "CASH", "NIFTY Commodities"),
    ("NIFTYMIDCAP", "NSE", "CASH", "Nifty Midcap 100"),
    ("NIFTYSMALL", "NSE", "CASH", "Nifty Smallcap 100"),
    ("NIFTYTOTALMCAP", "NSE", "CASH", "Nifty Total Market"),
    ("BANKEX", "BSE", "CASH", "BSE Bankex"),
    ("BSE100", "BSE", "CASH", "BSE 100"),
    ("BSEMIDCAP", "BSE", "CASH", "BSE Midcap"),
    ("BSESMLCAP", "BSE", "CASH", "BSE Smallcap"),
]


def fetch_indices_ohlc(tokens: list[str]) -> dict[str, dict]:
    """Fetch OHLC data via REST get_quote() only for requested tokens."""
    if not state.master_client or not tokens:
        return {}
    # Build lookup: token → (symbol, exchange, segment, name)
    index_lookup = {sym: (sym, ex, seg, name) for sym, ex, seg, name in MAJOR_INDICES}
    results = {}
    for token in tokens:
        info = index_lookup.get(token)
        if not info:
            continue
        symbol, exchange, segment, name = info
        try:
            q = state.master_client.get_quote(symbol, exchange, segment)
            if q and q.get("last_price"):
                results[token] = {
                    "name": name,
                    "exchange": exchange,
                    "token": token,
                    "ltp": q.get("last_price"),
                    "change": q.get("day_change", 0),
                    "changePercent": q.get("day_change_perc", 0),
                    "open": q.get("ohlc", {}).get("open"),
                    "high": q.get("ohlc", {}).get("high"),
                    "low": q.get("ohlc", {}).get("low"),
                    "close": q.get("ohlc", {}).get("close"),
                    "high52": q.get("week_52_high"),
                    "low52": q.get("week_52_low"),
                }
        except Exception:
            continue
    return results


@app.get("/api/indices")
async def get_indices(pinned: str = ""):
    """Return index data for pinned tokens only.
    Feed provides live LTP, REST provides OHLC — merged together.
    Query param: ?pinned=NIFTY,BANKNIFTY,SENSEX"""
    tokens = [t.strip() for t in pinned.split(",") if t.strip()] if pinned else []

    if not tokens:
        # No pinned list sent — return feed data as-is (no OHLC)
        if state.index_data:
            return list(state.index_data.values())
        return []

    # Fetch OHLC only for pinned tokens (cached per request cycle)
    ohlc_cache = getattr(state, '_indices_ohlc_cache', {})
    # Check if we need to refresh — fetch missing tokens or if cache is stale
    missing = [t for t in tokens if t not in ohlc_cache]
    cache_age = time.time() - getattr(state, '_ohlc_cache_time', 0)
    if missing or cache_age > 300:  # refresh every 5 min
        new_data = await asyncio.to_thread(fetch_indices_ohlc, tokens)
        if new_data:
            if not hasattr(state, '_indices_ohlc_cache'):
                state._indices_ohlc_cache = {}
            state._indices_ohlc_cache.update(new_data)
            state._ohlc_cache_time = time.time()
            ohlc_cache = state._indices_ohlc_cache
            log.info(f"Refreshed OHLC for {len(new_data)} pinned indices")

    # Merge: feed data (live LTP) + REST data (OHLC)
    indices = []
    for token in tokens:
        feed_d = state.index_data.get(token, {})
        rest_d = ohlc_cache.get(token, {})
        if not feed_d and not rest_d:
            continue
        merged = {**rest_d, **feed_d}  # feed overwrites LTP/change, REST fills OHLC
        # Ensure OHLC comes from REST
        for key in ("open", "high", "low", "close", "high52", "low52"):
            if rest_d.get(key):
                merged[key] = rest_d[key]
        indices.append(merged)
    return indices


# ── Copy log ──────────────────────────────────────────────────────────────────

@app.get("/api/copy-log")
async def copy_log(request: Request):
    user = request.state.user
    logs = state.copy_log[-100:]
    if user["role"] != "master":
        # Followers only see log entries related to their name
        name = user["name"]
        logs = [l for l in logs if l.get("follower") == name or not l.get("follower")]
    return logs


# ── Copier controls ──────────────────────────────────────────────────────────

def _require_master(request: Request):
    user = request.state.user
    if user["role"] != "master":
        raise HTTPException(403, "Master only")


@app.post("/api/copier/start")
async def copier_start(request: Request):
    _require_master(request)
    if state.running:
        return {"message": "Already running"}
    if not state.master_client:
        raise HTTPException(400, "Master account not authenticated")
    if not state.follower_clients:
        log.warning("No followers authenticated — copier will detect orders but not copy")

    # Snapshot existing master orders so we only copy NEW orders placed after start
    existing_orders = await asyncio.to_thread(fetch_master_orders, state.master_client)
    skipped = 0
    for o in existing_orders:
        oid = o.get("groww_order_id")
        if oid and oid not in state.copier_state.copied_order_ids:
            state.copier_state.copied_order_ids.add(oid)
            skipped += 1
    if skipped:
        state.copier_state.save()
        log.info(f"Skipped {skipped} pre-existing orders on startup")
        state.add_log(
            f"Skipped {skipped} pre-existing orders",
            status="info",
            details="Only new orders placed after start will be copied",
        )

    state.running = True
    state.start_time = time.time()
    state.copier_task = asyncio.create_task(copier_loop())
    return {"message": "Copier started"}


@app.post("/api/copier/stop")
async def copier_stop(request: Request):
    _require_master(request)
    if not state.running:
        return {"message": "Already stopped"}
    state.running = False
    state.start_time = None
    if state.copier_task:
        state.copier_task.cancel()
        state.copier_task = None
    return {"message": "Copier stopped"}


@app.post("/api/copier/dry-run")
async def toggle_dry_run(request: Request):
    _require_master(request)
    state.dry_run = not state.dry_run
    mode = "ON" if state.dry_run else "OFF"
    state.add_log(f"Dry run mode {mode}", status="warning")
    return {"dry_run": state.dry_run, "message": f"Dry run {mode}"}


@app.post("/api/copier/test")
async def test_copy_pipeline(request: Request):
    """Simulate a master order to test the full copy pipeline (always dry run)."""
    _require_master(request)
    fake_order = {
        "groww_order_id": f"TEST_{int(time.time())}",
        "trading_symbol": "NIFTY26FEB25000CE",
        "order_status": "EXECUTED",
        "quantity": 75,
        "price": 150.0,
        "trigger_price": 0,
        "order_type": "LIMIT",
        "transaction_type": "BUY",
        "exchange": "NSE",
        "segment": "FNO",
        "product": "NRML",
    }
    oid = fake_order["groww_order_id"]
    symbol = fake_order["trading_symbol"]

    state.add_log(
        f"TEST: BUY 75x {symbol} @ 150.0 [EXECUTED]",
        symbol=symbol, status="info", details="simulated test order",
    )

    results = []
    for name, (client, fc) in state.follower_clients.items():
        # Force dry run for test
        old_dry = state.dry_run
        state.dry_run = True
        resp = copy_order_to_follower(client, fc, fake_order)
        state.dry_run = old_dry
        if resp:
            state.add_log(
                f"TEST copied to {name}",
                symbol=symbol, follower=name, status="success",
                details="DRY_RUN test",
            )
            results.append({"follower": name, "status": "ok", "response": resp})
        else:
            state.add_log(
                f"TEST failed for {name}",
                symbol=symbol, follower=name, status="error",
            )
            results.append({"follower": name, "status": "failed"})

    if not state.follower_clients:
        state.add_log("TEST: No authenticated followers to copy to", status="warning")

    return {
        "test_order": fake_order,
        "followers_tested": len(results),
        "results": results,
        "note": "No followers authenticated" if not results else "Dry run test completed",
    }


@app.post("/api/master/test-order")
async def place_test_order(request: Request):
    """Place a tiny LIMIT order on master at an absurdly low price (won't execute).
    Used to test if the WebSocket feed detects the order. Cancel it after."""
    _require_master(request)
    if not state.master_client:
        raise HTTPException(400, "Master not authenticated")

    try:
        resp = await asyncio.to_thread(
            state.master_client.place_order,
            trading_symbol="SENSEX26FEB82000PE",
            quantity=20,  # 2 lots SENSEX
            validity=state.master_client.VALIDITY_DAY,
            exchange=state.master_client.EXCHANGE_BSE,
            segment=state.master_client.SEGMENT_FNO,
            product=state.master_client.PRODUCT_NRML,
            order_type=state.master_client.ORDER_TYPE_LIMIT,
            transaction_type=state.master_client.TRANSACTION_TYPE_BUY,
            price=0.05,  # ₹0.05 — will never execute
        )
        oid = resp.get("groww_order_id", "unknown")
        state.add_log(
            f"TEST ORDER placed: BUY 10x SENSEX26FEB82000PE @ ₹0.05",
            symbol="SENSEX26FEB82000PE", status="info",
            details=f"Order ID: {oid} — waiting for feed to detect it...",
        )
        return {"status": "ok", "order_id": oid, "response": resp}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/api/master/cancel-order/{order_id}")
async def cancel_test_order(order_id: str, request: Request):
    """Cancel an order on the master account."""
    _require_master(request)
    if not state.master_client:
        raise HTTPException(400, "Master not authenticated")

    try:
        resp = await asyncio.to_thread(
            state.master_client.cancel_order, order_id,
            segment=state.master_client.SEGMENT_FNO,
        )
        state.add_log(
            f"Order cancelled: {order_id}",
            status="warning", details=str(resp),
        )
        return {"status": "ok", "response": resp}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ── OI Analytics Endpoints ───────────────────────────────────────────────────

@app.get("/api/oi/status")
async def oi_status(request: Request):
    _require_master(request)
    oi_cfg = _get_oi_config()
    return {
        "running": state.oi_running,
        "last_poll": state.oi_last_poll,
        "instruments": list(state.oi_analysis.keys()),
        "config": oi_cfg,
    }


@app.get("/api/oi/analysis")
async def oi_analysis_all(request: Request):
    _require_master(request)
    result = {}
    for underlying, analysis in state.oi_analysis.items():
        has_prev = underlying in state.oi_snapshots and len(state.oi_snapshots[underlying]) >= 2
        result[underlying] = {
            **asdict(analysis),
            "has_prev_data": has_prev,
        }
    return result


@app.get("/api/oi/analysis/{underlying}")
async def oi_analysis_single(underlying: str, request: Request):
    _require_master(request)
    analysis = state.oi_analysis.get(underlying.upper())
    if not analysis:
        raise HTTPException(404, f"No analysis for {underlying}")
    has_prev = underlying.upper() in state.oi_snapshots and len(state.oi_snapshots[underlying.upper()]) >= 2
    return {**asdict(analysis), "has_prev_data": has_prev}


@app.post("/api/oi/start")
async def oi_start(request: Request):
    _require_master(request)
    if state.oi_running:
        return {"message": "Already running"}
    if not state.master_client:
        raise HTTPException(400, "Master not authenticated")
    state.oi_running = True
    state.oi_task = asyncio.create_task(oi_polling_loop())
    return {"message": "OI Analytics started"}


@app.post("/api/oi/stop")
async def oi_stop(request: Request):
    _require_master(request)
    if not state.oi_running:
        return {"message": "Already stopped"}
    state.oi_running = False
    if state.oi_task and not state.oi_task.done():
        state.oi_task.cancel()
        state.oi_task = None
    return {"message": "OI Analytics stopped"}


@app.put("/api/settings/oi-analytics")
async def update_oi_settings(body: dict, request: Request):
    _require_master(request)
    config = _read_config()
    oi_cfg = config.get("oi_analytics", {})
    for key in ("enabled", "instruments", "poll_interval_seconds", "unusual_oi_threshold"):
        if key in body:
            oi_cfg[key] = body[key]
    config["oi_analytics"] = oi_cfg
    _write_config(config)
    return {"message": "OI Analytics settings saved", "config": oi_cfg}


# ── Greeks Scanner Endpoints ─────────────────────────────────────────────────

@app.get("/api/greeks/status")
async def greeks_status(request: Request):
    _require_master(request)
    config = _read_config()
    return {
        "running": state.greeks_scanner_running,
        "underlyings": list(GREEKS_UNDERLYINGS.keys()),
        "last_scan_keys": list(state.greeks_history.keys()),
        "scan_counts": {k: len(v) for k, v in state.greeks_history.items()},
        "active_cooldowns": len(state.signal_cooldown),
        "config": config.get("greeks_scanner", {"enabled": False}),
    }


@app.post("/api/greeks/start")
async def greeks_start(request: Request):
    _require_master(request)
    if state.greeks_scanner_running:
        return {"message": "Already running"}
    if not state.master_client:
        raise HTTPException(400, "Master not authenticated")

    state.greeks_scanner_running = True
    state.greeks_scanner_task = asyncio.create_task(greeks_scanner_loop(state.bot_instance))
    return {"message": "Greeks scanner started"}


@app.post("/api/greeks/stop")
async def greeks_stop(request: Request):
    _require_master(request)
    if not state.greeks_scanner_running:
        return {"message": "Already stopped"}
    state.greeks_scanner_running = False
    if state.greeks_scanner_task and not state.greeks_scanner_task.done():
        state.greeks_scanner_task.cancel()
        state.greeks_scanner_task = None
    return {"message": "Greeks scanner stopped"}


@app.post("/api/greeks/scan-now")
async def greeks_scan_now(request: Request):
    """Trigger a single Greeks scan immediately (for testing)."""
    _require_master(request)
    if not state.master_client:
        raise HTTPException(400, "Master not authenticated")

    results = {}
    for underlying in GREEKS_UNDERLYINGS:
        try:
            snapshot, chart = await asyncio.gather(
                asyncio.to_thread(scan_greeks_for_underlying, state.master_client, underlying),
                asyncio.to_thread(_get_chart_analysis, underlying),
            )
            if snapshot:
                _, c_score = _chart_bias_score(chart)
                hist = state.greeks_history.setdefault(underlying, [])
                hist.append(snapshot)
                if len(hist) > 5:
                    hist.pop(0)
                signals = generate_greeks_signals(underlying, snapshot, hist, chart_score=c_score)
                results[underlying] = {
                    "strikes_scanned": len(snapshot),
                    "signals": signals,
                    "chart_score": c_score,
                }
        except Exception as e:
            results[underlying] = {"error": str(e)}

    return results


@app.put("/api/settings/greeks-scanner")
async def update_greeks_settings(body: dict, request: Request):
    _require_master(request)
    config = _read_config()
    gs_cfg = config.get("greeks_scanner", {})
    for key in ("enabled",):
        if key in body:
            gs_cfg[key] = body[key]
    config["greeks_scanner"] = gs_cfg
    _write_config(config)
    return {"message": "Greeks scanner settings saved", "config": gs_cfg}


# ── Settings ──────────────────────────────────────────────────────────────────

@app.get("/api/settings")
async def get_settings(request: Request):
    user = request.state.user
    config = json.loads(CONFIG_PATH.read_text())
    master = config.get("master", {})
    followers = config.get("followers", [])

    if user["role"] == "master":
        return {
            "poll_interval_seconds": config.get("poll_interval_seconds", 3),
            "oi_analytics": config.get("oi_analytics", {
                "enabled": False, "instruments": [],
                "poll_interval_seconds": 180, "unusual_oi_threshold": 2.0,
            }),
            "master": {
                "name": master.get("name", ""),
                "api_key": mask(master.get("api_key", "")),
                "secret": mask(master.get("secret", "")),
                "use_totp": master.get("use_totp", False),
                "totp_secret": mask(master.get("totp_secret", "")),
            },
            "followers": [
                {
                    "name": f.get("name", ""),
                    "api_key": mask(f.get("api_key", "")),
                    "secret": mask(f.get("secret", "")),
                    "use_totp": f.get("use_totp", False),
                    "totp_secret": mask(f.get("totp_secret", "")),
                    "lot_mode": f.get("lot_mode", "same"),
                    "lot_multiplier": f.get("lot_multiplier", 1.0),
                    "lot_fixed": f.get("lot_fixed", 1),
                    "enabled": f.get("enabled", True),
                }
                for f in followers
            ],
        }
    else:
        # Follower: only sees their own config
        own = next((f for f in followers if f.get("name") == user["name"]), {})
        return {
            "own": {
                "name": own.get("name", ""),
                "api_key": mask(own.get("api_key", "")),
                "secret": mask(own.get("secret", "")),
                "use_totp": own.get("use_totp", False),
                "totp_secret": mask(own.get("totp_secret", "")),
                "lot_mode": own.get("lot_mode", "same"),
                "lot_multiplier": own.get("lot_multiplier", 1.0),
                "lot_fixed": own.get("lot_fixed", 1),
            },
        }


def save_config(config: dict):
    CONFIG_PATH.write_text(json.dumps(config, indent=4))


@app.put("/api/settings/master")
async def update_master(body: dict, request: Request):
    _require_master(request)
    config = json.loads(CONFIG_PATH.read_text())
    for key in ("name", "api_key", "secret", "use_totp", "totp_secret"):
        if key in body:
            config["master"][key] = body[key]
    save_config(config)

    # Re-auth master
    state.master_cfg = AccountConfig(**config["master"])
    try:
        state.master_client = authenticate(state.master_cfg)
        return {"message": "Master updated and re-authenticated"}
    except Exception as e:
        state.master_client = None
        raise HTTPException(400, f"Auth failed: {e}")


@app.post("/api/settings/followers")
async def add_follower(body: dict, request: Request):
    _require_master(request)
    config = json.loads(CONFIG_PATH.read_text())
    new_follower = {
        "name": body.get("name", f"Follower-{len(config['followers']) + 1}"),
        "api_key": body.get("api_key", ""),
        "secret": body.get("secret", ""),
        "use_totp": body.get("use_totp", False),
        "totp_secret": body.get("totp_secret", ""),
        "lot_mode": body.get("lot_mode", "same"),
        "lot_multiplier": body.get("lot_multiplier", 1.0),
        "lot_fixed": body.get("lot_fixed", 1),
        "enabled": body.get("enabled", True),
    }
    config["followers"].append(new_follower)
    save_config(config)

    fc = AccountConfig(**new_follower)
    state.follower_cfgs.append(fc)
    if fc.enabled:
        try:
            client = authenticate(fc)
            state.follower_clients[fc.name] = (client, fc)
        except Exception as e:
            log.error(f"New follower auth failed: {e}")

    return {"message": f"Follower '{new_follower['name']}' added"}


@app.put("/api/settings/followers/{name}")
async def update_follower(name: str, body: dict, request: Request):
    user = request.state.user
    # Master can edit any follower, follower can only edit themselves
    if user["role"] != "master" and user["name"] != name:
        raise HTTPException(403, "Forbidden")
    config = json.loads(CONFIG_PATH.read_text())
    found = None
    for i, f in enumerate(config["followers"]):
        if f["name"] == name:
            found = i
            break
    if found is None:
        raise HTTPException(404, f"Follower '{name}' not found")

    for key in ("name", "api_key", "secret", "use_totp", "totp_secret", "lot_mode", "lot_multiplier", "lot_fixed", "enabled"):
        if key in body:
            config["followers"][found][key] = body[key]
    save_config(config)

    # Reload this follower
    fc = AccountConfig(**config["followers"][found])
    state.follower_cfgs = [AccountConfig(**f) for f in config["followers"]]

    # Remove old client
    if name in state.follower_clients:
        del state.follower_clients[name]

    if fc.enabled:
        try:
            client = authenticate(fc)
            state.follower_clients[fc.name] = (client, fc)
        except Exception as e:
            log.error(f"Follower re-auth failed: {e}")

    return {"message": f"Follower '{fc.name}' updated"}


@app.delete("/api/settings/followers/{name}")
async def delete_follower(name: str, request: Request):
    _require_master(request)
    config = json.loads(CONFIG_PATH.read_text())
    config["followers"] = [f for f in config["followers"] if f["name"] != name]
    save_config(config)

    state.follower_cfgs = [AccountConfig(**f) for f in config["followers"]]
    if name in state.follower_clients:
        del state.follower_clients[name]

    return {"message": f"Follower '{name}' deleted"}


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    state.ws_clients.add(ws)
    try:
        while True:
            await ws.receive_text()  # keep alive
    except WebSocketDisconnect:
        state.ws_clients.discard(ws)


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8002, reload=False)
