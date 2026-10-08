"""
CGL Indications API — FastAPI backend.

Drop-in replacement for /srv/cgl/main.py.

Design notes:
- Auth: username/password -> long-lived bearer token (default 90 days).
  No refresh-token flow — once a token expires, the client just signs in
  again. This is intentional (kept simple on request).
- Accounts are created only via the manage_users.py CLI on the server —
  there is deliberately no public "sign up" endpoint. Hand out credentials
  to whoever needs access.
- Data storage: a single JSON blob per logical document in a small
  key-value table (kv_store). The whole dashboard state (board +
  checklist) is stored under key "app_state". This mirrors exactly what
  the browser used to keep in localStorage, so the frontend's existing
  data shape didn't need to change — only where it's persisted.
- Concurrency model: last-write-wins on the whole "app_state" document.
  Fine for a small team; if this becomes a problem later (people
  clobbering each other's edits), the next step is splitting app_state
  into smaller documents (per cargo, per voyage) with per-document
  timestamps, or moving to proper row-level CRUD tables.
- MCP server: exposes the board (and knowledge base) as tools any Claude
  chat can call directly, mounted at /mcp on this same app. Auth is a
  single static bearer token (separate from user login tokens), generated
  on first run and stored in data/mcp_token.txt. This matches Claude.ai's
  "None + Request headers" custom-connector auth mode — no OAuth needed.
"""
import hashlib
import hmac
import json
import logging
import math
import os
import re
import secrets
import asyncio
import zlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import websockets
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.responses import JSONResponse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
AISSTREAM_API_KEY = os.environ.get("AISSTREAM_API_KEY")
logger = logging.getLogger("cgl.ais")
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "dashboard.db")
MCP_TOKEN_PATH = os.path.join(DATA_DIR, "mcp_token.txt")
os.makedirs(DATA_DIR, exist_ok=True)
DATABASE_URL = f"sqlite+aiosqlite:///{DB_PATH}"

engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

SESSION_TTL_DAYS = 90
PBKDF2_ITERATIONS = 200_000

# Update this list if the dashboard's hosting URL changes (e.g. new Netlify
# domain, or a custom domain later). Add http://localhost:... entries here
# too while testing locally.
ALLOWED_ORIGINS = [
    "https://indications-cgl.netlify.app",
    "https://meek-madeleine-d0ada5.netlify.app",
    "https://joyful-cat-d15faf.netlify.app",
    "http://localhost:5500",
    "http://localhost:3000",
    "http://127.0.0.1:5500",
]

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        salt TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        username TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS kv_store (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        updated_by TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS documents (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        source TEXT,
        category TEXT,
        created_at TEXT NOT NULL,
        created_by TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS vessels (
        imo TEXT PRIMARY KEY,
        mmsi TEXT,
        name TEXT,
        ais_type TEXT,
        manual_type TEXT,
        dwt REAL,
        manual_dwt REAL,
        dwt_source TEXT,
        loa REAL,
        max_draught REAL,
        destination TEXT,
        eta TEXT,
        lat REAL,
        lon REAL,
        sog REAL,
        last_seen TEXT,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_vessels_mmsi ON vessels (mmsi)
    """,
    """
    CREATE TABLE IF NOT EXISTS lion_regions (
        id TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        sort_order INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS lion_positions (
        id TEXT PRIMARY KEY,
        vessel_name TEXT NOT NULL,
        name_key TEXT NOT NULL,
        built INTEGER,
        dwt REAL,
        position TEXT,
        open_from TEXT,
        open_to TEXT,
        comments TEXT,
        region TEXT,
        region_manual INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_lion_positions_name ON lion_positions (name_key)
    """,
    """
    CREATE TABLE IF NOT EXISTS wa_vessels (
        id TEXT PRIMARY KEY,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        name TEXT,
        imo TEXT,
        mmsi TEXT,
        build_year INTEGER,
        flag TEXT,
        dwt REAL,
        dwcc REAL,
        intake_mt REAL,
        grain_cbft REAL,
        bale_cbft REAL,
        draft_m REAL,
        holds TEXT,
        gear TEXT,
        position TEXT,
        region TEXT,
        open_from TEXT,
        open_to TEXT,
        restrictions TEXT,
        freight_idea TEXT,
        contact TEXT,
        broker_company TEXT,
        status TEXT NOT NULL DEFAULT 'open',
        source_chat TEXT,
        source_sender TEXT,
        source_date TEXT,
        raw_text TEXT,
        note TEXT,
        match_key TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_wa_vessels_match_key ON wa_vessels (match_key)
    """,
    """
    CREATE TABLE IF NOT EXISTS state_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        updated_at TEXT NOT NULL,
        updated_by TEXT,
        size INTEGER,
        value BLOB NOT NULL
    )
    """,
]

# ── AIS region of interest (Black Sea + Azov + lower Danube + Mediterranean
# + Red Sea) — the broker's usual working area. AISStream wants
# [[lat_min, lon_min], [lat_max, lon_max]] boxes; kept as a couple of
# rectangles rather than one huge box so the subscription stays reasonably
# tight (fewer irrelevant vessels streamed).
AIS_BOUNDING_BOXES = [
    [[40.0, 27.0], [47.5, 42.0]],   # Box 1: Black Sea + Azov + Marmara + straits (Danube, Ukraine, Romania, Bulgaria, N.Turkey)
    [[31.0, 14.0], [41.0, 36.5]],   # Box 2: Central + Eastern Mediterranean (Malta east through the Levant, up to the Suez approach)
    # Deliberately NOT one box spanning both seas (wastes traffic over the Balkans/Turkey land mass in between),
    # and deliberately NOT extending south of 31.0N (that's the Suez Canal / Red Sea — out of scope for now).
]

# Reference ports used for "nearest port" / distance-to-port on the AIS tab —
# the load/discharge ports that already appear on the rate board.
REFERENCE_PORTS = [
    ("Izmail", 45.3547, 28.8419),
    ("Reni", 45.4547, 28.2444),
    ("Kiliya", 45.4517, 29.2669),
    ("Orlivka", 45.4064, 29.5219),
    ("Giurgiulesti", 45.4589, 28.1897),
    ("Constanta", 44.1733, 28.6383),
    ("Varna", 43.2000, 27.9500),
    ("Burgas", 42.4800, 27.4800),
    ("Sulina", 45.1500, 29.6700),
    ("Odesa", 46.4900, 30.7500),
    ("Chornomorsk", 46.3000, 30.6700),
    ("Pivdennyi", 46.6200, 31.0200),
    ("Novorossiysk", 44.7200, 37.7800),
    ("Trabzon", 41.0000, 39.7300),
    ("Izmir", 38.4400, 27.1400),
    ("Galati", 45.4353, 28.0453),
    ("Braila", 45.2692, 27.9575),
    ("Marmara (Istanbul)", 41.0082, 28.9784),
    ("Bandirma", 40.3500, 27.9667),
    ("Samsun", 41.2867, 36.3300),
    ("Mersin", 36.8000, 34.6333),
    ("Iskenderun", 36.5833, 36.1667),
    ("Poti", 42.1500, 41.6667),
    ("EC Greece (Thessaloniki)", 40.6403, 22.9439),
    ("WC Greece (Patras)", 38.2466, 21.7346),
    ("Famagusta", 35.1167, 33.9500),
    ("Beirut", 33.9000, 35.5167),
    ("Tartus", 34.8833, 35.8833),
    ("Latakia", 35.5167, 35.7833),
    ("Limassol", 34.6753, 33.0453),
    ("Port Said", 31.2653, 32.3019),
    ("Alexandria", 31.2001, 29.9187),
    ("El Arish", 31.1300, 33.8000),
    ("Egypt Med (Damietta)", 31.4167, 31.8167),
    ("Tunisia (Sfax)", 34.7333, 10.7333),
    ("EC Italy (Bari)", 41.1333, 16.8667),
    ("Spain Med (Valencia)", 39.4667, -0.3167),
    ("Aqaba", 29.5267, 35.0078),
    ("Gdansk", 54.3667, 18.6667),
]


def haversine_nm(lat1, lon1, lat2, lon2):
    r_nm = 3440.065  # earth radius in nautical miles
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return r_nm * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def nearest_port(lat, lon):
    if lat is None or lon is None:
        return None, None
    best_name, best_dist = None, None
    for name, plat, plon in REFERENCE_PORTS:
        d = haversine_nm(lat, lon, plat, plon)
        if best_dist is None or d < best_dist:
            best_name, best_dist = name, d
    return best_name, round(best_dist, 1) if best_dist is not None else None

# One-off migration for DBs created before the "category" column existed.
MIGRATION_STATEMENTS = [
    "ALTER TABLE documents ADD COLUMN category TEXT",
    "ALTER TABLE vessels ADD COLUMN manual_dwt REAL",
    "ALTER TABLE vessels ADD COLUMN loa REAL",
    "ALTER TABLE vessels ADD COLUMN max_draught REAL",
    "ALTER TABLE vessels ADD COLUMN destination TEXT",
    "ALTER TABLE vessels ADD COLUMN eta TEXT",
    "ALTER TABLE vessels ADD COLUMN dwt_source TEXT",
]


# ── password hashing (stdlib only — no extra dependency needed) ──
def hash_password(password: str, salt: Optional[str] = None) -> Tuple[str, str]:
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), PBKDF2_ITERATIONS)
    return dk.hex(), salt


def verify_password(password: str, salt: str, expected_hash: str) -> bool:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), PBKDF2_ITERATIONS)
    return hmac.compare_digest(dk.hex(), expected_hash)


# ── shared app-state helpers (used by both the REST API and the MCP tools) ──
async def load_app_state() -> dict:
    async with SessionLocal() as db:
        row = (await db.execute(text("SELECT value FROM kv_store WHERE key='app_state'"))).first()
        return json.loads(row.value) if row else {}


HISTORY_KEEP = 200


async def save_app_state(state: dict, actor: str) -> str:
    now = datetime.now(timezone.utc).isoformat()
    payload = json.dumps(state)
    async with SessionLocal() as db:
        await db.execute(
            text(
                """
                INSERT INTO kv_store (key, value, updated_at, updated_by)
                VALUES ('app_state', :v, :t, :u)
                ON CONFLICT(key) DO UPDATE SET value=:v, updated_at=:t, updated_by=:u
                """
            ),
            {"v": payload, "t": now, "u": actor},
        )
        # Keep the last HISTORY_KEEP saves (compressed) so an overwrite can be traced and undone.
        await db.execute(
            text("INSERT INTO state_history (updated_at, updated_by, size, value) VALUES (:t, :u, :s, :z)"),
            {"t": now, "u": actor, "s": len(payload), "z": zlib.compress(payload.encode("utf-8"), 6)},
        )
        await db.execute(
            text("DELETE FROM state_history WHERE id <= (SELECT MAX(id) FROM state_history) - :keep"),
            {"keep": HISTORY_KEEP},
        )
        await db.commit()
    return now


def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%d.%m.%y")


# ── AIS: vessel upserts + the background AISStream worker ──
def sanitize_text(s):
    """AIS ship-name fields occasionally carry malformed/mis-encoded bytes
    from the source transponder (lone UTF-16 surrogates in particular),
    which Python's json.loads happily accepts into a str but SQLite's C API
    then refuses to write. Strip anything that can't round-trip through
    UTF-8 rather than letting it crash the whole write (and, transitively,
    every other query sharing the connection)."""
    if s is None:
        return None
    return s.encode("utf-8", "ignore").decode("utf-8").strip() or None


async def upsert_vessel_position(imo_hint, mmsi, lat, lon, sog, ts):
    """Position reports (AIS types 1/2/3) carry MMSI but never IMO — IMO
    only arrives via a separate ShipStaticData message (type 5), sent much
    less often. So: use imo_hint if the message happened to carry one
    (rare), otherwise resolve this MMSI to the IMO already on file from an
    earlier static-data message. If we don't know this vessel's IMO yet at
    all, skip for now — its position will start recording as soon as its
    static data comes through (usually within a few minutes)."""
    if not mmsi:
        return
    now = datetime.now(timezone.utc).isoformat()
    async with SessionLocal() as db:
        imo = str(imo_hint) if imo_hint and str(imo_hint) not in ("0", "None") else None
        if not imo:
            row = (await db.execute(text("SELECT imo FROM vessels WHERE mmsi=:mmsi"), {"mmsi": str(mmsi)})).first()
            if not row:
                return
            imo = row.imo
        await db.execute(
            text(
                """
                INSERT INTO vessels (imo, mmsi, lat, lon, sog, last_seen, updated_at)
                VALUES (:imo, :mmsi, :lat, :lon, :sog, :ts, :now)
                ON CONFLICT(imo) DO UPDATE SET
                    mmsi=COALESCE(:mmsi, mmsi), lat=:lat, lon=:lon, sog=:sog,
                    last_seen=:ts, updated_at=:now
                """
            ),
            {"imo": imo, "mmsi": str(mmsi), "lat": lat, "lon": lon, "sog": sog, "ts": ts, "now": now},
        )
        await db.commit()


def format_ais_eta(eta_obj):
    """AIS ETA is Month/Day/Hour/Minute with no year (20-bit field per
    ITU-R M.1371) — AISStream hands it back as an object. Month=0 or Day=0
    means "not available" per spec, not January 0th."""
    if not eta_obj or not isinstance(eta_obj, dict):
        return None
    month = eta_obj.get("Month", eta_obj.get("month"))
    day = eta_obj.get("Day", eta_obj.get("day"))
    hour = eta_obj.get("Hour", eta_obj.get("hour"))
    minute = eta_obj.get("Minute", eta_obj.get("minute"))
    try:
        month, day = int(month), int(day)
        if month == 0 or day == 0:
            return None
        hour = int(hour) if hour is not None else 0
        minute = int(minute) if minute is not None else 0
        return f"{day:02d}.{month:02d} {hour:02d}:{minute:02d}"
    except (TypeError, ValueError):
        return None


async def upsert_vessel_static(imo, mmsi, name, ais_type, loa, max_draught, destination=None, eta=None):
    if not imo:
        return
    now = datetime.now(timezone.utc).isoformat()
    async with SessionLocal() as db:
        await db.execute(
            text(
                """
                INSERT INTO vessels (imo, mmsi, name, ais_type, loa, max_draught, destination, eta, updated_at)
                VALUES (:imo, :mmsi, :name, :ais_type, :loa, :draught, :dest, :eta, :now)
                ON CONFLICT(imo) DO UPDATE SET
                    mmsi=COALESCE(:mmsi, mmsi), name=COALESCE(:name, name),
                    ais_type=COALESCE(:ais_type, ais_type), loa=COALESCE(:loa, loa),
                    max_draught=COALESCE(:draught, max_draught),
                    destination=COALESCE(:dest, destination), eta=COALESCE(:eta, eta),
                    updated_at=:now
                """
            ),
            {"imo": str(imo), "mmsi": str(mmsi) if mmsi else None, "name": name, "ais_type": ais_type,
             "loa": loa, "draught": max_draught, "dest": destination, "eta": eta, "now": now},
        )
        await db.commit()


async def ais_worker():
    """Holds one persistent WebSocket to aisstream.io for the broker's
    working region and upserts every position/static-data message into the
    vessels table. Runs for the lifetime of the app; reconnects with
    backoff on any drop. No-ops (logs once) if no API key is configured."""
    if not AISSTREAM_API_KEY:
        logger.warning("AISSTREAM_API_KEY not set — AIS worker disabled.")
        return
    backoff = 5
    while True:
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream", ping_interval=20, ping_timeout=20) as ws:
                subscribe = {
                    "APIKey": AISSTREAM_API_KEY,
                    "BoundingBoxes": AIS_BOUNDING_BOXES,
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
                }
                await ws.send(json.dumps(subscribe))
                logger.info("AIS worker connected and subscribed.")
                backoff = 5
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    try:
                        mtype = msg.get("MessageType")
                        meta = msg.get("MetaData", {}) or {}
                        mmsi = meta.get("MMSI")
                        ts = meta.get("time_utc") or datetime.now(timezone.utc).isoformat()
                        if mtype == "PositionReport":
                            body = msg.get("Message", {}).get("PositionReport", {}) or {}
                            imo_hint = body.get("ImoNumber") or meta.get("IMO")
                            lat = body.get("Latitude", meta.get("latitude"))
                            lon = body.get("Longitude", meta.get("longitude"))
                            sog = body.get("Sog")
                            if mmsi and lat is not None and lon is not None:
                                await upsert_vessel_position(imo_hint, mmsi, lat, lon, sog, ts)
                        elif mtype == "ShipStaticData":
                            body = msg.get("Message", {}).get("ShipStaticData", {}) or {}
                            imo = body.get("ImoNumber")
                            name = sanitize_text(body.get("Name") or meta.get("ShipName"))
                            ais_type = body.get("Type")
                            dims = body.get("Dimension") or {}
                            a, b = dims.get("A"), dims.get("B")
                            loa = (a + b) if (a is not None and b is not None) else None
                            max_draught = body.get("MaximumStaticDraught")
                            destination = sanitize_text(body.get("Destination"))
                            eta = format_ais_eta(body.get("Eta"))
                            if imo and str(imo) not in ("0", "None"):
                                await upsert_vessel_static(imo, mmsi, name, str(ais_type) if ais_type is not None else None, loa, max_draught, destination, eta)
                    except Exception as msg_err:
                        logger.warning("AIS worker: skipping one bad message (%s)", msg_err)
                        continue
        except Exception as e:
            logger.warning("AIS worker disconnected (%s) — retrying in %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)


# ══════════════════════════════════════════════════════════
# MCP server — same data, exposed as tools for any Claude chat
# ══════════════════════════════════════════════════════════
def _get_or_create_mcp_token() -> str:
    if os.path.exists(MCP_TOKEN_PATH):
        with open(MCP_TOKEN_PATH) as f:
            existing = f.read().strip()
            if existing:
                return existing
    token = secrets.token_urlsafe(32)
    with open(MCP_TOKEN_PATH, "w") as f:
        f.write(token)
    os.chmod(MCP_TOKEN_PATH, 0o600)
    return token


MCP_TOKEN = _get_or_create_mcp_token()
# Allow the real public hostname through the MCP SDK's DNS-rebinding
# protection (it only trusts localhost by default). Our own bearer-token
# check below is the actual access control; this just lets legitimate
# external requests (from Claude.ai) through in the first place.
from mcp.server.transport_security import TransportSecuritySettings  # noqa: E402

MCP_ALLOWED_HOSTS = [
    "94-136-184-214.sslip.io",
    "94-136-184-214.sslip.io:443",
    "localhost",
    "localhost:8090",
    "127.0.0.1",
    "127.0.0.1:8090",
]
mcp_server = FastMCP(
    "CGL Indications",
    stateless_http=True,
    streamable_http_path="/",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=MCP_ALLOWED_HOSTS,
        allowed_origins=["https://" + h for h in MCP_ALLOWED_HOSTS] + ["https://claude.ai"],
    ),
)


@mcp_server.tool()
async def get_board() -> dict:
    """Get the full current freight indications board: every cargo type, its
    directions/routes, current rate ranges (low/high/unit), direction
    (outbound/backhaul), and owners/charterers idea notes with source+date."""
    state = await load_app_state()
    return state.get("board", {})


@mcp_server.tool()
async def list_cargo_types() -> dict:
    """List every cargo type id and its display label currently on the board."""
    state = await load_app_state()
    board = state.get("board", {})
    return {cid: c.get("label") for cid, c in board.get("cargoData", {}).items()}


@mcp_server.tool()
async def update_indication(
    cargo_id: str,
    route_id: str,
    low: float,
    high: float,
    idea_type: str = "estimate",
    note: str = "",
    source: str = "",
    date: str = "",
) -> dict:
    """Update the rate range on an existing direction/route, and optionally log
    a new idea note (owners/charterers/estimate) with a source and date.
    idea_type must be one of: 'owners', 'charterers', 'estimate'."""
    state = await load_app_state()
    board = state.setdefault("board", {})
    cargo = board.get("cargoData", {}).get(cargo_id)
    if not cargo:
        return {"error": f"cargo_id '{cargo_id}' not found"}
    route = next((r for r in cargo["routes"] if r["id"] == route_id), None)
    if not route:
        return {"error": f"route_id '{route_id}' not found under cargo '{cargo_id}'"}
    route["low"], route["high"] = low, high
    route["updatedAt"] = date or today_str()
    if note:
        bucket = "ownersIdeas" if idea_type == "owners" else ("charterersIdeas" if idea_type == "charterers" else "ownersIdeas")
        entry_text = note + (" (estimate)" if idea_type == "estimate" else "")
        route.setdefault(bucket, []).insert(0, {
            "text": entry_text,
            "source": source or "Claude (MCP)",
            "date": date or today_str(),
        })
        route[bucket] = route[bucket][:6]
    await save_app_state(state, "mcp-agent")
    return {"ok": True, "route": route}


@mcp_server.tool()
async def add_route(cargo_id: str, label: str, direction: str, low: float, high: float, unit: str = "$/mt", lot: Optional[str] = None) -> dict:
    """Add a new direction/route under an existing cargo type.
    direction must be 'outbound' (ex-Ukraine export) or 'backhaul' (import into Ukraine/CVB).
    lot is the parcel-size bracket this rate is for, so the dashboard files it under the right
    tonnage tab — one of '3000', '5000-6000', '6000-8000', '8000-10000', '20000-25000',
    '25000-30000' (handysize). ALWAYS pass it when the order/indication states a quantity;
    a route without a lot shows up under every tonnage tab at once. For a cargo quoted at
    several sizes, add one route per lot with the same label."""
    state = await load_app_state()
    board = state.setdefault("board", {})
    cargo = board.get("cargoData", {}).get(cargo_id)
    if not cargo:
        return {"error": f"cargo_id '{cargo_id}' not found - call add_cargo first if this is a new cargo type"}
    if direction not in ("outbound", "backhaul"):
        return {"error": "direction must be 'outbound' or 'backhaul'"}
    new_id = "r" + secrets.token_hex(6)
    route = {
        "id": new_id, "label": label, "direction": direction,
        "low": low, "high": high, "unit": unit,
        "ownersIdeas": [], "charterersIdeas": [], "updatedAt": today_str(),
    }
    if lot:
        route["lot"] = lot
    cargo["routes"].append(route)
    await save_app_state(state, "mcp-agent")
    return {"ok": True, "route": route}


@mcp_server.tool()
async def update_route(cargo_id: str, route_id: str, label: Optional[str] = None, lot: Optional[str] = None, direction: Optional[str] = None) -> dict:
    """Change a route's label, tonnage bracket (lot) and/or direction without
    touching its rate or notes — for fixing a wrong port name (e.g. a load port
    mislabelled) or filing a route under the right tonnage tab. Only the fields
    you pass are changed. lot: '3000', '5000-6000', '6000-8000', '8000-10000',
    '20000-25000', '25000-30000'."""
    state = await load_app_state()
    cargo = state.get("board", {}).get("cargoData", {}).get(cargo_id)
    if not cargo:
        return {"error": f"cargo_id '{cargo_id}' not found"}
    route = next((r for r in cargo.get("routes", []) if r.get("id") == route_id), None)
    if not route:
        return {"error": f"route_id '{route_id}' not found in {cargo_id}"}
    if direction is not None and direction not in ("outbound", "backhaul"):
        return {"error": "direction must be 'outbound' or 'backhaul'"}
    if label is not None and label.strip():
        route["label"] = label.strip()
    if lot is not None:
        if lot.strip():
            route["lot"] = lot.strip()
        else:
            route.pop("lot", None)
    if direction is not None:
        route["direction"] = direction
    route["updatedAt"] = today_str()
    await save_app_state(state, "mcp-agent")
    return {"ok": True, "route": route}


@mcp_server.tool()
async def add_cargo(cargo_id: str, label: str) -> dict:
    """Create a new cargo type bucket on the board. cargo_id should be a short
    lowercase slug (e.g. 'soybean'); label is the display name shown on the site."""
    state = await load_app_state()
    board = state.setdefault("board", {})
    cargo_data = board.setdefault("cargoData", {})
    if cargo_id in cargo_data:
        return {"error": f"cargo_id '{cargo_id}' already exists"}
    cargo_data[cargo_id] = {"label": label, "routes": []}
    await save_app_state(state, "mcp-agent")
    return {"ok": True, "cargo_id": cargo_id, "label": label}


@mcp_server.tool()
async def delete_route(cargo_id: str, route_id: str) -> dict:
    """Delete a direction/route from a cargo type."""
    state = await load_app_state()
    board = state.setdefault("board", {})
    cargo = board.get("cargoData", {}).get(cargo_id)
    if not cargo:
        return {"error": f"cargo_id '{cargo_id}' not found"}
    before = len(cargo["routes"])
    cargo["routes"] = [r for r in cargo["routes"] if r["id"] != route_id]
    if len(cargo["routes"]) == before:
        return {"error": f"route_id '{route_id}' not found under cargo '{cargo_id}'"}
    await save_app_state(state, "mcp-agent")
    return {"ok": True}


@mcp_server.tool()
async def add_note(text_: str) -> dict:
    """Add a quick manual note to the dashboard's notes log (visible to everyone on the board)."""
    state = await load_app_state()
    board = state.setdefault("board", {})
    notes = board.setdefault("notes", [])
    notes.insert(0, {
        "id": "nt" + secrets.token_hex(6),
        "text": text_,
        "ts": today_str() + " " + datetime.now(timezone.utc).strftime("%H:%M"),
    })
    board["notes"] = notes[:30]
    await save_app_state(state, "mcp-agent")
    return {"ok": True}


@mcp_server.tool()
async def delete_note(note_id: str) -> dict:
    """Delete a note from the dashboard's notes log by its id (get the id from
    get_board -> board.notes[].id)."""
    state = await load_app_state()
    board = state.setdefault("board", {})
    notes = board.setdefault("notes", [])
    before = len(notes)
    board["notes"] = [n for n in notes if n.get("id") != note_id]
    if len(board["notes"]) == before:
        return {"error": f"note_id '{note_id}' not found"}
    await save_app_state(state, "mcp-agent")
    return {"ok": True}


@mcp_server.tool()
async def list_documents_tool() -> list:
    """List knowledge-base documents (freight reports/circulars) saved on the
    dashboard, including their full text content. Each document has a
    category: 'general' (General Information), 'real' (Real data),
    'claude_calc' (Claude calculation), or 'lumpsum' (Lumpsum calculation) —
    this is what groups them under the site's "IND FAQ" view."""
    async with SessionLocal() as db:
        rows = (
            await db.execute(
                text("SELECT id, title, content, source, category, created_at, created_by FROM documents ORDER BY created_at DESC")
            )
        ).all()
        return [
            {"id": r.id, "title": r.title, "content": r.content, "source": r.source,
             "category": normalize_category(r.category), "created_at": r.created_at, "created_by": r.created_by}
            for r in rows
        ]


@mcp_server.tool()
async def add_document_tool(title: str, content: str, source: str = "", category: str = "general") -> dict:
    """Save a new freight report/circular into the dashboard's knowledge base.
    category must be one of: 'general' (General Information), 'real' (Real
    data), 'claude_calc' (Claude calculation), 'lumpsum' (Lumpsum
    calculation) — this groups it under the site's "IND FAQ" view. Defaults
    to 'general' if omitted or unrecognized."""
    if not title.strip() or not content.strip():
        return {"error": "title and content are required"}
    doc_id = secrets.token_urlsafe(12)
    now = datetime.now(timezone.utc).isoformat()
    cat = normalize_category(category)
    async with SessionLocal() as db:
        await db.execute(
            text(
                "INSERT INTO documents (id, title, content, source, category, created_at, created_by) "
                "VALUES (:id, :t, :c, :s, :cat, :ca, :cb)"
            ),
            {"id": doc_id, "t": title.strip(), "c": content, "s": (source or "").strip() or None,
             "cat": cat, "ca": now, "cb": "mcp-agent"},
        )
        await db.commit()
    return {"id": doc_id, "created_at": now, "category": cat}


@mcp_server.tool()
async def delete_document_tool(doc_id: str) -> dict:
    """Delete a knowledge-base document by its id (get the id from list_documents_tool)."""
    async with SessionLocal() as db:
        result = await db.execute(text("DELETE FROM documents WHERE id=:id"), {"id": doc_id})
        await db.commit()
        if result.rowcount == 0:
            return {"error": f"doc_id '{doc_id}' not found"}
    return {"ok": True}


@mcp_server.tool()
async def update_document_tool(
    doc_id: str,
    title: Optional[str] = None,
    content: Optional[str] = None,
    source: Optional[str] = None,
    category: Optional[str] = None,
) -> dict:
    """Update an existing knowledge-base document (get the id from
    list_documents_tool). Only the fields you pass are changed — leave the
    rest as None to keep them as-is. category must be one of: 'general'
    (General Information), 'real' (Real data), 'claude_calc' (Claude
    calculation), 'lumpsum' (Lumpsum calculation)."""
    async with SessionLocal() as db:
        row = (
            await db.execute(
                text("SELECT id, title, content, source, category FROM documents WHERE id=:id"),
                {"id": doc_id},
            )
        ).first()
        if not row:
            return {"error": f"doc_id '{doc_id}' not found"}
        new_title = title.strip() if (title is not None and title.strip()) else row.title
        new_content = content if (content is not None and content.strip()) else row.content
        new_source = (source.strip() if source is not None else row.source) or None
        new_category = normalize_category(category) if category is not None else normalize_category(row.category)
        await db.execute(
            text("UPDATE documents SET title=:t, content=:c, source=:s, category=:cat WHERE id=:id"),
            {"t": new_title, "c": new_content, "s": new_source, "cat": new_category, "id": doc_id},
        )
        await db.commit()
    return {"ok": True, "id": doc_id, "title": new_title, "category": new_category}


@mcp_server.tool()
async def find_vessels_near(lat: Optional[float] = None, lon: Optional[float] = None, radius_nm: float = 30,
                            cargo_only: bool = True, port: Optional[str] = None,
                            dwt_min: Optional[float] = None, dwt_max: Optional[float] = None,
                            loa_min: Optional[float] = None, loa_max: Optional[float] = None,
                            limit: int = 25) -> dict:
    """Find open vessels from the live AIS feed within radius_nm nautical
    miles of a point (lat/lon) OR of a named port: port="Varna", "Constanta",
    "Burgas", "Izmail"... or port="CVB" for Constanta+Varna+Burgas together
    (the handysize load region; use "CVB" for those orders). Size filters —
    dwt_min/dwt_max (e.g. 20000/40000 for handysize) judge vessels by DWT;
    add loa_min/loa_max (hull length in metres, e.g. 160/190) to ALSO include
    vessels whose DWT is not known yet, judged by hull length (each result
    says which one matched in "size_match"). DWT values marked
    dwt_source="name-match" are approximate. cargo_only=True (default) filters
    to AIS type codes 70-79 (cargo/bulk-type traffic) so yachts, passenger
    and other irrelevant vessel types the feed also picks up are excluded
    — set False to see everything. Returns each vessel's IMO, MMSI, name,
    AIS-reported type (plus any manually-corrected type), DWT if it's been
    recorded, LOA/draught from AIS, position, nearest reference port +
    distance to it, and how long ago it was last seen, plus speed over
    ground (sog) with a derived 'underway' flag (>0.5kn), and any
    self-reported destination/ETA from the crew (dd.mm hh:mm, no year) —
    useful for spotting a vessel already heading toward this exact port
    (it may become open once it discharges there), but destination/ETA is
    crew-entered free text and often stale/wrong, so treat it as a hint,
    not a fact. A vessel near 0 knots close to a port is a stronger
    "possibly waiting for cargo" signal than one passing through at speed —
    but a vessel underway AND reporting this port as its destination is
    also a good candidate (arriving soon). IMPORTANT: this is
    a physical-presence signal only, not confirmation that the vessel is
    open/available — always say so when relaying results, and prefer
    cross-checking a candidate against a broker circular (mail) or a
    direct check before treating it as a real candidate."""
    centers = None
    if port:
        centers, _ = resolve_port_centers(port)
        if centers is None:
            return {"error": f"unknown port '{port}'", "known_ports": known_port_names()}
    elif lat is not None and lon is not None:
        centers = [(lat, lon)]
    else:
        return {"error": "give either port=<name> (or 'CVB') or lat+lon"}
    async with SessionLocal() as db:
        rows = (await db.execute(text("SELECT * FROM vessels WHERE lat IS NOT NULL AND lon IS NOT NULL"))).all()
    found = select_vessels(rows, centers, radius_nm, cargo_only, dwt_min, dwt_max, loa_min, loa_max)
    return {"vessels": found[:limit], "total_matching": len(found), "disclaimer": AIS_DISCLAIMER}


@mcp_server.tool()
async def get_vessel_by_imo(imo: str) -> dict:
    """Look up one vessel's latest known AIS position/static data by IMO
    number (the reliable identifier — never match vessels by name alone,
    since many are near-duplicates, e.g. the Sormovskiy-type series).
    Same physical-presence-only caveat as find_vessels_near applies."""
    async with SessionLocal() as db:
        row = (await db.execute(text("SELECT * FROM vessels WHERE imo=:imo"), {"imo": str(imo)})).first()
    if not row:
        return {"error": f"no AIS data for IMO {imo}"}
    v = _vessel_row_to_dict(row)
    v["disclaimer"] = AIS_DISCLAIMER
    return v


@mcp_server.tool()
async def get_vessel_by_mmsi(mmsi: str) -> dict:
    """Look up one vessel's latest known AIS position/static data by MMSI.
    Same physical-presence-only caveat as find_vessels_near applies."""
    async with SessionLocal() as db:
        row = (await db.execute(text("SELECT * FROM vessels WHERE mmsi=:mmsi ORDER BY updated_at DESC"), {"mmsi": str(mmsi)})).first()
    if not row:
        return {"error": f"no AIS data for MMSI {mmsi}"}
    v = _vessel_row_to_dict(row)
    v["disclaimer"] = AIS_DISCLAIMER
    return v


@mcp_server.tool()
async def set_vessel_manual_type_tool(imo: str, manual_type: str) -> dict:
    """Manually correct a vessel's cargo/hull type when AIS's own type field
    is wrong (known failure mode — e.g. MV JOE 1, IMO 8714114, is AIS-typed
    as a container ship but is actually a bulker/general cargo vessel per
    real circulars). This manual value is then shown alongside — and
    flagged against — the AIS-reported type on the dashboard and in lookups."""
    async with SessionLocal() as db:
        result = await db.execute(text("UPDATE vessels SET manual_type=:t WHERE imo=:imo"), {"t": manual_type, "imo": str(imo)})
        await db.commit()
        if result.rowcount == 0:
            return {"error": f"no AIS data for IMO {imo} yet — it must appear in the AIS feed at least once before you can annotate it"}
    return {"ok": True}


@mcp_server.tool()
async def set_vessel_dwt(imo: str, dwt: float, source: Optional[str] = None) -> dict:
    """Record a vessel's deadweight tonnage (DWT) by IMO. Raw AIS never
    carries DWT — it's registry data — so use this after looking the
    vessel up on a free public source (e.g. Equasis, MagicPort) or from
    the broker's own vessel database, and it'll show on the dashboard's
    AIS/Vessels tab from then on. The vessel must already have appeared in
    the AIS feed at least once (i.e. have a row) before you can annotate it.
    source (optional): where the figure came from — "manual" (default), "crm-imo", or
    "name-match" if it was only matched by vessel name (shown as approximate on the dashboard)."""
    async with SessionLocal() as db:
        result = await db.execute(text("UPDATE vessels SET manual_dwt=:d, dwt_source=:s WHERE imo=:imo"), {"d": dwt, "s": source or "manual", "imo": str(imo)})
        await db.commit()
        if result.rowcount == 0:
            return {"error": f"no AIS data for IMO {imo} yet — it must appear in the AIS feed at least once before you can annotate it"}
    return {"ok": True}


@mcp_server.tool()
async def get_state_history(limit: int = 15) -> dict:
    """Recent saves of the dashboard's board/notes data — when, by whom (a dashboard user's
    browser, 'mcp-agent' for chat tools, or a script run under a user's login) and how big.
    Read-only; use it to find out what overwrote a change. A version can be restored on the
    server with restore_state_version.py."""
    async with SessionLocal() as db:
        rows = (await db.execute(
            text("SELECT id, updated_at, updated_by, size FROM state_history ORDER BY id DESC LIMIT :n"),
            {"n": max(1, min(int(limit), 100))},
        )).all()
    return {"saves": [{"id": r.id, "updated_at": r.updated_at, "updated_by": r.updated_by, "bytes": r.size} for r in rows]}


@mcp_server.tool()
async def add_wa_vessel(
    name: Optional[str] = None, imo: Optional[str] = None, mmsi: Optional[str] = None,
    build_year: Optional[int] = None, flag: Optional[str] = None, dwt: Optional[float] = None,
    dwcc: Optional[float] = None, intake_mt: Optional[float] = None, grain_cbft: Optional[float] = None,
    bale_cbft: Optional[float] = None, draft_m: Optional[float] = None, holds: Optional[str] = None,
    gear: Optional[str] = None, position: Optional[str] = None, region: Optional[str] = None,
    open_from: Optional[str] = None, open_to: Optional[str] = None, restrictions: Optional[str] = None,
    freight_idea: Optional[str] = None, contact: Optional[str] = None, broker_company: Optional[str] = None,
    status: Optional[str] = None, source_chat: Optional[str] = None, source_sender: Optional[str] = None,
    source_date: Optional[str] = None, raw_text: Optional[str] = None, note: Optional[str] = None,
) -> dict:
    """Log one vessel offer / position seen in a WhatsApp chat (SHIPS OFFERS & UPDATES,
    Fixing Team, a personal contact, etc.) so any later search — here or in another chat —
    can find it, instead of the message only living inside this one conversation.

    You (the calling chat) should already have read and understood the WhatsApp message —
    pass the fields you extracted from it directly; don't just pass raw_text and expect
    server-side parsing. Leave a field out (None) if the message didn't give it rather than
    guessing. raw_text is still worth passing for provenance/audit even though you parsed it
    yourself. region must be one of: "Danube inside", "Sulina", "Black Sea", "Marmara",
    "Aegean", "E.Med", "C.Med", "Other". status defaults to "open" if not given
    (one of: open, on_subs, fixed, withdrawn, expired).

    Matching/dedup: if imo is given, this upserts by IMO (the same vessel re-listed with a
    newer date updates the existing row instead of duplicating it). Otherwise it upserts by
    a composite of name + dwt + build_year. If BOTH name and imo are missing (e.g. a
    "cargo-seeking tonnage" request with no vessel named), a new row is always inserted —
    there's nothing reliable to match it against.

    Put hard limits in restrictions (no UKR/RUS, Sulina only...) and every other useful detail
    from the message (free days, demurrage, "ready via Bystroe", laycan notes, owner's terms)
    in note, and any rate talk in freight_idea — they are searchable later via list_wa_vessels(q=...).

    IMPORTANT: a WhatsApp listing is not a confirmed open vessel any more than an AIS sighting
    is — always say so when relaying results, and prefer confirming with the broker/owner
    before treating it as a firm candidate. restrictions (no UKR/RUS, Sulina-only, etc.) is
    often the single detail that rules a vessel out, so always surface it when present."""
    fields = {
        "name": name, "imo": imo, "mmsi": mmsi, "build_year": build_year, "flag": flag, "dwt": dwt,
        "dwcc": dwcc, "intake_mt": intake_mt, "grain_cbft": grain_cbft, "bale_cbft": bale_cbft,
        "draft_m": draft_m, "holds": holds, "gear": gear, "position": position, "region": region,
        "open_from": open_from, "open_to": open_to, "restrictions": restrictions,
        "freight_idea": freight_idea, "contact": contact, "broker_company": broker_company,
        "status": status, "source_chat": source_chat, "source_sender": source_sender,
        "source_date": source_date, "raw_text": raw_text, "note": note,
    }
    async with SessionLocal() as db:
        row = await upsert_wa_vessel(db, fields)
        await db.commit()
    return row


@mcp_server.tool()
async def list_wa_vessels(
    status: Optional[str] = None, region: Optional[str] = None, dwt_min: Optional[float] = None,
    dwt_max: Optional[float] = None, max_draft: Optional[float] = None, open_before: Optional[str] = None,
    inside_danube: Optional[bool] = None, q: Optional[str] = None,
) -> dict:
    """List logged WhatsApp vessel offers/positions, optionally filtered.
    q: free-text search — every word must appear (case-insensitive) in the vessel's name, IMO,
    position, restrictions, freight_idea, note, contact or the original WhatsApp message, e.g.
    q="bystroe" or q="no ukr". ALWAYS read each result's restrictions, note and freight_idea
    (and raw_text when in doubt) before suggesting a vessel — details like "Sulina only",
    free days, or "ready via Bystroe" live there, not in dedicated fields.
    status: one of open/on_subs/fixed/withdrawn/expired (default: all). region: one of
    "Danube inside", "Sulina", "Black Sea", "Marmara", "Aegean", "E.Med", "C.Med", "Other".
    dwt_min/dwt_max: deadweight range. max_draft: only vessels with draft_m <= this (metres).
    open_before: only vessels open by this date (YYYY-MM-DD) — compares against open_from.
    inside_danube=True: shortcut for region="Danube inside". Each result includes
    source_date and raw_text so you can judge how fresh/reliable it is, and restrictions
    (the usual deal-breaker field). This is a WhatsApp listing, not a confirmed open
    vessel — treat it the same way as an AIS sighting: a lead to confirm, not a fact."""
    async with SessionLocal() as db:
        await wa_sweep_expired(db)
        await db.commit()
        rows = await wa_query(
            db, status=status, region=region, dwt_min=dwt_min, dwt_max=dwt_max,
            max_draft=max_draft, open_before=open_before, inside_danube=inside_danube, q=q,
        )
    return {"vessels": rows, "total": len(rows)}


@mcp_server.tool()
async def get_wa_vessel(imo: Optional[str] = None, name: Optional[str] = None) -> dict:
    """Look up one logged WhatsApp vessel entry by IMO (preferred) or by name. Returns the
    most recently updated matching row, including source_date/raw_text and restrictions."""
    if not imo and not name:
        return {"error": "give imo or name"}
    async with SessionLocal() as db:
        await wa_sweep_expired(db)
        await db.commit()
        if imo:
            row = (await db.execute(
                text("SELECT * FROM wa_vessels WHERE imo=:imo ORDER BY updated_at DESC LIMIT 1"), {"imo": str(imo)}
            )).first()
        else:
            row = (await db.execute(
                text("SELECT * FROM wa_vessels WHERE name LIKE :n ORDER BY updated_at DESC LIMIT 1"),
                {"n": f"%{name}%"},
            )).first()
    if not row:
        return {"error": "no matching WhatsApp vessel entry"}
    return _wa_row_to_dict(row)


@mcp_server.tool()
async def update_wa_vessel(
    id: str, name: Optional[str] = None, imo: Optional[str] = None, mmsi: Optional[str] = None,
    build_year: Optional[int] = None, flag: Optional[str] = None, dwt: Optional[float] = None,
    dwcc: Optional[float] = None, intake_mt: Optional[float] = None, grain_cbft: Optional[float] = None,
    bale_cbft: Optional[float] = None, draft_m: Optional[float] = None, holds: Optional[str] = None,
    gear: Optional[str] = None, position: Optional[str] = None, region: Optional[str] = None,
    open_from: Optional[str] = None, open_to: Optional[str] = None, restrictions: Optional[str] = None,
    freight_idea: Optional[str] = None, contact: Optional[str] = None, broker_company: Optional[str] = None,
    status: Optional[str] = None, note: Optional[str] = None,
) -> dict:
    """Update one logged WhatsApp vessel entry by id (from list_wa_vessels/get_wa_vessel/
    add_wa_vessel's returned row) — most often just a status change (e.g. status="on_subs"
    or status="fixed" once you hear it went). Only pass the fields you want to change; the
    rest are left as-is. There is no delete — a bad or superseded entry should get
    status="withdrawn" instead, never removed."""
    fields = {
        "name": name, "imo": imo, "mmsi": mmsi, "build_year": build_year, "flag": flag, "dwt": dwt,
        "dwcc": dwcc, "intake_mt": intake_mt, "grain_cbft": grain_cbft, "bale_cbft": bale_cbft,
        "draft_m": draft_m, "holds": holds, "gear": gear, "position": position, "region": region,
        "open_from": open_from, "open_to": open_to, "restrictions": restrictions,
        "freight_idea": freight_idea, "contact": contact, "broker_company": broker_company,
        "status": status, "note": note,
    }
    fields = {k: v for k, v in fields.items() if v is not None}
    if not fields:
        return {"error": "nothing to update"}
    async with SessionLocal() as db:
        row = await wa_update_by_id(db, id, fields)
        await db.commit()
        if row is None:
            return {"error": f"no WhatsApp vessel entry with id {id}"}
    return row


mcp_asgi_app = mcp_server.streamable_http_app()


class MCPAuthMiddleware:
    """Single static bearer token, checked on every request to the mounted MCP
    app. Matches Claude.ai's custom-connector "None + Request headers" mode:
    no OAuth handshake, just a fixed header value Claude attaches to every
    call. Token lives in data/mcp_token.txt (see _get_or_create_mcp_token).

    Claude.ai's connector UI does its own connectivity/discovery probe
    (an MCP "initialize" call, and sometimes "tools/list") *before* it
    treats the connector as usable, and — even in "No sign-in" mode — a
    401 on that probe makes the UI report the whole server as
    unreachable rather than just "auth failed". So low-sensitivity
    discovery methods (which reveal only protocol/tool names+schemas, no
    actual board/document data) are let through without the token; any
    real data-touching call (tools/call, resources, etc.) still requires
    it."""

    UNAUTH_METHODS = {"initialize", "notifications/initialized", "tools/list", "ping"}

    def __init__(self, wrapped_app, token: str):
        self.wrapped_app = wrapped_app
        self.token = token

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.wrapped_app(scope, receive, send)
            return

        headers = dict(scope.get("headers") or [])
        auth = headers.get(b"authorization", b"").decode()
        if auth == f"Bearer {self.token}":
            await self.wrapped_app(scope, receive, send)
            return

        # Not authorized as-is — buffer the body so we can inspect the
        # JSON-RPC method, then replay it to the wrapped app either way.
        body_chunks = []
        more_body = True
        while more_body:
            message = await receive()
            body_chunks.append(message.get("body", b""))
            more_body = message.get("more_body", False)
        body = b"".join(body_chunks)

        method = None
        try:
            payload = json.loads(body or b"{}")
            method = payload.get("method")
        except Exception:
            method = None

        if method not in self.UNAUTH_METHODS:
            response = JSONResponse({"error": "unauthorized"}, status_code=401)
            await response(scope, receive, send)
            return

        sent = False

        async def replay_receive():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.wrapped_app(scope, replay_receive, send)


mcp_asgi_app_protected = MCPAuthMiddleware(mcp_asgi_app, MCP_TOKEN)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with mcp_server.session_manager.run():
        async with engine.begin() as conn:
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            for stmt in SCHEMA_STATEMENTS:
                await conn.execute(text(stmt))
            for stmt in MIGRATION_STATEMENTS:
                try:
                    await conn.execute(text(stmt))
                except Exception:
                    pass  # column already exists (older DB already migrated)
            existing = (await conn.execute(text("SELECT COUNT(*) FROM lion_regions"))).scalar()
            if not existing:
                await conn.execute(
                    text("INSERT INTO lion_regions (id, label, sort_order) VALUES (:id, :label, :o)"),
                    [{"id": "bsea_med", "label": "BSEA - MED", "o": 0},
                     {"id": "cont_balt", "label": "CONT - BALT", "o": 1}],
                )
        ais_task = asyncio.create_task(ais_worker())
        try:
            yield
        finally:
            ais_task.cancel()
            try:
                await ais_task
            except (asyncio.CancelledError, Exception):
                pass


app = FastAPI(title="CGL Indications API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)
app.mount("/mcp", mcp_asgi_app_protected)


class LoginBody(BaseModel):
    username: str
    password: str


class StateBody(BaseModel):
    value: dict
    # The updated_at the client last loaded/saved. If it no longer matches the server's,
    # somebody else (the indications chat, a script, another tab) changed the data since —
    # the save is refused with 409 instead of silently overwriting their work.
    base_updated_at: Optional[str] = None
    # One-off maintenance scripts read the fresh state right before writing and may skip the check.
    force: bool = False


@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "1.1.0"}


@app.post("/api/login")
async def login(body: LoginBody):
    # A password typed with the wrong keyboard layout can arrive as lone
    # surrogate characters that can't be encoded to UTF-8 — that used to blow
    # up inside the database driver as a 500. It's just a wrong password.
    try:
        body.username.encode("utf-8")
        body.password.encode("utf-8")
    except UnicodeEncodeError:
        raise HTTPException(status_code=401, detail="Invalid username or password (check the keyboard layout)")
    async with SessionLocal() as db:
        row = (
            await db.execute(
                text("SELECT id, password_hash, salt FROM users WHERE username=:u"),
                {"u": body.username},
            )
        ).first()
        if not row or not verify_password(body.password, row.salt, row.password_hash):
            raise HTTPException(status_code=401, detail="Invalid username or password")

        token = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        expires = now + timedelta(days=SESSION_TTL_DAYS)
        await db.execute(
            text(
                "INSERT INTO sessions (token, user_id, username, created_at, expires_at) "
                "VALUES (:t, :u, :un, :c, :e)"
            ),
            {
                "t": token,
                "u": row.id,
                "un": body.username,
                "c": now.isoformat(),
                "e": expires.isoformat(),
            },
        )
        await db.commit()
        return {"token": token, "username": body.username, "expires_at": expires.isoformat()}


@app.post("/api/logout")
async def logout(authorization: Optional[str] = Header(None)):
    token = _extract_token(authorization)
    async with SessionLocal() as db:
        await db.execute(text("DELETE FROM sessions WHERE token=:t"), {"t": token})
        await db.commit()
    return {"ok": True}


def _extract_token(authorization: Optional[str]) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    return authorization[len("Bearer "):]


async def get_current_username(authorization: Optional[str] = Header(None)) -> str:
    token = _extract_token(authorization)
    async with SessionLocal() as db:
        row = (
            await db.execute(
                text("SELECT username, expires_at FROM sessions WHERE token=:t"),
                {"t": token},
            )
        ).first()
        if not row:
            raise HTTPException(status_code=401, detail="Invalid session")
        expires_at = datetime.fromisoformat(row.expires_at)
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at < datetime.now(timezone.utc):
            raise HTTPException(status_code=401, detail="Session expired, please sign in again")
        return row.username


@app.get("/api/state")
async def get_state(username: str = Depends(get_current_username)):
    async with SessionLocal() as db:
        row = (
            await db.execute(text("SELECT value, updated_at, updated_by FROM kv_store WHERE key='app_state'"))
        ).first()
        if not row:
            return {"value": None, "updated_at": None, "updated_by": None}
        return {"value": json.loads(row.value), "updated_at": row.updated_at, "updated_by": row.updated_by}


@app.put("/api/state")
async def put_state(body: StateBody, username: str = Depends(get_current_username)):
    async with SessionLocal() as db:
        row = (await db.execute(text("SELECT updated_at FROM kv_store WHERE key='app_state'"))).first()
    current = row.updated_at if row else None
    if current is not None and not body.force:
        if body.base_updated_at is None:
            # An old copy of the page (open since before this check existed) would otherwise
            # silently overwrite everything the chat/scripts changed since it loaded.
            raise HTTPException(status_code=428, detail="This page is an old version and can't save safely — reload it (Ctrl+Shift+R). Nothing was overwritten.")
        if current != body.base_updated_at:
            raise HTTPException(status_code=409, detail="The data was changed elsewhere since this page loaded — reload to get the latest version")
    now = await save_app_state(body.value, username)
    return {"ok": True, "updated_at": now, "updated_by": username}


# ── Knowledge base documents ──
# Freight circulars, weekly reports, cargo-order lists, etc. that the broker
# pastes in. Stored as plain rows (not part of the app_state JSON blob,
# since they can be sizeable and are logically separate). The AI panel pulls
# these in as extra context alongside mail search and web search.
# category buckets the "IND FAQ" view on the dashboard groups documents into.
VALID_DOC_CATEGORIES = {"general", "real", "claude_calc", "lumpsum"}


def normalize_category(cat: Optional[str]) -> str:
    c = (cat or "").strip().lower()
    return c if c in VALID_DOC_CATEGORIES else "general"


class DocumentBody(BaseModel):
    title: str
    content: str
    source: Optional[str] = None
    category: Optional[str] = None


@app.get("/api/documents")
async def list_documents(username: str = Depends(get_current_username)):
    async with SessionLocal() as db:
        rows = (
            await db.execute(
                text(
                    "SELECT id, title, content, source, category, created_at, created_by "
                    "FROM documents ORDER BY created_at DESC"
                )
            )
        ).all()
        return {
            "documents": [
                {
                    "id": r.id,
                    "title": r.title,
                    "content": r.content,
                    "source": r.source,
                    "category": normalize_category(r.category),
                    "created_at": r.created_at,
                    "created_by": r.created_by,
                }
                for r in rows
            ]
        }


@app.post("/api/documents")
async def create_document(body: DocumentBody, username: str = Depends(get_current_username)):
    if not body.title.strip() or not body.content.strip():
        raise HTTPException(status_code=400, detail="Title and content are required")
    doc_id = secrets.token_urlsafe(12)
    now = datetime.now(timezone.utc).isoformat()
    category = normalize_category(body.category)
    async with SessionLocal() as db:
        await db.execute(
            text(
                "INSERT INTO documents (id, title, content, source, category, created_at, created_by) "
                "VALUES (:id, :t, :c, :s, :cat, :ca, :cb)"
            ),
            {
                "id": doc_id,
                "t": body.title.strip(),
                "c": body.content,
                "s": (body.source or "").strip() or None,
                "cat": category,
                "ca": now,
                "cb": username,
            },
        )
        await db.commit()
    return {"id": doc_id, "created_at": now, "category": category}


class DocumentUpdateBody(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None
    source: Optional[str] = None
    category: Optional[str] = None


@app.put("/api/documents/{doc_id}")
async def update_document(doc_id: str, body: DocumentUpdateBody, username: str = Depends(get_current_username)):
    async with SessionLocal() as db:
        row = (
            await db.execute(
                text("SELECT id, title, content, source, category FROM documents WHERE id=:id"),
                {"id": doc_id},
            )
        ).first()
        if not row:
            raise HTTPException(status_code=404, detail="Document not found")
        new_title = body.title.strip() if (body.title is not None and body.title.strip()) else row.title
        new_content = body.content if (body.content is not None and body.content.strip()) else row.content
        new_source = (body.source.strip() if body.source is not None else row.source) or None
        new_category = normalize_category(body.category) if body.category is not None else normalize_category(row.category)
        await db.execute(
            text("UPDATE documents SET title=:t, content=:c, source=:s, category=:cat WHERE id=:id"),
            {"t": new_title, "c": new_content, "s": new_source, "cat": new_category, "id": doc_id},
        )
        await db.commit()
    return {"ok": True, "id": doc_id, "title": new_title, "category": new_category}


@app.delete("/api/documents/{doc_id}")
async def delete_document(doc_id: str, username: str = Depends(get_current_username)):
    async with SessionLocal() as db:
        result = await db.execute(text("DELETE FROM documents WHERE id=:id"), {"id": doc_id})
        await db.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Document not found")
    return {"ok": True}


# ── AIS / vessels ──
STALE_HOURS = 6
AIS_DISCLAIMER = (
    "AIS shows physical presence near a port, not confirmed open tonnage. "
    "Treat as a candidate only — confirm via a broker circular (mail) or a direct check "
    "before treating the vessel as available."
)


def is_cargo_type(ais_type):
    """AIS type codes 70-79 are the 'Cargo' category (includes bulk
    carriers, general cargo, etc.) — coarse, but enough to filter out
    yachts/passenger/tanker/fishing traffic the AIS feed also picks up.
    Unknown type (static data not received yet) is excluded from the
    default cargo-only view until it's classified."""
    return bool(ais_type) and ais_type.strip().startswith("7")


def parse_ais_time(s):
    """AISStream stamps messages Go-style ('2026-09-28 08:40:17.980293281 +0000 UTC',
    nanoseconds, no ISO 'T'), which datetime.fromisoformat can't read — that's why
    the fresh/stale flag was always empty. Also accepts plain ISO strings."""
    if not s:
        return None
    s = str(s).strip()
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    m = re.match(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d+))?", s)
    if not m:
        return None
    frac = (m.group(3) or "0")[:6].ljust(6, "0")
    return datetime.strptime(f"{m.group(1)} {m.group(2)}.{frac}", "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)


def _vessel_row_to_dict(r):
    lat, lon = r.lat, r.lon
    port_name, port_dist = nearest_port(lat, lon)
    is_stale = None
    last_dt = parse_ais_time(r.last_seen)
    if last_dt:
        age_hours = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600.0
        is_stale = age_hours > STALE_HOURS
    type_mismatch = bool(r.manual_type and r.ais_type and r.manual_type != r.ais_type)
    is_underway = (r.sog is not None and r.sog > 0.5) if r.sog is not None else None
    return {
        "imo": r.imo, "mmsi": r.mmsi, "name": r.name,
        "ais_type": r.ais_type, "manual_type": r.manual_type, "type_mismatch": type_mismatch,
        "dwt": r.manual_dwt,  # only ever set manually/imported — AIS itself carries no DWT field
        # None/"manual"/"crm-imo" = trusted (typed in or matched by IMO); "name-match" = matched by
        # vessel NAME against a broker list — plausible, but not confirmed by IMO
        "dwt_source": r.dwt_source,
        "loa": r.loa, "max_draught": r.max_draught,
        "destination": r.destination, "eta": r.eta,
        "lat": lat, "lon": lon, "sog": r.sog, "underway": is_underway,
        "nearest_port": port_name, "distance_nm": port_dist,
        "last_seen": last_dt.strftime("%Y-%m-%dT%H:%M:%SZ") if last_dt else r.last_seen, "stale": is_stale,
    }


def resolve_port_centers(port):
    """'CVB' = Constanta + Varna + Burgas (the handysize load region); otherwise any
    reference port whose name contains the text. Returns (centers, matched_names) or (None, None)."""
    p = (port or "").strip().lower()
    if not p:
        return None, None
    wanted = {"constanta", "varna", "burgas"} if p == "cvb" else None
    matched = [(n, la, lo) for n, la, lo in REFERENCE_PORTS
               if (n.lower() in wanted if wanted else p in n.lower())]
    if not matched:
        return None, None
    return [(la, lo) for _, la, lo in matched], [n for n, _, _ in matched]


def known_port_names():
    return ["CVB"] + [n for n, _, _ in REFERENCE_PORTS]


def _in_range(x, lo, hi):
    return x is not None and (lo is None or x >= lo) and (hi is None or x <= hi)


def select_vessels(rows, centers=None, radius_nm=30, cargo_only=True,
                   dwt_min=None, dwt_max=None, loa_min=None, loa_max=None):
    """Shared filter for the dashboard AIS tab and the chat tool.
    Size rule: with a DWT range set, a vessel passes on its DWT; if a hull-length range is
    ALSO set, vessels whose DWT is unknown can pass on hull length instead (that is what
    the length range is for). With only a length range, everything is judged by length."""
    dwt_active = dwt_min is not None or dwt_max is not None
    loa_active = loa_min is not None or loa_max is not None
    out = []
    for r in rows:
        if cargo_only and not is_cargo_type(r.ais_type):
            continue
        dist = None
        if centers:
            if r.lat is None or r.lon is None:
                continue
            dist = min(haversine_nm(la, lo, r.lat, r.lon) for la, lo in centers)
            if dist > radius_nm:
                continue
        loa = r.loa if (r.loa and r.loa > 0) else None
        size_match = None
        if dwt_active and loa_active:
            if _in_range(r.manual_dwt, dwt_min, dwt_max):
                size_match = "dwt"
            elif r.manual_dwt is None and _in_range(loa, loa_min, loa_max):
                size_match = "hull length (DWT unknown)"
            else:
                continue
        elif dwt_active:
            if not _in_range(r.manual_dwt, dwt_min, dwt_max):
                continue
            size_match = "dwt"
        elif loa_active:
            if not _in_range(loa, loa_min, loa_max):
                continue
            size_match = "hull length"
        v = _vessel_row_to_dict(r)
        if dist is not None:
            v["distance_from_query_nm"] = round(dist, 1)
        if size_match:
            v["size_match"] = size_match
        out.append(v)
    if centers:
        out.sort(key=lambda v: v["distance_from_query_nm"])
    return out


async def query_vessels_near(lat, lon, radius_nm, limit=25, cargo_only=True):
    async with SessionLocal() as db:
        rows = (await db.execute(text("SELECT * FROM vessels WHERE lat IS NOT NULL AND lon IS NOT NULL"))).all()
    return select_vessels(rows, [(lat, lon)], radius_nm, cargo_only)[:limit]


@app.get("/api/vessels")
async def list_vessels(all_types: bool = False, port: Optional[str] = None, radius_nm: float = 30,
                       dwt_min: Optional[float] = None, dwt_max: Optional[float] = None,
                       loa_min: Optional[float] = None, loa_max: Optional[float] = None,
                       limit: Optional[int] = None, username: str = Depends(get_current_username)):
    centers = None
    if port:
        centers, _ = resolve_port_centers(port)
        if centers is None:
            raise HTTPException(status_code=400, detail=f"Unknown port '{port}'")
    async with SessionLocal() as db:
        rows = (await db.execute(text("SELECT * FROM vessels ORDER BY updated_at DESC"))).all()
    vessels = select_vessels(rows, centers, radius_nm, not all_types, dwt_min, dwt_max, loa_min, loa_max)
    total = len(vessels)
    if limit:
        vessels = vessels[:limit]
    return {"vessels": vessels, "total_matching": total, "ports": known_port_names(), "disclaimer": AIS_DISCLAIMER}


class VesselAnnotateBody(BaseModel):
    manual_type: Optional[str] = None
    manual_dwt: Optional[float] = None
    dwt_source: Optional[str] = None


@app.put("/api/vessels/{imo}/type")
async def set_vessel_manual_type(imo: str, body: VesselAnnotateBody, username: str = Depends(get_current_username)):
    async with SessionLocal() as db:
        row = (await db.execute(text("SELECT imo, manual_type, manual_dwt, dwt_source FROM vessels WHERE imo=:imo"), {"imo": imo})).first()
        if not row:
            raise HTTPException(status_code=404, detail="Vessel not found")
        new_type = body.manual_type if body.manual_type is not None else row.manual_type
        new_dwt = body.manual_dwt if body.manual_dwt is not None else row.manual_dwt
        # typing a DWT in by hand (no source given) makes it a trusted value again
        new_src = (body.dwt_source or "manual") if body.manual_dwt is not None else row.dwt_source
        await db.execute(
            text("UPDATE vessels SET manual_type=:t, manual_dwt=:d, dwt_source=:s WHERE imo=:imo"),
            {"t": new_type, "d": new_dwt, "s": new_src, "imo": imo},
        )
        await db.commit()
    return {"ok": True}


# ══════════════════════════════════════════════════════════
# Lion SP — Lion Maritime's own open-positions list (uploaded as an
# .xls/.xlsx export, no IMO in the source data, matched by vessel name)
# ══════════════════════════════════════════════════════════
LION_STALE_DAYS = 21

# Keyword -> region id, checked against the "Position"/"Open Area" text
# (case-insensitive substring match, longest-keyword-first so e.g.
# "north adriatic" doesn't get shadowed by a shorter unrelated match).
LION_REGION_KEYWORDS = {
    "bsea_med": [
        "black sea", "azov", "mediterranean", "med sea", "adriatic", "aegean", "marmara",
        "danube", "bosphorus", "dardanelles", "bizerte", "sfax", "gabes", "tunis", "tripoli",
        "libya", "algeria", "algiers", "annaba", "oran", "djen djen", "bejaia", "nador",
        "egypt", "alexandria", "damietta", "port said", "israel", "ashdod", "haifa",
        "lebanon", "beirut", "syria", "latakia", "tartus", "cyprus", "famagusta", "limassol",
        "turkey", "mersin", "iskenderun", "samsun", "trabzon", "tuzla", "ambarli", "aliaga",
        "bandirma", "nemrut", "izmir", "canakkale", "dilis kelesi", "diliskelesi",
        "greece", "piraeus", "chalkis", "ravenna", "vatika", "spain", "spanish med", "sagunto",
        "tarragona", "castellon", "valencia", "fos", "italy", "italian", "porto marghera",
        "otranto", "livorno", "piombino", "naples", "catania", "monopoli", "varna", "burgas",
        "constanta", "istanbul", "izmail", "reni", "odesa", "odessa", "poti", "georgia",
    ],
    "cont_balt": [
        "baltic", "north sea", "continent", "rotterdam", "amsterdam", "antwerp", "hamburg",
        "bremen", "gdansk", "gdynia", "klaipeda", "riga", "tallinn", "st petersburg",
        "ust-luga", "ust luga", "uk", "united kingdom", "france", "le havre", "dunkirk",
        "skaw", "denmark", "poland", "germany",
    ],
}


def classify_lion_region(position_text):
    if not position_text:
        return None
    p = position_text.lower()
    best_region, best_len = None, 0
    for region_id, keywords in LION_REGION_KEYWORDS.items():
        for kw in keywords:
            if kw in p and len(kw) > best_len:
                best_region, best_len = region_id, len(kw)
    return best_region


def lion_name_key(name):
    return re.sub(r"[^A-Z0-9]+", "", str(name or "").upper())


_DATE_RANGE_RE = re.compile(
    r"(\d{1,2}[./]\d{1,2}[./]\d{2,4})\s*-\s*(\d{1,2}[./]\d{1,2}[./]\d{2,4})"
)
_DATE_SINGLE_RE = re.compile(r"(\d{1,2}[./]\d{1,2}[./]\d{2,4})")


def _parse_one_date(s):
    s = s.replace("/", ".")
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_lion_date_range(raw):
    """'05.10.2026 - 07.10.2026' -> (from, to); a single date -> (d, d);
    anything unrecognised -> (None, None) (the row is still imported,
    just without dates to sort/expire by)."""
    if not raw:
        return None, None
    raw = str(raw).strip()
    m = _DATE_RANGE_RE.search(raw)
    if m:
        return _parse_one_date(m.group(1)), _parse_one_date(m.group(2))
    m = _DATE_SINGLE_RE.search(raw)
    if m:
        d = _parse_one_date(m.group(1))
        return d, d
    return None, None


def parse_lion_dwt(raw):
    if raw is None or raw == "":
        return None
    s = re.sub(r"[^\d.]", "", str(raw).replace("\xa0", "").replace(",", ""))
    try:
        return float(s) if s else None
    except ValueError:
        return None


def parse_lion_built(raw):
    try:
        y = int(float(raw))
        return y if 1960 <= y <= 2035 else None
    except (ValueError, TypeError):
        return None


def parse_lion_workbook(raw_bytes, filename):
    """Reads the 'No, Vessel Name, DWT, BLT, Open Area, Open Date' export
    (column order detected by header name, not position, so a reordered or
    lightly-different export still works) from .xls or .xlsx. Returns a
    list of raw row dicts; header/column matching is case-insensitive and
    tolerant of the exact wording (e.g. 'Vessel Name' or 'VESSEL')."""
    import io

    def col_index(headers, *candidates):
        low = [str(h or "").strip().lower() for h in headers]
        for cand in candidates:
            for i, h in enumerate(low):
                if cand in h:
                    return i
        return None

    rows_out = []
    lower_name = (filename or "").lower()
    if lower_name.endswith(".xlsx"):
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(raw_bytes), data_only=True, read_only=True)
        ws = wb.worksheets[0]
        raw_rows = [list(r) for r in ws.iter_rows(values_only=True)]
    else:
        import xlrd
        wb = xlrd.open_workbook(file_contents=raw_bytes)
        ws = wb.sheet_by_index(0)
        raw_rows = [[ws.cell_value(r, c) for c in range(ws.ncols)] for r in range(ws.nrows)]

    if not raw_rows:
        return []
    headers = raw_rows[0]
    i_name = col_index(headers, "vessel")
    i_dwt = col_index(headers, "dwt")
    i_built = col_index(headers, "blt", "built")
    i_area = col_index(headers, "open area", "position", "area")
    i_date = col_index(headers, "open date", "open from", "date")
    i_comments = col_index(headers, "comment")
    if i_name is None:
        raise ValueError("Couldn't find a vessel-name column in this file's header row")

    def cell(row, idx):
        if idx is None or idx >= len(row):
            return None
        v = row[idx]
        return None if v == "" else v

    for r in raw_rows[1:]:
        name = cell(r, i_name)
        if not name or not str(name).strip():
            continue
        rows_out.append({
            "vessel_name": str(name).strip(),
            "dwt": parse_lion_dwt(cell(r, i_dwt)),
            "built": parse_lion_built(cell(r, i_built)),
            "position": str(cell(r, i_area) or "").strip() or None,
            "open_raw": cell(r, i_date),
            "comments": str(cell(r, i_comments) or "").strip() or None,
        })
    return rows_out


async def lion_purge_stale(db):
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=LION_STALE_DAYS)).isoformat()
    result = await db.execute(
        text(
            "DELETE FROM lion_positions WHERE COALESCE(open_to, open_from) IS NOT NULL "
            "AND COALESCE(open_to, open_from) < :cutoff"
        ),
        {"cutoff": cutoff},
    )
    return result.rowcount or 0


@app.get("/api/lion-positions")
async def list_lion_positions(username: str = Depends(get_current_username)):
    async with SessionLocal() as db:
        regions = (await db.execute(text("SELECT id, label, sort_order FROM lion_regions ORDER BY sort_order"))).all()
        rows = (await db.execute(text("SELECT * FROM lion_positions ORDER BY dwt"))).all()
    return {
        "regions": [{"id": r.id, "label": r.label} for r in regions],
        "positions": [
            {
                "id": r.id, "vessel_name": r.vessel_name, "built": r.built, "dwt": r.dwt,
                "position": r.position, "open_from": r.open_from, "open_to": r.open_to,
                "comments": r.comments, "region": r.region, "region_manual": bool(r.region_manual),
                "updated_at": r.updated_at,
            }
            for r in rows
        ],
    }


@app.post("/api/lion-positions/upload")
async def upload_lion_positions(body: dict, username: str = Depends(get_current_username)):
    """body: {filename, content_base64}. Parses the workbook, collapses
    same-named duplicates within the upload (row order = recency in this
    export — the first/topmost row for a given vessel is its newest
    listing, later duplicates are older re-listings and are dropped),
    upserts each vessel by name (an update replaces that vessel's position
    entirely — the new circular supersedes the old one), auto-classifies
    the region from the position text unless the row was manually
    reassigned, then purges anything now more than 21 days past its open
    date across the whole table (not just this upload)."""
    import base64

    filename = body.get("filename") or ""
    b64 = body.get("content_base64")
    if not b64:
        raise HTTPException(status_code=400, detail="No file content received")
    try:
        raw_bytes = base64.b64decode(b64)
        parsed = parse_lion_workbook(raw_bytes, filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Couldn't read this file: {e}")
    if not parsed:
        raise HTTPException(status_code=400, detail="No vessel rows found in this file")

    by_key = {}
    for row in parsed:
        key = lion_name_key(row["vessel_name"])
        if not key:
            continue
        open_from, open_to = parse_lion_date_range(row["open_raw"])
        row["open_from"], row["open_to"] = open_from, open_to
        if key not in by_key:
            by_key[key] = row  # row order = recency (row 1 is the newest email); first occurrence wins, later duplicates are older re-listings

    now = datetime.now(timezone.utc).isoformat()
    added, updated = 0, 0
    async with SessionLocal() as db:
        for key, row in by_key.items():
            existing = (await db.execute(
                text("SELECT id, region, region_manual FROM lion_positions WHERE name_key=:k"), {"k": key}
            )).first()
            region = existing.region if (existing and existing.region_manual) else classify_lion_region(row["position"])
            if existing:
                await db.execute(
                    text(
                        "UPDATE lion_positions SET vessel_name=:name, built=:built, dwt=:dwt, position=:pos, "
                        "open_from=:of, open_to=:ot, comments=:c, region=:r, updated_at=:now WHERE id=:id"
                    ),
                    {"name": row["vessel_name"], "built": row["built"], "dwt": row["dwt"], "pos": row["position"],
                     "of": row["open_from"], "ot": row["open_to"], "c": row["comments"], "r": region,
                     "now": now, "id": existing.id},
                )
                updated += 1
            else:
                await db.execute(
                    text(
                        "INSERT INTO lion_positions (id, vessel_name, name_key, built, dwt, position, open_from, "
                        "open_to, comments, region, region_manual, updated_at, created_at) VALUES "
                        "(:id, :name, :key, :built, :dwt, :pos, :of, :ot, :c, :r, 0, :now, :now)"
                    ),
                    {"id": secrets.token_hex(12), "name": row["vessel_name"], "key": key, "built": row["built"],
                     "dwt": row["dwt"], "pos": row["position"], "of": row["open_from"], "ot": row["open_to"],
                     "c": row["comments"], "r": region, "now": now},
                )
                added += 1
        removed = await lion_purge_stale(db)
        await db.commit()
    return {"ok": True, "rows_in_file": len(parsed), "added": added, "updated": updated, "removed_stale": removed}


class LionRowBody(BaseModel):
    vessel_name: Optional[str] = None
    built: Optional[int] = None
    dwt: Optional[float] = None
    position: Optional[str] = None
    open_from: Optional[str] = None
    open_to: Optional[str] = None
    comments: Optional[str] = None
    region: Optional[str] = None


class LionRegionsBody(BaseModel):
    regions: list


@app.put("/api/lion-positions/regions")
async def set_lion_regions(body: LionRegionsBody, username: str = Depends(get_current_username)):
    async with SessionLocal() as db:
        existing_ids = {r.id for r in (await db.execute(text("SELECT id FROM lion_regions"))).all()}
        new_ids = {r["id"] for r in body.regions}
        for rid in existing_ids - new_ids:
            await db.execute(text("UPDATE lion_positions SET region=NULL WHERE region=:r"), {"r": rid})
            await db.execute(text("DELETE FROM lion_regions WHERE id=:id"), {"id": rid})
        await db.execute(text("DELETE FROM lion_regions"))
        await db.execute(
            text("INSERT INTO lion_regions (id, label, sort_order) VALUES (:id, :label, :o)"),
            [{"id": r["id"], "label": r["label"], "o": i} for i, r in enumerate(body.regions)],
        )
        await db.commit()
    return {"ok": True}


@app.put("/api/lion-positions/{row_id}")
async def update_lion_position(row_id: str, body: LionRowBody, username: str = Depends(get_current_username)):
    fields = body.dict(exclude_unset=True)
    if not fields:
        return {"ok": True}
    now = datetime.now(timezone.utc).isoformat()
    sets, params = [], {"id": row_id, "now": now}
    for k, v in fields.items():
        sets.append(f"{k}=:{k}")
        params[k] = v
    if "region" in fields:
        sets.append("region_manual=1")
    if "vessel_name" in fields:
        params["name_key"] = lion_name_key(fields["vessel_name"])
        sets.append("name_key=:name_key")
    sets.append("updated_at=:now")
    async with SessionLocal() as db:
        result = await db.execute(text(f"UPDATE lion_positions SET {', '.join(sets)} WHERE id=:id"), params)
        await db.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Row not found")
    return {"ok": True}


@app.delete("/api/lion-positions/{row_id}")
async def delete_lion_position(row_id: str, username: str = Depends(get_current_username)):
    async with SessionLocal() as db:
        result = await db.execute(text("DELETE FROM lion_positions WHERE id=:id"), {"id": row_id})
        await db.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Row not found")
    return {"ok": True}


# ══════════════════════════════════════════════════════════════════════════
# WA vessels ("CGL WA PS" tab) — vessel offers/positions that only ever show
# up in WhatsApp chats (SHIPS OFFERS & UPDATES, Fixing Team, personal
# contacts), never in email or AIS. One row = one offer/sighting. Unlike
# Lion SP (which purges stale rows), these are NEVER deleted — only their
# status changes, including the automatic "expired" sweep below — so the
# history of what was offered stays queryable.
# ══════════════════════════════════════════════════════════════════════════

WA_REGIONS = ["Danube inside", "Sulina", "Black Sea", "Marmara", "Aegean", "E.Med", "C.Med", "Other"]
WA_STATUSES = ["open", "on_subs", "fixed", "withdrawn", "expired"]
WA_STALE_DAYS = 20  # open_to (or open_from if no open_to) this many days in the past -> auto status "expired"

WA_FIELDS = [
    "name", "imo", "mmsi", "build_year", "flag", "dwt", "dwcc", "intake_mt", "grain_cbft",
    "bale_cbft", "draft_m", "holds", "gear", "position", "region", "open_from", "open_to",
    "restrictions", "freight_idea", "contact", "broker_company", "status", "source_chat",
    "source_sender", "source_date", "raw_text", "note",
]


def _wa_row_to_dict(row) -> dict:
    d = {"id": row.id, "created_at": row.created_at, "updated_at": row.updated_at}
    for f in WA_FIELDS:
        d[f] = getattr(row, f)
    return d


def wa_match_key(imo, name, dwt, build_year):
    """IMO is the reliable key when present. Otherwise fall back to a composite
    of normalized name + dwt (rounded to the nearest 1000t, to tolerate a
    figure quoted slightly differently across messages) + build year. If
    BOTH name and imo are absent (e.g. a cargo-seeking-tonnage request with no
    vessel named), there is nothing to match against — return None, meaning
    "always insert a new row, never try to upsert"."""
    imo = str(imo).strip() if imo else ""
    if imo:
        return "imo:" + imo
    name_key = lion_name_key(name) if name else ""
    if not name_key:
        return None
    dwt_r = ""
    if dwt not in (None, ""):
        try:
            dwt_r = str(round(float(dwt) / 1000) * 1000)
        except (TypeError, ValueError):
            dwt_r = ""
    year = ""
    if build_year not in (None, ""):
        try:
            year = str(int(build_year))
        except (TypeError, ValueError):
            year = ""
    return f"name:{name_key}:{dwt_r}:{year}"


async def wa_sweep_expired(db):
    """Lazy auto-expiry, run on every read: never deletes a row, just flips
    status to 'expired' once it's well past its open window — and only for
    rows still at the default 'open' status. A row anyone has manually moved
    to on_subs/fixed/withdrawn (or expired) is left alone — a manual status
    is a deliberate decision and a stale date must never silently overwrite
    it (e.g. a vessel put 'on_subs' days ago must not flip back to 'expired'
    just because its original open window has since passed)."""
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=WA_STALE_DAYS)).isoformat()
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        text(
            "UPDATE wa_vessels SET status='expired', updated_at=:now "
            "WHERE status='open' AND COALESCE(open_to, open_from) IS NOT NULL "
            "AND COALESCE(open_to, open_from) < :cutoff"
        ),
        {"cutoff": cutoff, "now": now},
    )


async def upsert_wa_vessel(db, fields: dict) -> dict:
    """Shared by the REST 'save draft' endpoint and the add_wa_vessel MCP tool.
    fields may have None for anything not known — those are simply not written
    (an update never blanks out a previously-known field with None; to clear a
    field on purpose use update_wa_vessel/PUT with an explicit empty string).
    Matches by wa_match_key(); a match updates that row in place (the newer
    message's fields win, so the same vessel re-listed over several days
    collapses into one current row instead of duplicating); no match inserts a
    new row. A fresh sighting of a vessel previously marked 'expired' revives
    it to 'open' unless the caller passed an explicit status."""
    key = wa_match_key(fields.get("imo"), fields.get("name"), fields.get("dwt"), fields.get("build_year"))
    now = datetime.now(timezone.utc).isoformat()
    existing = None
    if key:
        existing = (await db.execute(
            text("SELECT * FROM wa_vessels WHERE match_key=:k ORDER BY updated_at DESC LIMIT 1"), {"k": key}
        )).first()

    if existing:
        merged = {f: getattr(existing, f) for f in WA_FIELDS}
        for f in WA_FIELDS:
            if fields.get(f) is not None:
                merged[f] = fields[f]
        if fields.get("status") is None and merged["status"] == "expired":
            merged["status"] = "open"  # reappeared -> treat as live again unless told otherwise
        params = dict(merged)
        params["id"] = existing.id
        params["now"] = now
        params["match_key"] = key
        set_clause = ", ".join(f"{f}=:{f}" for f in WA_FIELDS) + ", updated_at=:now, match_key=:match_key"
        await db.execute(text(f"UPDATE wa_vessels SET {set_clause} WHERE id=:id"), params)
        row = (await db.execute(text("SELECT * FROM wa_vessels WHERE id=:id"), {"id": existing.id})).first()
        return _wa_row_to_dict(row)

    new_id = secrets.token_hex(12)
    params = {f: fields.get(f) for f in WA_FIELDS}
    params["status"] = params.get("status") or "open"
    params["id"] = new_id
    params["now"] = now
    params["match_key"] = key
    cols = ["id", "created_at", "updated_at", "match_key"] + WA_FIELDS
    placeholders = ["id", "now", "now", "match_key"] + WA_FIELDS
    await db.execute(
        text(f"INSERT INTO wa_vessels ({', '.join(cols)}) VALUES ({', '.join(':' + p for p in placeholders)})"),
        params,
    )
    row = (await db.execute(text("SELECT * FROM wa_vessels WHERE id=:id"), {"id": new_id})).first()
    return _wa_row_to_dict(row)


async def wa_update_by_id(db, row_id: str, fields: dict, allow_null: bool = False):
    """Direct edit by id (not match-key upsert) — used for explicit edits
    including status changes. Returns the updated row dict, or None if the
    id doesn't exist."""
    fields = {k: v for k, v in fields.items() if k in WA_FIELDS and (allow_null or v is not None)}
    if not fields:
        row = (await db.execute(text("SELECT * FROM wa_vessels WHERE id=:id"), {"id": row_id})).first()
        return _wa_row_to_dict(row) if row else None
    now = datetime.now(timezone.utc).isoformat()
    params = dict(fields)
    params["id"] = row_id
    params["now"] = now
    if "name" in fields or "imo" in fields or "dwt" in fields or "build_year" in fields:
        current = (await db.execute(text("SELECT * FROM wa_vessels WHERE id=:id"), {"id": row_id})).first()
        if current is None:
            return None
        merged = {f: getattr(current, f) for f in WA_FIELDS}
        merged.update(fields)
        params["match_key"] = wa_match_key(merged["imo"], merged["name"], merged["dwt"], merged["build_year"])
        set_clause = ", ".join(f"{f}=:{f}" for f in fields) + ", updated_at=:now, match_key=:match_key"
    else:
        set_clause = ", ".join(f"{f}=:{f}" for f in fields) + ", updated_at=:now"
    result = await db.execute(text(f"UPDATE wa_vessels SET {set_clause} WHERE id=:id"), params)
    if result.rowcount == 0:
        return None
    row = (await db.execute(text("SELECT * FROM wa_vessels WHERE id=:id"), {"id": row_id})).first()
    return _wa_row_to_dict(row)


WA_TEXT_COLUMNS = ("name", "imo", "flag", "holds", "gear", "position", "region", "restrictions",
                   "freight_idea", "contact", "broker_company", "note", "raw_text")


async def wa_query(db, status=None, region=None, dwt_min=None, dwt_max=None, max_draft=None,
                    open_before=None, inside_danube=None, q=None):
    clauses, params = [], {}
    # Free-text search: every whitespace-separated word must appear (case-insensitive)
    # somewhere in the descriptive columns — including restrictions, note, freight_idea
    # and the original message — so things like "bystroe" or "free days" are findable
    # even though they aren't dedicated fields.
    for i, term in enumerate((q or "").split()):
        key = f"q{i}"
        clauses.append("(" + " OR ".join(f"COALESCE({c},'') LIKE :{key} ESCAPE '\\'" for c in WA_TEXT_COLUMNS) + ")")
        params[key] = "%" + term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    if inside_danube:
        clauses.append("region = :region_danube")
        params["region_danube"] = "Danube inside"
    elif region:
        clauses.append("region = :region")
        params["region"] = region
    if status:
        clauses.append("status = :status")
        params["status"] = status
    if dwt_min is not None:
        clauses.append("dwt >= :dwt_min")
        params["dwt_min"] = dwt_min
    if dwt_max is not None:
        clauses.append("dwt <= :dwt_max")
        params["dwt_max"] = dwt_max
    if max_draft is not None:
        clauses.append("draft_m <= :max_draft")
        params["max_draft"] = max_draft
    if open_before:
        clauses.append("open_from <= :open_before")
        params["open_before"] = open_before
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = (await db.execute(text(f"SELECT * FROM wa_vessels {where} ORDER BY updated_at DESC"), params)).all()
    return [_wa_row_to_dict(r) for r in rows]


class WaVesselBody(BaseModel):
    name: Optional[str] = None
    imo: Optional[str] = None
    mmsi: Optional[str] = None
    build_year: Optional[int] = None
    flag: Optional[str] = None
    dwt: Optional[float] = None
    dwcc: Optional[float] = None
    intake_mt: Optional[float] = None
    grain_cbft: Optional[float] = None
    bale_cbft: Optional[float] = None
    draft_m: Optional[float] = None
    holds: Optional[str] = None
    gear: Optional[str] = None
    position: Optional[str] = None
    region: Optional[str] = None
    open_from: Optional[str] = None
    open_to: Optional[str] = None
    restrictions: Optional[str] = None
    freight_idea: Optional[str] = None
    contact: Optional[str] = None
    broker_company: Optional[str] = None
    status: Optional[str] = None
    source_chat: Optional[str] = None
    source_sender: Optional[str] = None
    source_date: Optional[str] = None
    raw_text: Optional[str] = None
    note: Optional[str] = None


class WaVesselUpdateBody(BaseModel):
    name: Optional[str] = None
    imo: Optional[str] = None
    mmsi: Optional[str] = None
    build_year: Optional[int] = None
    flag: Optional[str] = None
    dwt: Optional[float] = None
    dwcc: Optional[float] = None
    intake_mt: Optional[float] = None
    grain_cbft: Optional[float] = None
    bale_cbft: Optional[float] = None
    draft_m: Optional[float] = None
    holds: Optional[str] = None
    gear: Optional[str] = None
    position: Optional[str] = None
    region: Optional[str] = None
    open_from: Optional[str] = None
    open_to: Optional[str] = None
    restrictions: Optional[str] = None
    freight_idea: Optional[str] = None
    contact: Optional[str] = None
    broker_company: Optional[str] = None
    status: Optional[str] = None
    note: Optional[str] = None


@app.get("/api/wa-vessels")
async def list_wa_vessels_rest(
    status: Optional[str] = None, region: Optional[str] = None, dwt_min: Optional[float] = None,
    dwt_max: Optional[float] = None, max_draft: Optional[float] = None, open_before: Optional[str] = None,
    inside_danube: Optional[bool] = None, q: Optional[str] = None,
    username: str = Depends(get_current_username),
):
    async with SessionLocal() as db:
        await wa_sweep_expired(db)
        await db.commit()
        rows = await wa_query(db, status=status, region=region, dwt_min=dwt_min, dwt_max=dwt_max,
                               max_draft=max_draft, open_before=open_before, inside_danube=inside_danube, q=q)
    return {"vessels": rows, "regions": WA_REGIONS, "statuses": WA_STATUSES}


@app.post("/api/wa-vessels")
async def create_wa_vessel(body: WaVesselBody, username: str = Depends(get_current_username)):
    """Save a reviewed draft row from the "paste a WhatsApp message" box (after the
    frontend's own AI-parse step). Upserts by match key — see upsert_wa_vessel."""
    async with SessionLocal() as db:
        row = await upsert_wa_vessel(db, body.dict())
        await db.commit()
    return row


@app.put("/api/wa-vessels/{row_id}")
async def update_wa_vessel_rest(row_id: str, body: WaVesselUpdateBody, username: str = Depends(get_current_username)):
    fields = body.dict(exclude_unset=True)
    async with SessionLocal() as db:
        row = await wa_update_by_id(db, row_id, fields, allow_null=True)
        await db.commit()
        if row is None:
            raise HTTPException(status_code=404, detail="Row not found")
    return row
