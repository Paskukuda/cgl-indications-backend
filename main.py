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
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.responses import JSONResponse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
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
        created_at TEXT NOT NULL,
        created_by TEXT
    )
    """,
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
        await db.commit()
    return now


def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%d.%m.%y")


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
async def add_route(cargo_id: str, label: str, direction: str, low: float, high: float, unit: str = "$/mt") -> dict:
    """Add a new direction/route under an existing cargo type.
    direction must be 'outbound' (ex-Ukraine export) or 'backhaul' (import into Ukraine/CVB)."""
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
    cargo["routes"].append(route)
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
async def list_documents_tool() -> list:
    """List knowledge-base documents (freight reports/circulars) saved on the
    dashboard, including their full text content."""
    async with SessionLocal() as db:
        rows = (
            await db.execute(
                text("SELECT id, title, content, source, created_at, created_by FROM documents ORDER BY created_at DESC")
            )
        ).all()
        return [
            {"id": r.id, "title": r.title, "content": r.content, "source": r.source,
             "created_at": r.created_at, "created_by": r.created_by}
            for r in rows
        ]


@mcp_server.tool()
async def add_document_tool(title: str, content: str, source: str = "") -> dict:
    """Save a new freight report/circular into the dashboard's knowledge base."""
    if not title.strip() or not content.strip():
        return {"error": "title and content are required"}
    doc_id = secrets.token_urlsafe(12)
    now = datetime.now(timezone.utc).isoformat()
    async with SessionLocal() as db:
        await db.execute(
            text(
                "INSERT INTO documents (id, title, content, source, created_at, created_by) "
                "VALUES (:id, :t, :c, :s, :ca, :cb)"
            ),
            {"id": doc_id, "t": title.strip(), "c": content, "s": (source or "").strip() or None,
             "ca": now, "cb": "mcp-agent"},
        )
        await db.commit()
    return {"id": doc_id, "created_at": now}


mcp_asgi_app = mcp_server.streamable_http_app()


class MCPAuthMiddleware:
    """Single static bearer token, checked on every request to the mounted MCP
    app. Matches Claude.ai's custom-connector "None + Request headers" mode:
    no OAuth handshake, just a fixed header value Claude attaches to every
    call. Token lives in data/mcp_token.txt (see _get_or_create_mcp_token)."""

    def __init__(self, wrapped_app, token: str):
        self.wrapped_app = wrapped_app
        self.token = token

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope.get("headers") or [])
            auth = headers.get(b"authorization", b"").decode()
            if auth != f"Bearer {self.token}":
                response = JSONResponse({"error": "unauthorized"}, status_code=401)
                await response(scope, receive, send)
                return
        await self.wrapped_app(scope, receive, send)


mcp_asgi_app_protected = MCPAuthMiddleware(mcp_asgi_app, MCP_TOKEN)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with mcp_server.session_manager.run():
        async with engine.begin() as conn:
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            for stmt in SCHEMA_STATEMENTS:
                await conn.execute(text(stmt))
        yield


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


@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "1.1.0"}


@app.post("/api/login")
async def login(body: LoginBody):
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
    now = await save_app_state(body.value, username)
    return {"ok": True, "updated_at": now, "updated_by": username}


# ── Knowledge base documents ──
# Freight circulars, weekly reports, cargo-order lists, etc. that the broker
# pastes in. Stored as plain rows (not part of the app_state JSON blob,
# since they can be sizeable and are logically separate). The AI panel pulls
# these in as extra context alongside mail search and web search.
class DocumentBody(BaseModel):
    title: str
    content: str
    source: Optional[str] = None


@app.get("/api/documents")
async def list_documents(username: str = Depends(get_current_username)):
    async with SessionLocal() as db:
        rows = (
            await db.execute(
                text(
                    "SELECT id, title, content, source, created_at, created_by "
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
    async with SessionLocal() as db:
        await db.execute(
            text(
                "INSERT INTO documents (id, title, content, source, created_at, created_by) "
                "VALUES (:id, :t, :c, :s, :ca, :cb)"
            ),
            {
                "id": doc_id,
                "t": body.title.strip(),
                "c": body.content,
                "s": (body.source or "").strip() or None,
                "ca": now,
                "cb": username,
            },
        )
        await db.commit()
    return {"id": doc_id, "created_at": now}


@app.delete("/api/documents/{doc_id}")
async def delete_document(doc_id: str, username: str = Depends(get_current_username)):
    async with SessionLocal() as db:
        result = await db.execute(text("DELETE FROM documents WHERE id=:id"), {"id": doc_id})
        await db.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Document not found")
    return {"ok": True}
