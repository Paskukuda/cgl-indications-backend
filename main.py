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
"""
import hashlib
import hmac
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "dashboard.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
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

app = FastAPI(title="CGL Indications API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

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
]


@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA journal_mode=WAL"))
        for stmt in SCHEMA_STATEMENTS:
            await conn.execute(text(stmt))


# ── password hashing (stdlib only — no extra dependency needed) ──
def hash_password(password: str, salt: Optional[str] = None) -> Tuple[str, str]:
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), PBKDF2_ITERATIONS)
    return dk.hex(), salt


def verify_password(password: str, salt: str, expected_hash: str) -> bool:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), PBKDF2_ITERATIONS)
    return hmac.compare_digest(dk.hex(), expected_hash)


class LoginBody(BaseModel):
    username: str
    password: str


class StateBody(BaseModel):
    value: dict


@app.get("/api/health")
async def health():
    return {"status": "ok"}


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
    now = datetime.now(timezone.utc).isoformat()
    payload = json.dumps(body.value)
    async with SessionLocal() as db:
        await db.execute(
            text(
                """
                INSERT INTO kv_store (key, value, updated_at, updated_by)
                VALUES ('app_state', :v, :t, :u)
                ON CONFLICT(key) DO UPDATE SET value=:v, updated_at=:t, updated_by=:u
                """
            ),
            {"v": payload, "t": now, "u": username},
        )
        await db.commit()
    return {"ok": True, "updated_at": now, "updated_by": username}
