#!/usr/bin/env python3
"""
CGL Indications — user management CLI.

Run this directly on the server (e.g. via Termius/SSH) to create, list,
remove accounts, or change a password for the dashboard's login. This does
NOT require the API service (cgl-api.service) to be running — it talks to
the same SQLite file directly.

There is no public "sign up" page in the dashboard on purpose — accounts
are only created here, by whoever has server access, and handed out
manually to whoever needs one.

Usage:
    python3 manage_users.py add <username>        # prompts for password
    python3 manage_users.py list
    python3 manage_users.py remove <username>
    python3 manage_users.py passwd <username>      # change password, prompts

Example:
    cd /srv/cgl
    source venv/bin/activate
    python3 manage_users.py add broker1
    python3 manage_users.py list
    deactivate
"""
import getpass
import hashlib
import os
import secrets
import sqlite3
import sys
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "dashboard.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

PBKDF2_ITERATIONS = 200_000

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    salt TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    return conn


def hash_password(password, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), PBKDF2_ITERATIONS)
    return dk.hex(), salt


def prompt_password(confirm=True):
    pw = getpass.getpass("Password: ")
    if confirm:
        pw2 = getpass.getpass("Confirm password: ")
        if pw != pw2:
            print("Passwords do not match.")
            return None
    if len(pw) < 6:
        print("Password should be at least 6 characters.")
        return None
    return pw


def cmd_add(username):
    conn = get_conn()
    if conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone():
        print(f"User '{username}' already exists. Use 'passwd {username}' to change the password.")
        return
    pw = prompt_password()
    if pw is None:
        return
    h, salt = hash_password(pw)
    conn.execute(
        "INSERT INTO users (username, password_hash, salt, created_at) VALUES (?,?,?,?)",
        (username, h, salt, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    print(f"User '{username}' created.")


def cmd_list():
    conn = get_conn()
    rows = conn.execute("SELECT username, created_at FROM users ORDER BY created_at").fetchall()
    if not rows:
        print("No users yet.")
        return
    for u, c in rows:
        print(f"{u}\t(created {c})")


def cmd_remove(username):
    conn = get_conn()
    cur = conn.execute("DELETE FROM users WHERE username=?", (username,))
    conn.commit()
    print("Removed." if cur.rowcount else f"User '{username}' not found.")


def cmd_passwd(username):
    conn = get_conn()
    if not conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone():
        print(f"User '{username}' not found.")
        return
    pw = prompt_password()
    if pw is None:
        return
    h, salt = hash_password(pw)
    conn.execute("UPDATE users SET password_hash=?, salt=? WHERE username=?", (h, salt, username))
    conn.commit()
    print("Password updated.")


def main():
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "add":
        cmd_add(args[1])
    elif len(args) == 1 and args[0] == "list":
        cmd_list()
    elif len(args) == 2 and args[0] == "remove":
        cmd_remove(args[1])
    elif len(args) == 2 and args[0] == "passwd":
        cmd_passwd(args[1])
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
