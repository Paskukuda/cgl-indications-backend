#!/usr/bin/env python3
"""
List / restore earlier saves of the dashboard's board+notes data.
Runs directly against the server's database (no login needed).

    python3 restore_state_version.py            # list the most recent saves
    python3 restore_state_version.py 123        # put save #123 back as the current data

The current data is saved to the history first, so a restore can itself be undone.
Open dashboard pages notice the change on their own (or press Refresh).
"""
import json
import os
import sqlite3
import sys
import zlib
from datetime import datetime, timezone

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "dashboard.db")


def main():
    conn = sqlite3.connect(DB)
    if len(sys.argv) < 2:
        rows = conn.execute("SELECT id, updated_at, updated_by, size FROM state_history ORDER BY id DESC LIMIT 25").fetchall()
        if not rows:
            print("No saves recorded yet.")
        for i, t, u, size in rows:
            print(f"#{i:<5} {t[:19].replace('T', ' ')} UTC   by {u or '?':<12} {size or 0:>8,} bytes")
        print("\nRestore one with:  python3 restore_state_version.py <number>")
        return
    vid = int(sys.argv[1])
    row = conn.execute("SELECT value, updated_at FROM state_history WHERE id=?", (vid,)).fetchone()
    if not row:
        sys.exit(f"No save #{vid}.")
    restored = zlib.decompress(row[0]).decode("utf-8")
    json.loads(restored)  # sanity: must be valid JSON before it goes live
    now = datetime.now(timezone.utc).isoformat()
    current = conn.execute("SELECT value FROM kv_store WHERE key='app_state'").fetchone()
    if current:  # keep what is being replaced, so this restore is reversible
        conn.execute("INSERT INTO state_history (updated_at, updated_by, size, value) VALUES (?,?,?,?)",
                     (now, "before-restore", len(current[0]), zlib.compress(current[0].encode("utf-8"), 6)))
    conn.execute(
        "INSERT INTO kv_store (key, value, updated_at, updated_by) VALUES ('app_state', ?, ?, 'restore') "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at, updated_by=excluded.updated_by",
        (restored, now))
    conn.execute("INSERT INTO state_history (updated_at, updated_by, size, value) VALUES (?,?,?,?)",
                 (now, "restore", len(restored), zlib.compress(restored.encode("utf-8"), 6)))
    conn.commit()
    print(f"Restored save #{vid} (from {row[1][:19].replace('T', ' ')} UTC).")


if __name__ == "__main__":
    main()
