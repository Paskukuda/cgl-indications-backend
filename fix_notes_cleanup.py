#!/usr/bin/env python3
"""
One-time cleanup of the dashboard's Manual note log:

1. Removes 2 notes that are technical/process reports, not freight facts:
   - "Полная синхронизация 10-11.09.26: добавлен груз..."
   - "Обновление 10-11.09.26: Marmara wheat поднята..."

2. Replaces the long "Стандарт на будущее..." note with 3 dry, fact-only
   lines (vessel / fixture / rate / cargo / route), dropping the
   style-guide commentary.

3. Adds one more dry fact that was buried inside the deleted
   "Полная синхронизация" note (a real corn fixture), correcting the
   vessel name typo "Пропуск" -> M/V PROPUS.

4. Leaves the other, already-dry notes untouched.

Matches existing notes by a distinctive text prefix, so it's safe to
re-run — anything already removed/added won't be duplicated or fail.

Usage:
    python3 fix_notes_cleanup.py
It will prompt for your dashboard username and password (input hidden).
"""
import getpass
import json
import secrets
import urllib.error
import urllib.request

API_BASE = "https://94-136-184-214.sslip.io"

REMOVE_PREFIXES = [
    "Полная синхронизация 10-11.09.26",
    "Обновление 10-11.09.26",
]
REPLACE_PREFIX = "Стандарт на будущее"
REPLACEMENT_NOTES = [
    {"text": "M/V AGN LAGERTHA — Marmara wheat $95/mt — MHP — 09-10.09.26", "ts": "11.09.26 12:37"},
    {"text": "M/V GREEN LINE — Marmara wheat $93.50/mt — GRANOLA — 09-10.09.26", "ts": "11.09.26 12:37"},
    {"text": "Bandirma — wheat 3,000t $94.85/mt — Owner Indication — 09-10.09.26", "ts": "11.09.26 12:37"},
]
EXTRA_NOTE = {"text": "Corn Izmail\u2192Larnaca 6,000t $105/mt — M/V PROPUS — 10-11.09.26", "ts": "11.09.26 11:55"}
EXTRA_NOTE_MARKER = "Corn Izmail\u2192Larnaca 6,000t $105/mt"  # exact-ish prefix, not just "M/V PROPUS" (which also appears inside the long note being replaced, as a style example)


def api_call(method, path, token=None, body=None):
    url = API_BASE + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print("ERROR", e.code, e.read().decode("utf-8"))
        raise


def main():
    username = input("Dashboard username: ")
    password = getpass.getpass("Dashboard password: ")

    login = api_call("POST", "/api/login", body={"username": username, "password": password})
    token = login["token"]
    print("Logged in as", login["username"])

    state_resp = api_call("GET", "/api/state", token=token)
    state = state_resp.get("value") or {}
    board = state.setdefault("board", {})
    notes = board.setdefault("notes", [])

    before = len(notes)
    already_has_extra = any(EXTRA_NOTE_MARKER in n.get("text", "") for n in notes)

    kept = []
    removed_count = 0
    replaced = False
    for n in notes:
        txt = n.get("text", "")
        if any(txt.startswith(p) for p in REMOVE_PREFIXES):
            removed_count += 1
            continue
        if txt.startswith(REPLACE_PREFIX):
            replaced = True
            continue
        kept.append(n)

    new_notes = list(kept)
    if replaced:
        for rn in reversed(REPLACEMENT_NOTES):
            new_notes.insert(0, {"id": "nt" + secrets.token_hex(6), "text": rn["text"], "ts": rn["ts"]})
        print(f"Replaced the long note with {len(REPLACEMENT_NOTES)} dry facts.")
    else:
        print("Long 'Стандарт на будущее' note not found (already handled?) — skipped.")

    if not already_has_extra:
        insert_at = len(REPLACEMENT_NOTES) if replaced else 0
        new_notes.insert(insert_at, {"id": "nt" + secrets.token_hex(6), "text": EXTRA_NOTE["text"], "ts": EXTRA_NOTE["ts"]})
        print("Added the corn fixture fact (M/V PROPUS).")
    else:
        print("Corn fixture fact already present — skipped.")

    board["notes"] = new_notes

    if removed_count:
        print(f"Removed {removed_count} technical-work note(s).")
    else:
        print("No technical-work notes found to remove (already handled?).")

    print(f"\nNotes before: {before}, after: {len(new_notes)}")

    api_call("PUT", "/api/state", token=token, body={"value": state})
    print("Saved.")


if __name__ == "__main__":
    main()
