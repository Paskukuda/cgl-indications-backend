#!/usr/bin/env python3
"""
One-time fix: re-assign the correct category to the 5 IND FAQ knowledge-base
documents, which were all saved without a category and so defaulted to
"General Information".

Title -> category mapping applied:
    "Общая рамка"     -> general      (stays — it IS the general framework doc)
    "Реальные данные" -> real
    "Claude расчёт"   -> claude_calc
    "Lumpsum расчёт"  -> lumpsum
    "IND FAQ"         -> general      (stays — it's the meta/description doc)

Matches by exact title. Safe to re-run: if a document is already in the
right category, it's left alone and reported as "already correct".

Usage:
    python3 fix_ind_faq_categories.py
It will prompt for your dashboard username and password (input hidden).
"""
import getpass
import json
import urllib.error
import urllib.request

API_BASE = "https://94-136-184-214.sslip.io"

TITLE_TO_CATEGORY = {
    "Общая рамка": "general",
    "Реальные данные": "real",
    "Claude расчёт": "claude_calc",
    "Lumpsum расчёт": "lumpsum",
    "IND FAQ": "general",
}


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

    docs = api_call("GET", "/api/documents", token=token)["documents"]

    changed = 0
    for title, target_cat in TITLE_TO_CATEGORY.items():
        matches = [d for d in docs if d["title"] == title]
        if not matches:
            print(f"- not found: '{title}' (skipped)")
            continue
        for doc in matches:
            if doc.get("category") == target_cat:
                print(f"- '{title}' already category={target_cat}, no change")
                continue
            api_call("PUT", f"/api/documents/{doc['id']}", token=token, body={"category": target_cat})
            print(f"- '{title}' -> category={target_cat} (was {doc.get('category')})")
            changed += 1

    print(f"\nDone. Updated {changed} document(s).")


if __name__ == "__main__":
    main()
