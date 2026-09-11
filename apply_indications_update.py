#!/usr/bin/env python3
"""
One-time migration: apply the rate refresh discussed in the "Indications"
chat to the live CGL dashboard.

What it does, precisely:
  1. Updates the Danube -> Egypt Med wheat route to 93-100 (confirmed 02.09.26 fixture)
  2. Adds 3 new wheat routes ex Romanian/Moldovan ports (no Danube war premium):
     Reni/Orlivka -> Constanta, Braila -> Larnaca, Constanta -> Marmara
  3. Adds a new cargo "Sunflower Seeds (SF ~75)" with 4 routes ex Reni (Marmara/Mersin, 3k/5k lots)
  4. Adds a new cargo "Soybean / Sunflower Meal (SF ~56-58)" with 1 route to Poti (flagged stale)
  5. Adds a new backhaul route under Fertilizers: Prahovo (Serbia) -> Giurgiulesti

It fetches the CURRENT board first and only adds/updates these specific
items - your voyages, notes, AI history, and any other cargo/routes you
already have are left untouched. Safe to re-run: it checks for existing
labels/ids before adding, so it will not create duplicates.

Usage:
    python3 apply_indications_update.py
It will prompt for your dashboard username and password (input is hidden).
"""
import getpass
import json
import urllib.error
import urllib.request
from datetime import date

API_BASE = "https://94-136-184-214.sslip.io"


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


def today():
    return date.today().strftime("%d.%m.%y")


def main():
    username = input("Dashboard username: ")
    password = getpass.getpass("Dashboard password: ")

    login = api_call("POST", "/api/login", body={"username": username, "password": password})
    token = login["token"]
    print("Logged in as", login["username"])

    state_resp = api_call("GET", "/api/state", token=token)
    state = state_resp.get("value") or {}
    board = state.setdefault("board", {})
    cargo_data = board.setdefault("cargoData", {})

    changes = []

    # 1 + 2: wheat updates
    wheat = cargo_data.get("wheat")
    if wheat:
        for r in wheat["routes"]:
            if r["id"] == "w2" and (r["low"], r["high"]) != (93, 100):
                r["low"], r["high"] = 93, 100
                r["updatedAt"] = "02.09.26"
                r.setdefault("ownersIdeas", []).insert(0, {
                    "text": "Confirmed real fixture 02.09.26",
                    "source": "internal fixture log / chat",
                    "date": "02.09.26",
                })
                changes.append("Updated Danube->Egypt Med wheat range to 93-100")

        existing_labels = {r["label"] for r in wheat["routes"]}
        new_wheat_routes = [
            {
                "id": "w8", "label": "Reni/Orlivka \u2192 Constanta (barges)",
                "direction": "outbound", "low": 45, "high": 50, "unit": "$/mt",
                "ownersIdeas": [{"text": "Updated indication, wheat via barge, Danube-Constanta corridor (estimate)",
                                 "source": "internal chat log", "date": "05.09.26"}],
                "charterersIdeas": [], "updatedAt": "05.09.26",
            },
            {
                "id": "w9", "label": "Braila \u2192 Larnaca",
                "direction": "outbound", "low": 32, "high": 34, "unit": "$/mt",
                "ownersIdeas": [{"text": "Calibrated reference rate - no Danube war premium on this port",
                                 "source": "internal calibration, broker management", "date": "09.26"}],
                "charterersIdeas": [], "updatedAt": "09.26",
            },
            {
                "id": "w10", "label": "Constanta \u2192 Marmara",
                "direction": "outbound", "low": 40, "high": 45, "unit": "$/mt",
                "ownersIdeas": [{"text": "6,000t wheat; calibrated reference rate - no Danube war premium on this port",
                                 "source": "internal calibration, broker management", "date": "09.26"}],
                "charterersIdeas": [], "updatedAt": "09.26",
            },
        ]
        for nr in new_wheat_routes:
            if nr["label"] not in existing_labels:
                wheat["routes"].append(nr)
                changes.append(f"Added wheat route: {nr['label']}")

    # 3: sunflower seeds cargo
    if "sunflower_seeds" not in cargo_data:
        note = "Calculated estimate - stowage-factor premium over grain benchmark, extrapolated from 26.07 base"
        cargo_data["sunflower_seeds"] = {
            "label": "Sunflower Seeds (SF ~75)",
            "routes": [
                {"id": "ss1", "label": "Reni \u2192 Marmara (3,000t lot)", "direction": "outbound",
                 "low": 65, "high": 70, "unit": "$/mt",
                 "ownersIdeas": [{"text": note, "source": "internal calculation", "date": "01.09.26"}],
                 "charterersIdeas": [], "updatedAt": "01.09.26"},
                {"id": "ss2", "label": "Reni \u2192 Marmara (5,000t lot)", "direction": "outbound",
                 "low": 59, "high": 64, "unit": "$/mt",
                 "ownersIdeas": [{"text": note, "source": "internal calculation", "date": "01.09.26"}],
                 "charterersIdeas": [], "updatedAt": "01.09.26"},
                {"id": "ss3", "label": "Reni \u2192 Mersin (3,000t lot)", "direction": "outbound",
                 "low": 76, "high": 81, "unit": "$/mt",
                 "ownersIdeas": [{"text": note, "source": "internal calculation", "date": "01.09.26"}],
                 "charterersIdeas": [], "updatedAt": "01.09.26"},
                {"id": "ss4", "label": "Reni \u2192 Mersin (5,000t lot)", "direction": "outbound",
                 "low": 69, "high": 74, "unit": "$/mt",
                 "ownersIdeas": [{"text": note, "source": "internal calculation", "date": "01.09.26"}],
                 "charterersIdeas": [], "updatedAt": "01.09.26"},
            ],
        }
        changes.append("Added new cargo: Sunflower Seeds (SF ~75), 4 routes")

    # 4: soybean/sunflower meal cargo
    if "meal" not in cargo_data:
        cargo_data["meal"] = {
            "label": "Soybean / Sunflower Meal (SF ~56-58)",
            "routes": [
                {"id": "m1", "label": "Ukraine (Izmail/Reni/Orlivka/Galati) \u2192 Poti, Georgia",
                 "direction": "outbound", "low": 31, "high": 33, "unit": "$/mt",
                 "ownersIdeas": [],
                 "charterersIdeas": [{
                     "text": "Real fixtures, 3,000-5,500t lots, sf56-57 - dated Mar-Jun 2026, likely outdated given market volatility - verify before quoting",
                     "source": "CRM freight database (own fixtures)", "date": "05.06.26 (stale)"}],
                 "updatedAt": "05.06.26 (stale, verify)"},
            ],
        }
        changes.append("Added new cargo: Soybean / Sunflower Meal (SF ~56-58), 1 route (flagged stale)")

    # 5: fertilizers backhaul addition
    fert = cargo_data.get("fertilizers")
    if fert:
        labels = {r["label"] for r in fert["routes"]}
        label = "Prahovo (Serbia) \u2192 Giurgiulesti (backhaul)"
        if label not in labels:
            fert["routes"].append({
                "id": "f4", "label": label,
                "direction": "backhaul", "low": 47, "high": 56, "unit": "\u20ac/mt",
                "ownersIdeas": [],
                "charterersIdeas": [{"text": "Fertilizer in big bags (NPK non-ADR), 3,000t",
                                      "source": "internal chat log", "date": "04.09.26"}],
                "updatedAt": "04.09.26",
            })
            changes.append(f"Added fertilizers backhaul route: {label}")

    if not changes:
        print("Nothing to change - looks like this has already been applied.")
        return

    api_call("PUT", "/api/state", token=token, body={"value": state})
    print("\nDone. Changes applied:")
    for c in changes:
        print(" -", c)


if __name__ == "__main__":
    main()
