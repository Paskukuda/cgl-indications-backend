#!/usr/bin/env python3
"""
One-time restructuring:

1. Splits the "meal" cargo (mislabeled "Soybean/Sunflower Meal (SF 54-58)"
   but actually containing a mix of SF58 Poti routes, SF60 pellets, and
   SF75 non-pelleted routes) into two honestly-labeled cargoes:
     - meal_pellets:     "Meal — Pellets (SF ~58-60)"
     - meal_nonpelleted: "Meal — Non-pelleted (SF ~75)"
   Route labels lose their "(Pellets SF60)"/"(Non-pelleted SF75)" suffix
   since that's now the cargo itself, not a per-route qualifier.

2. Tags every route across every cargo with its best-known tonnage
   bracket (3000 / 5000-6000 / 6000-8000 / 8000-10000), inferred from the
   quantities already mentioned in each route's real fixtures/ideas where
   available, defaulting to 5000-6000 otherwise.

3. Generates the other 3 bracket variants for every route, using the
   documented lot-premium relationship (smaller lot ~ +4-8% vs a larger
   one; here approximated as a flat 6%-per-bracket-step multiplier
   outward from whichever bracket already has real data), clearly
   labelled as an estimate with the formula named as its source. Where
   real ladder data already exists for a route (Bandirma BB fertilizer),
   that overrides the formula.

Safe to re-run: skips any route that already has a `lot` field set, and
skips the meal split if "meal_pellets"/"meal_nonpelleted" already exist.

Usage:
    python3 add_tonnage_brackets.py
It will prompt for your dashboard username and password (input hidden).
"""
import getpass
import json
import secrets
import urllib.error
import urllib.request

API_BASE = "https://94-136-184-214.sslip.io"

LOT_ORDER = ["3000", "5000-6000", "6000-8000", "8000-10000"]
STEP_PCT = 0.06  # documented ~4-8% smaller-lot premium per bracket step

# route id -> anchor bracket (where the existing real rate belongs)
ANCHOR_MAP = {
    # wheat
    "w1": "3000",            # Danube->Marmara: fixtures explicitly "min 3000mt"
    "w2": "5000-6000",       # Egypt/Mersin/Iskenderun: 5500t idea
    "w3": "5000-6000",       # Greece
    "w4": "5000-6000",       # Cyprus/Famagusta
    "w5": "5000-6000",       # Lebanon/Syria
    "w6": "5000-6000",       # Tunisia
    "w7": "6000-8000",       # EC Italy: PARESA fixture 6400mt
    "r110e38e03d5a": "3000", # Reni/Izmail/Orlivka -> Constanta: "3000-5000t"
    # corn
    "c1": "5000-6000",       # Larnaca: PROPUS fixture 6000t
    # fertilizers
    "f1": "3000",            # East Med: 3,000mt
    "f2": "3000",            # Egypt Med: 3,000mt
    "f3": "5000-6000",       # Trabzon: 4000-4800t urea
    "r89d350c4fbb3": "5000-6000",  # Aqaba -> Izmail/Reni: 5000t
    "r7be6755b2a3e": "5000-6000",  # Aqaba -> Constanta
    "rffdf03a48d00": "5000-6000",  # Aqaba -> Gdansk direct
    "rc724a34afaf6": "5000-6000",  # Aqaba -> Gdansk backhaul
    "rf4b7385b99af": "5000-6000",  # Samsun bulk
    "r78b9b1fd3226": "3000",       # Bandirma BB (special-cased below)
    "r309985bde9e8": "3000",       # Prahovo BB: 3000t NPK
    # salt
    "s1": "5000-6000",       # El Arish: 5,000-6,000t
    "s2": "6000-8000",       # Alexandria: 6,000-7,000t
}
DEFAULT_ANCHOR = "5000-6000"

# route id -> {bracket: (low, high)} — real ladder data, overrides the formula
SPECIAL_LADDERS = {
    "r78b9b1fd3226": {  # Bandirma (Turkey) -> Izmail/Reni/Orlivka BB
        "3000": (46, 48),
        "5000-6000": (40, 44),
        "6000-8000": (37, 40),
        "8000-10000": (34, 37),
    },
}

MEAL_PELLETS_ROUTE_IDS = {
    "re41a3379aeb8",  # Izmail/Orlivka -> Poti
    "rcf6bcd64982e",  # Galati -> Poti
    "r183db695777d",  # Marmara Pellets SF60
    "ra2ae3cd9f57f",  # Cyprus Pellets SF60
    "r41b647940a1e",  # Egypt Pellets SF60
    "r6a5c59ab71d2",  # Israel Pellets SF60
    "r6af3b1fbe9e6",  # Crete Pellets SF60
}
MEAL_NONPELLETED_ROUTE_IDS = {
    "r8a61d1d2d46e",  # Marmara Non-pelleted SF75
    "r951866c28e14",  # Cyprus Non-pelleted SF75
    "r5040021480b5",  # Egypt Non-pelleted SF75
    "r76d4315f17b1",  # Israel Non-pelleted SF75
    "r99c8f95d1b9f",  # Crete Non-pelleted SF75
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


def clean_label(label):
    for suffix in (" (Pellets SF60)", " (Non-pelleted SF75)"):
        if label.endswith(suffix):
            return label[: -len(suffix)]
    return label


def new_id():
    return "r" + secrets.token_hex(6)


def bracket_rate(anchor_bracket, anchor_low, anchor_high, target_bracket):
    a_idx = LOT_ORDER.index(anchor_bracket)
    t_idx = LOT_ORDER.index(target_bracket)
    mult = 1 + STEP_PCT * (a_idx - t_idx)
    return round(anchor_low * mult, 1), round(anchor_high * mult, 1)


def expand_route(route, anchor_bracket):
    """Returns the list of route dicts (one per LOT_ORDER bracket). Real
    ladder data (SPECIAL_LADDERS) always wins over both the anchor's raw
    rate and the generic formula, for every bracket it covers."""
    out = []
    special = SPECIAL_LADDERS.get(route["id"], {})
    for bracket in LOT_ORDER:
        if bracket in special:
            low, high = special[bracket]
        elif bracket == anchor_bracket:
            low, high = route["low"], route["high"]
        else:
            low, high = bracket_rate(anchor_bracket, route["low"], route["high"], bracket)

        if bracket == anchor_bracket and bracket not in special:
            r = dict(route)
            r["lot"] = bracket
        else:
            is_ladder = bracket in special
            note = (
                "Real ladder data for this lot size (from the shipowner's own firm idea)"
                if is_ladder
                else f"Calculated: stepped {int(STEP_PCT*100)}%/bracket from the {anchor_bracket}t anchor rate (estimate)"
            )
            r = {
                "id": new_id(),
                "label": route["label"],
                "direction": route["direction"],
                "lot": bracket,
                "low": low,
                "high": high,
                "unit": route.get("unit", "$/mt"),
                "ownersIdeas": [{"text": note, "source": "Lot-bracket calculation", "date": "16.09.26"}],
                "charterersIdeas": [],
                "updatedAt": "16.09.26",
            }
        out.append(r)
    return out


def process_cargo_routes(routes):
    new_routes = []
    for route in routes:
        if "lot" in route and route["lot"]:
            new_routes.append(route)  # already processed, idempotent skip
            continue
        anchor = ANCHOR_MAP.get(route["id"], DEFAULT_ANCHOR)
        new_routes.extend(expand_route(route, anchor))
    return new_routes


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

    # 1. Split "meal" cargo, if not already split
    if "meal" in cargo_data and "meal_pellets" not in cargo_data and "meal_nonpelleted" not in cargo_data:
        old_meal = cargo_data.pop("meal")
        pellets_routes, nonpell_routes = [], []
        for r in old_meal["routes"]:
            r = dict(r)
            r["label"] = clean_label(r["label"])
            if r["id"] in MEAL_NONPELLETED_ROUTE_IDS:
                nonpell_routes.append(r)
            else:
                pellets_routes.append(r)  # Poti routes + Pellets SF60 routes
        cargo_data["meal_pellets"] = {"label": "Meal \u2014 Pellets (SF ~58-60)", "routes": pellets_routes}
        cargo_data["meal_nonpelleted"] = {"label": "Meal \u2014 Non-pelleted (SF ~75)", "routes": nonpell_routes}
        changes.append(f"Split 'meal' into meal_pellets ({len(pellets_routes)} routes) and meal_nonpelleted ({len(nonpell_routes)} routes)")
    else:
        print("Meal cargo already split (or not present) \u2014 skipped.")

    # 2+3. Tag + expand every cargo's routes into all 4 lot brackets
    total_before = sum(len(c["routes"]) for c in cargo_data.values())
    for cid, cargo in cargo_data.items():
        cargo["routes"] = process_cargo_routes(cargo["routes"])
    total_after = sum(len(c["routes"]) for c in cargo_data.values())
    changes.append(f"Routes before: {total_before}, after tonnage-bracket expansion: {total_after}")

    if not changes:
        print("Nothing to change.")
        return

    api_call("PUT", "/api/state", token=token, body={"value": state})
    print("\nDone. Changes applied:")
    for c in changes:
        print(" -", c)


if __name__ == "__main__":
    main()
