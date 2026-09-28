#!/usr/bin/env python3
"""
One-time cleanup of what the indications chat added on 25.09.26
(peas / barley / meal-pellets EC Italy + SF56 routes), checked against
the broker's original message:

Peas (SF46, 20-25k, 5/5k) — CALCULATED by Claude from the ARISTA barley idea:
  * load port is CVB (Constanta/Varna/Burgas), not "Black Sea (POC)"
  * add the missing West Coast India route ($37-41; point indication $40)
  * East Coast India stays $40-46 (point indication $45)
  * file both under the 20,000-25,000t tonnage tab
Barley (SF52, 25-30k, 5/5k) — the REAL charterers' idea from ARISTA, $38-39:
  * CVB load port, 25,000-30,000t tab, note carries the actual message
Meal pellets:
  * Giurgiulesti -> Famagusta SF56: the broker's own idea is $47-51 (the chat
    had overwritten it with its own calc of $50-54) — restored
  * SF56 routes (3500t +/-10%) and EC Italy rows had no tonnage tag, so they
    showed under every tonnage tab: tagged 3000 / 5000-6000
  * EC Italy only had 2 of the 4 tonnage brackets: adds calculated
    6,000-8,000t and 8,000-10,000t rows (same 6%/bracket step as everywhere)

Safe to re-run: nothing is duplicated and nothing already correct is touched.

Usage:
    python3 fix_handysize_and_meal.py
It will prompt for your dashboard username and password (input hidden).
"""
import getpass
import json
import secrets
import urllib.error
import urllib.request
from datetime import datetime

API_BASE = "https://94-136-184-214.sslip.io"
TODAY = datetime.now().strftime("%d.%m.%y")
CVB = "CVB (Constanta/Varna/Burgas)"


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


def route_by_id(cargo, rid):
    return next((r for r in cargo["routes"] if r["id"] == rid), None)


def add_idea(route, kind, text, source):
    lst = route.setdefault(kind, [])
    if any(i.get("text") == text for i in lst):
        return False
    lst.insert(0, {"text": text, "source": source, "date": TODAY})
    return True


def set_lot(route, lot, log, name):
    if route.get("lot") != lot:
        route["lot"] = lot
        log.append(f"{name}: tonnage tab -> {lot}")


def new_route(cargo, label, direction, low, high, lot, note, source):
    r = {
        "id": "r" + secrets.token_hex(6), "label": label, "direction": direction,
        "lot": lot, "low": low, "high": high, "unit": "$/mt",
        "ownersIdeas": [{"text": note, "source": source, "date": TODAY}],
        "charterersIdeas": [], "updatedAt": TODAY,
    }
    cargo["routes"].append(r)
    return r


def main():
    username = input("Dashboard username: ")
    password = getpass.getpass("Dashboard password: ")
    login = api_call("POST", "/api/login", body={"username": username, "password": password})
    token = login["token"]
    print("Logged in as", login["username"])

    state = api_call("GET", "/api/state", token=token)["value"]
    cd = state["board"]["cargoData"]
    log = []

    # ── Meal pellets ─────────────────────────────────────────────
    meal = cd.get("meal_pellets")
    if meal:
        fam = route_by_id(meal, "r5a8c745350a8")  # Giurgiulesti -> Famagusta (SF56)
        if fam:
            if (fam["low"], fam["high"]) != (47, 51):
                log.append(f"Giurgiulesti->Famagusta SF56: {fam['low']}-{fam['high']} -> 47-51 (broker's own idea)")
                fam["low"], fam["high"] = 47, 51
                fam["updatedAt"] = TODAY
            add_idea(fam, "ownersIdeas",
                     "3500t +/-10%, SF56. Frt idea abt USD 47-51 pmt (индикация брокера; ранее было 50-54 по автоматическому расчёту)",
                     "Рабочий чат (индикация брокера)")
            set_lot(fam, "3000", log, "Giurgiulesti->Famagusta SF56")
        crete = route_by_id(meal, "r276e6893c33e")  # Reni -> Crete (SF56)
        if crete:
            set_lot(crete, "3000", log, "Reni->Crete SF56")

        it3 = route_by_id(meal, "r30cd34e4321a")    # EC Italy 3000t
        it5 = route_by_id(meal, "r269353494171")    # EC Italy 5000-6000t
        if it3:
            set_lot(it3, "3000", log, "Meal EC Italy 131-140")
        if it5:
            set_lot(it5, "5000-6000", log, "Meal EC Italy 125-132")
        label = "Izmail/Reni → EC Italy"
        note = "Calculated: stepped 6%/bracket from the 5000-6000t anchor rate (estimate)"
        for lot, low, high in (("6000-8000", 118, 124), ("8000-10000", 110, 116)):
            if not any(r["label"] == label and r.get("lot") == lot for r in meal["routes"]):
                new_route(meal, label, "outbound", low, high, lot, note, "Lot-bracket calculation")
                log.append(f"Meal EC Italy: added calculated {lot}t row {low}-{high}")
    else:
        print("meal_pellets not found — skipped.")

    # ── Peas (calculated from the ARISTA barley idea) ────────────
    peas = cd.get("peas")
    if peas:
        basis = "Расчёт Claude из CRM на основании ставки ячменя ARISTA ($38-39, 25-30к, sf52, 5/5к)"
        eci = route_by_id(peas, "rab90ec68f1cd") or next((r for r in peas["routes"] if "East Coast" in r["label"]), None)
        if eci:
            if "Black Sea" in eci["label"]:
                eci["label"] = f"{CVB} → East Coast India (Chennai/Krishnapatnam/Vizag)"
                log.append("Peas ECI: load port -> CVB")
            add_idea(eci, "ownersIdeas",
                     f"Итоговая индикация для SINTEZ (23-25к гороха sf46, 5/5к): ECI $45 pmt (диапазон 40-46). {basis} (estimate)",
                     "Расчёт от ячменя ARISTA")
            set_lot(eci, "20000-25000", log, "Peas ECI")
        if not any("West Coast" in r["label"] for r in peas["routes"]):
            wci = new_route(
                peas, f"{CVB} → West Coast India (Mundra/Kandla/Mumbai)", "outbound", 37, 41, "20000-25000",
                f"Итоговая индикация для SINTEZ (23-25к гороха sf46, 5/5к): WCI $40 pmt (диапазон 37-41). {basis} (estimate)",
                "Расчёт от ячменя ARISTA")
            log.append("Peas WCI: added 37-41")
    else:
        print("peas not found — skipped.")

    # ── Barley (the REAL ARISTA charterers' idea) ────────────────
    barley = cd.get("barley")
    if barley:
        b = route_by_id(barley, "r735314812850") or (barley["routes"][0] if barley["routes"] else None)
        if b:
            if "Black Sea" in b["label"]:
                b["label"] = f"{CVB} → India (WCI/ECI в запросе не указано)"
                log.append("Barley: load port -> CVB")
            real = ("Ячмень sf52, 25-30к, 5/5к, даты с 12.10. Нет адресной. Фрахт 38-39. Account ARISTA. "
                    "Порт назначения (WCI/ECI) в запросе не указан.")
            if not any(i.get("text") == real for i in b.get("charterersIdeas", [])):
                b["charterersIdeas"] = [{"text": real, "source": "WhatsApp (ARISTA)", "date": "25.09.26"}]
                log.append("Barley: note replaced with the actual ARISTA message")
            set_lot(b, "25000-30000", log, "Barley")
    else:
        print("barley not found — skipped.")

    if not log:
        print("Nothing to change — everything already in place.")
        return
    api_call("PUT", "/api/state", token=token, body={"value": state, "force": True})
    print("\nDone. Changes:")
    for line in log:
        print(" -", line)


if __name__ == "__main__":
    main()
