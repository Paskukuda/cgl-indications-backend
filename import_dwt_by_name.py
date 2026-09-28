#!/usr/bin/env python3
"""
Fills in DWT for vessels the AIS feed has seen, using the broker's
Handy/Supramax position list (broker_position_list_dwt.json, built from the
"Input" sheet of Position_list_Handy-Supramaxes.xlsx: ~7,500 names).

That list has NO IMO numbers, so this is matching by NAME — exactly the kind
of match that goes wrong when several ships share a name. It is therefore
deliberately conservative. A vessel gets a DWT only if ALL of these hold:

  1. it has no DWT yet (nothing already entered / imported is ever overwritten)
  2. the name matches exactly one vessel in the list (a list with the same
     hull under spelling variants and the same DWT counts as one)
  3. only ONE vessel on the dashboard carries that name
  4. AIS gives the ship's length, and the DWT is physically plausible for
     that length (a 190 m hull can't be a 4,000 t coaster)

Everything else is reported for a human look, not applied. Applied values are
saved with source "name-match" so the dashboard shows them as approximate (≈)
until someone confirms the IMO — typing a DWT in by hand clears that flag.

Safe to re-run as the AIS worker spots more vessels.

Usage:
    python3 import_dwt_by_name.py
It will prompt for your dashboard username and password (input hidden).
"""
import getpass
import json
import os
import re
import statistics
import urllib.error
import urllib.request
from collections import defaultdict

API_BASE = "https://94-136-184-214.sslip.io"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REFERENCE_PATH = os.path.join(BASE_DIR, "broker_position_list_dwt.json")

# DWT ≈ k · LOA³ for real cargo ships sits around 0.0045-0.0085 (coasters ~0.005,
# supramaxes ~0.008). Accept a wide band; it only has to catch gross mismatches.
K_MIN, K_MAX = 0.003, 0.014
SAME_VESSEL_TOLERANCE = 1.03  # list duplicates within 3% = same hull, spelled twice


def norm(name):
    n = str(name or "").upper().strip()
    n = re.sub(r"\(\s*\d+\s*K\s*\)", "", n)
    n = re.sub(r"^(M/V|MV|M\.V\.|MT|M/T)\s+", "", n)
    return re.sub(r"[^A-Z0-9]+", "", n)


def plausible(dwt, loa):
    k = dwt / (loa ** 3)
    return K_MIN <= k <= K_MAX


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
    with open(REFERENCE_PATH, encoding="utf-8") as f:
        reference = json.load(f)
    print(f"Loaded {len(reference)} vessel names from the broker position list.")

    username = input("Dashboard username: ")
    password = getpass.getpass("Dashboard password: ")
    login = api_call("POST", "/api/login", body={"username": username, "password": password})
    token = login["token"]
    print("Logged in as", login["username"])

    vessels = api_call("GET", "/api/vessels?all_types=true", token=token)["vessels"]
    print(f"{len(vessels)} vessels on the dashboard.\n")

    by_name = defaultdict(list)
    for v in vessels:
        by_name[norm(v.get("name"))].append(v)

    applied, amb_list, amb_dash, implausible, no_loa, not_found, had_dwt = [], [], [], [], [], 0, 0
    for v in vessels:
        if v.get("dwt"):
            had_dwt += 1
            continue
        key = norm(v.get("name"))
        entries = reference.get(key)
        if not key or not entries:
            not_found += 1
            continue
        dwts = [e[0] for e in entries]
        if max(dwts) / min(dwts) > SAME_VESSEL_TOLERANCE:
            amb_list.append((v, sorted(set(dwts))))
            continue
        if len(by_name[key]) > 1:
            amb_dash.append((v, len(by_name[key])))
            continue
        dwt = int(statistics.median(dwts))
        loa = v.get("loa")
        if not loa or loa < 20:
            no_loa.append((v, dwt))
            continue
        if not plausible(dwt, loa):
            implausible.append((v, dwt))
            continue
        api_call("PUT", f"/api/vessels/{v['imo']}/type", token=token,
                 body={"manual_dwt": dwt, "dwt_source": "name-match"})
        applied.append((v, dwt))

    print(f"APPLIED (unique name + plausible for the hull length): {len(applied)}")
    for v, dwt in applied[:40]:
        print(f"  {v.get('name'):24} IMO={v['imo']}  LOA={round(v['loa'])}m  ->  DWT {dwt:,}  (≈ by name)")
    if len(applied) > 40:
        print(f"  ... and {len(applied) - 40} more")

    if implausible:
        print(f"\nSKIPPED - DWT doesn't fit the hull length (likely a DIFFERENT ship with the same name): {len(implausible)}")
        for v, dwt in implausible[:15]:
            print(f"  {v.get('name'):24} IMO={v['imo']}  LOA={round(v['loa'])}m  list says {dwt:,}")
    if amb_list:
        print(f"\nSKIPPED - several different ships with this name in the list: {len(amb_list)}")
        for v, ds in amb_list[:10]:
            print(f"  {v.get('name'):24} IMO={v['imo']}  LOA={round(v.get('loa') or 0)}m  list has {ds}")
    if amb_dash:
        print(f"\nSKIPPED - more than one ship on the dashboard with this name: {len(amb_dash)}")
    if no_loa:
        print(f"\nSKIPPED - no hull length from AIS to cross-check against: {len(no_loa)}")

    print(f"\nAlready had a DWT: {had_dwt}. Not in the broker list: {not_found}.")
    print("Applied values show as ≈ on the AIS tab until confirmed; safe to re-run later.")


if __name__ == "__main__":
    main()
