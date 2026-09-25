#!/usr/bin/env python3
"""
One-off (but safe-to-re-run) bulk import: fills in manual_dwt for any
vessel that already has a row in the dashboard's `vessels` table (i.e. the
AIS worker has seen it at least once), using the broker's own CRM vessel
export (crm_vessel_dwt_reference.json, built from
srm_vessels_0_-_11000_dwt.txt — 8,865 vessels, IMO -> {name, dwt}).

Deliberately does NOT create new vessel rows for IMOs the AIS feed hasn't
seen yet — that would fill the table with thousands of position-less
"ghost" entries that will never show up in a "vessels near this port"
search anyway. Instead, re-run this script periodically (e.g. weekly) as
the AIS worker spots more candidates over time, and it'll backfill DWT for
whichever of them happen to be in the CRM reference.

Only fills DWT that isn't already set (never overwrites a value someone
already entered by hand, e.g. via the dashboard or MCP set_vessel_dwt).

Usage:
    python3 import_crm_dwt.py
It will prompt for your dashboard username and password (input hidden).
"""
import getpass
import json
import os
import urllib.error
import urllib.request

API_BASE = "https://94-136-184-214.sslip.io"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REFERENCE_PATH = os.path.join(BASE_DIR, "crm_vessel_dwt_reference.json")


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
    print(f"Loaded {len(reference)} vessels from the CRM reference.")

    username = input("Dashboard username: ")
    password = getpass.getpass("Dashboard password: ")

    login = api_call("POST", "/api/login", body={"username": username, "password": password})
    token = login["token"]
    print("Logged in as", login["username"])

    current = api_call("GET", "/api/vessels?all_types=true", token=token)["vessels"]
    print(f"{len(current)} vessels currently in the dashboard (seen by AIS at least once).")

    updated, already_set, not_in_reference = 0, 0, 0
    for v in current:
        imo = v["imo"]
        if v.get("dwt"):
            already_set += 1
            continue
        ref = reference.get(imo)
        if not ref:
            not_in_reference += 1
            continue
        api_call("PUT", f"/api/vessels/{imo}/type", token=token, body={"manual_dwt": ref["dwt"]})
        print(f"  {v.get('name') or ref['name']:25} IMO={imo}  DWT={ref['dwt']}")
        updated += 1

    print(f"\nDone. Updated {updated}, already had a DWT {already_set}, not in CRM reference {not_in_reference}.")
    print("Safe to re-run later as the AIS worker spots more vessels.")


if __name__ == "__main__":
    main()
