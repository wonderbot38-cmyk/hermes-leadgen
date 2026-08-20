"""Airtable CRM helper for Hermes lead-gen."""
import json, os, urllib.request, urllib.error
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

def _load_env():
    env = {}
    for name in (".env", "airtable.env"):
        p = BASE_DIR / name
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env

ENV = _load_env()
TOKEN = ENV.get("AIRTABLE_TOKEN", "")
BASE_ID = ENV.get("AIRTABLE_BASE_ID", "applIuDEJ0Nwdkpf4")
TABLE_ID = ENV.get("AIRTABLE_TABLE_ID", "tblk8mwQeDcP8QDeu")


def _req(method, path, body=None):
    url = f"https://api.airtable.com/v0/{path}"
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        raise RuntimeError(f"Airtable {method} {path}: {e.code} {err[:400]}")


def upsert_lead(lead: dict) -> dict:
    """Create a lead row in Airtable. lead keys match local engine fields."""
    fields = {
        "Name": lead.get("name") or "",
        "Email": lead.get("email") or None,
        "Website": lead.get("website") or None,
        "Phone": lead.get("phone") or None,
        "Address": lead.get("address") or None,
        "Stage": lead.get("stage") or "found",
        "Email Valid": lead.get("email_valid") or None,
        "Subject": lead.get("subject") or None,
        "Body": lead.get("body") or None,
        "Drive Link": lead.get("drive_link") or None,
        "Reply Status": lead.get("reply_status") or "none",
        "Source": lead.get("source") or "hermes",
        "Notes": lead.get("notes") or None,
    }
    # drop empty / None
    fields = {k: v for k, v in fields.items() if v not in (None, "")}
    # Airtable rejects invalid select values
    res = _req("POST", f"{BASE_ID}/{TABLE_ID}", {"records": [{"fields": fields}], "typecast": True})
    return res["records"][0]


def list_leads(max_records=100) -> list:
    res = _req("GET", f"{BASE_ID}/{TABLE_ID}?maxRecords={max_records}")
    return res.get("records", [])


def update_lead(record_id: str, fields: dict) -> dict:
    fields = {k: v for k, v in fields.items() if v is not None}
    res = _req("PATCH", f"{BASE_ID}/{TABLE_ID}", {
        "records": [{"id": record_id, "fields": fields}],
        "typecast": True,
    })
    return res["records"][0]


if __name__ == "__main__":
    rows = list_leads(5)
    print(f"Airtable OK — {len(rows)} recent leads")
    for r in rows:
        f = r.get("fields", {})
        print(" -", f.get("Name"), "|", f.get("Stage"), "|", f.get("Email"))
