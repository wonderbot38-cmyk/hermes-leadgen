"""
Hermes Lead-Gen Engine
Pipeline: Apify → Snov → Linkup personalize → draft (+Drive) → Gmail → Airtable
Plus: daily send cap 100, reply check every 4h
Never raises uncaught errors to the UI — all failures return {ok:False,error:...}
"""
from __future__ import annotations

import json
import os
import re
import smtplib
import sqlite3
import ssl
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Optional

BASE = Path(__file__).resolve().parent
DB_PATH = BASE / "leads.db"
DAILY_SEND_CAP = 100
DRIVE_DEFAULT = (
    "https://drive.google.com/file/d/1AOMdL8vaTHF-bL7wmCEGJvw-PrEa9sRZ/view?usp=sharing"
)


# ─── config ───────────────────────────────────────────────────────────────
def load_env() -> dict:
    env: dict[str, str] = {}
    for name in (".env", "airtable.env"):
        p = BASE / name
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


ENV = load_env()


def cfg(key: str, default: str = "") -> str:
    return ENV.get(key, default) or default


# ─── sqlite ───────────────────────────────────────────────────────────────
def db() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            address TEXT,
            website TEXT,
            phone TEXT,
            source TEXT DEFAULT 'apify',
            stage TEXT DEFAULT 'found',
            email TEXT,
            email_valid TEXT,
            subject TEXT,
            body TEXT,
            personalization TEXT,
            drive_link TEXT,
            airtable_id TEXT,
            gmail_message_id TEXT,
            reply_status TEXT DEFAULT 'none',
            sent_at TEXT,
            last_checked_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(email) ON CONFLICT IGNORE
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT,
            status TEXT,
            detail TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    # migrate older DBs quietly
    cols = {r[1] for r in c.execute("PRAGMA table_info(leads)").fetchall()}
    for col, typ in [
        ("personalization", "TEXT"),
        ("drive_link", "TEXT"),
        ("airtable_id", "TEXT"),
        ("gmail_message_id", "TEXT"),
        ("reply_status", "TEXT DEFAULT 'none'"),
        ("last_checked_at", "TEXT"),
    ]:
        if col not in cols:
            try:
                c.execute(f"ALTER TABLE leads ADD COLUMN {col} {typ}")
            except Exception:
                pass
    c.commit()
    return c


def log_run(kind: str, status: str, detail: Any = None) -> None:
    try:
        c = db()
        c.execute(
            "INSERT INTO runs (kind, status, detail) VALUES (?,?,?)",
            (kind, status, json.dumps(detail) if not isinstance(detail, str) else detail),
        )
        c.commit()
        c.close()
    except Exception:
        pass


def row_to_dict(r: sqlite3.Row) -> dict:
    return {k: r[k] for k in r.keys()}


# ─── http helper ──────────────────────────────────────────────────────────
def http_json(
    method: str,
    url: str,
    headers: Optional[dict] = None,
    body: Any = None,
    form: Optional[dict] = None,
    timeout: int = 60,
) -> tuple[bool, Any]:
    try:
        data = None
        hdrs = dict(headers or {})
        if form is not None:
            data = urllib.parse.urlencode(form, doseq=True).encode()
            hdrs.setdefault("Content-Type", "application/x-www-form-urlencoded")
        elif body is not None:
            data = json.dumps(body).encode()
            hdrs.setdefault("Content-Type", "application/json")
        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            if not raw:
                return True, {}
            try:
                return True, json.loads(raw)
            except Exception:
                return True, raw
    except urllib.error.HTTPError as e:
        err = e.read().decode(errors="replace")
        try:
            err_j = json.loads(err)
        except Exception:
            err_j = err[:500]
        return False, {"http_status": e.code, "error": err_j}
    except Exception as e:
        return False, {"error": str(e)}


# ─── Apify ────────────────────────────────────────────────────────────────
def apify_pull(query: str, location: str, limit: int = 5) -> tuple[bool, Any]:
    tok = cfg("APIFY_TOKEN")
    if not tok:
        return False, "APIFY_TOKEN missing"
    actor = "compass~crawler-google-places"
    ok, run = http_json(
        "POST",
        f"https://api.apify.com/v2/acts/{actor}/runs?token={tok}",
        body={
            "searchStringsArray": [query],
            "locationQuery": location,
            "maxCrawledPlaces": int(limit),
            "language": "en",
        },
        timeout=90,
    )
    if not ok:
        msg = run
        if isinstance(run, dict):
            msg = (
                run.get("error", {}).get("message")
                if isinstance(run.get("error"), dict)
                else run.get("error") or run
            )
        return False, f"Apify start failed: {msg}"
    data = run.get("data") or run
    run_id = data.get("id")
    ds_id = data.get("defaultDatasetId")
    if not run_id:
        return False, f"Apify bad response: {run}"

    status = "READY"
    for _ in range(40):
        ok, st = http_json(
            "GET",
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            headers={"Authorization": f"Bearer {tok}"},
        )
        if ok:
            status = (st.get("data") or st).get("status", status)
            if status in ("SUCCEEDED", "FAILED", "TIMED-OUT", "ABORTED"):
                break
        time.sleep(6)
    if status != "SUCCEEDED":
        return False, f"Apify run ended: {status}"

    ok, rows = http_json(
        "GET",
        f"https://api.apify.com/v2/datasets/{ds_id}/items?clean=true&limit=100",
        headers={"Authorization": f"Bearer {tok}"},
    )
    if not ok:
        return False, f"Apify dataset failed: {rows}"
    if not isinstance(rows, list):
        return False, f"Apify unexpected dataset: {rows}"
    return True, rows


# ─── Snov ─────────────────────────────────────────────────────────────────
_snov_tok: dict[str, Any] = {"token": None, "exp": 0.0}


def snov_token() -> tuple[bool, str]:
    now = time.time()
    if _snov_tok["token"] and now < _snov_tok["exp"] - 60:
        return True, _snov_tok["token"]
    cid, csec = cfg("SNOV_CLIENT_ID"), cfg("SNOV_CLIENT_SECRET")
    if not cid or not csec:
        return False, "Snov credentials missing"
    ok, res = http_json(
        "POST",
        "https://api.snov.io/v1/oauth/access_token",
        form={
            "grant_type": "client_credentials",
            "client_id": cid,
            "client_secret": csec,
        },
    )
    if not ok or not isinstance(res, dict) or not res.get("access_token"):
        return False, f"Snov auth failed: {res}"
    _snov_tok["token"] = res["access_token"]
    _snov_tok["exp"] = now + float(res.get("expires_in", 3600))
    return True, _snov_tok["token"]


def snov_find(domain: str) -> tuple[bool, Any]:
    ok, tok = snov_token()
    if not ok:
        return False, tok
    ok, start = http_json(
        "POST",
        "https://api.snov.io/v2/domain-search/domain-emails/start",
        headers={"Authorization": f"Bearer {tok}"},
        body={"domain": domain},
    )
    if not ok:
        return False, start
    # task_hash can be in data or meta
    th = None
    if isinstance(start, dict):
        th = (start.get("data") or {}).get("task_hash") if isinstance(start.get("data"), dict) else None
        if not th:
            th = (start.get("meta") or {}).get("task_hash")
        if not th:
            link = (start.get("links") or {}).get("result") or ""
            if "/result/" in link:
                th = link.rstrip("/").split("/")[-1]
    if not th:
        # sometimes data is list and meta has hash
        th = (start.get("meta") or {}).get("task_hash") if isinstance(start, dict) else None
    if not th:
        return False, f"Snov find: no task_hash in {start}"

    emails: list[str] = []
    for _ in range(12):
        time.sleep(4)
        ok, res = http_json(
            "GET",
            f"https://api.snov.io/v2/domain-search/domain-emails/result/{th}",
            headers={"Authorization": f"Bearer {tok}"},
        )
        if not ok:
            continue
        data = res.get("data") if isinstance(res, dict) else None
        if isinstance(data, list) and data:
            for e in data:
                if isinstance(e, str):
                    emails.append(e)
                elif isinstance(e, dict) and e.get("email"):
                    emails.append(e["email"])
            break
        # empty list can mean still running or no results
        if isinstance(data, list) and res.get("status") in ("completed", "done"):
            break
    return True, emails


def snov_verify(email: str) -> tuple[bool, str]:
    ok, tok = snov_token()
    if not ok:
        return False, tok
    ok, start = http_json(
        "POST",
        "https://api.snov.io/v2/email-verification/start",
        headers={"Authorization": f"Bearer {tok}"},
        form={"emails[]": email},
    )
    if not ok:
        return False, "unknown"
    th = (start.get("data") or {}).get("task_hash") if isinstance(start, dict) else None
    if not th:
        return False, "unknown"
    for _ in range(10):
        time.sleep(4)
        ok, res = http_json(
            "GET",
            f"https://api.snov.io/v2/email-verification/result?task_hash={th}",
            headers={"Authorization": f"Bearer {tok}"},
        )
        if not ok:
            continue
        if isinstance(res, dict) and res.get("status") == "completed":
            for e in res.get("data") or []:
                if e.get("email") == email:
                    return True, (e.get("result") or {}).get("smtp_status") or "unknown"
            return True, "unknown"
    return True, "unknown"


def snov_balance() -> dict:
    ok, tok = snov_token()
    if not ok:
        return {"ok": False, "error": tok}
    ok, res = http_json(
        "GET", f"https://api.snov.io/v1/get-balance?access_token={tok}"
    )
    if not ok:
        return {"ok": False, "error": res}
    return {"ok": True, "data": res.get("data") if isinstance(res, dict) else res}


# ─── Linkup personalization ───────────────────────────────────────────────
def linkup_personalize(name: str, website: str = "", address: str = "") -> tuple[bool, str]:
    key = cfg("LINKUP_API_KEY")
    if not key:
        return False, ""
    q = (
        f"Business: {name}. Website: {website}. Location: {address}. "
        f"Give 4-6 short factual bullets for a personalized B2B outreach email: "
        f"what they do, specialties, locations, notable details, founder if known. "
        f"Facts only, no fluff."
    )
    ok, res = http_json(
        "POST",
        "https://api.linkup.so/v1/search",
        headers={"Authorization": f"Bearer {key}"},
        body={"q": q, "depth": "standard", "outputType": "sourcedAnswer"},
        timeout=90,
    )
    if not ok:
        return False, ""
    if isinstance(res, dict) and res.get("answer"):
        return True, str(res["answer"]).strip()
    return False, ""


# ─── Gmail ────────────────────────────────────────────────────────────────
def gmail_send(to: str, subject: str, body: str) -> tuple[bool, str]:
    user, app = cfg("GMAIL_USER"), cfg("GMAIL_APP_PASSWORD")
    if not user or not app:
        return False, "Gmail credentials missing"
    try:
        msg = EmailMessage()
        msg["From"] = user
        msg["To"] = to
        msg["Subject"] = subject
        msg.set_content(body)
        ctx = ssl.create_default_context()
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as s:
            s.starttls(context=ctx)
            s.login(user, app)
            s.send_message(msg)
        return True, "sent"
    except Exception as e:
        return False, str(e)


def gmail_search_replies_via_oauth(since_days: int = 14) -> tuple[bool, Any]:
    """Use google_api.py if token exists; else return empty gracefully."""
    token_path = Path.home() / ".hermes" / "google_token.json"
    gapi = (
        Path.home()
        / ".hermes"
        / "skills"
        / "productivity"
        / "google-workspace"
        / "scripts"
        / "google_api.py"
    )
    if not token_path.exists() or not gapi.exists():
        return True, []  # not configured — not an error
    try:
        import subprocess

        q = f"in:inbox newer_than:{since_days}d"
        out = subprocess.check_output(
            ["python3", str(gapi), "gmail", "search", q, "--max", "50"],
            stderr=subprocess.STDOUT,
            timeout=60,
            text=True,
        )
        data = json.loads(out)
        if isinstance(data, list):
            return True, data
        return True, []
    except Exception as e:
        return False, str(e)


# ─── Airtable ─────────────────────────────────────────────────────────────
def airtable_upsert(lead: dict) -> tuple[bool, str]:
    token = cfg("AIRTABLE_TOKEN")
    base_id = cfg("AIRTABLE_BASE_ID")
    table_id = cfg("AIRTABLE_TABLE_ID")
    if not token or not base_id or not table_id:
        return False, "Airtable not configured"
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
        "Notes": lead.get("personalization") or lead.get("notes") or None,
    }
    fields = {k: v for k, v in fields.items() if v not in (None, "")}
    ok, res = http_json(
        "POST",
        f"https://api.airtable.com/v0/{base_id}/{table_id}",
        headers={"Authorization": f"Bearer {token}"},
        body={"records": [{"fields": fields}], "typecast": True},
    )
    if not ok:
        return False, str(res)[:300]
    try:
        rid = res["records"][0]["id"]
        return True, rid
    except Exception:
        return False, str(res)[:300]


def airtable_update(record_id: str, fields: dict) -> tuple[bool, str]:
    token = cfg("AIRTABLE_TOKEN")
    base_id = cfg("AIRTABLE_BASE_ID")
    table_id = cfg("AIRTABLE_TABLE_ID")
    if not token or not record_id:
        return False, "missing"
    ok, res = http_json(
        "PATCH",
        f"https://api.airtable.com/v0/{base_id}/{table_id}",
        headers={"Authorization": f"Bearer {token}"},
        body={"records": [{"id": record_id, "fields": fields}], "typecast": True},
    )
    return (True, "ok") if ok else (False, str(res)[:300])


# ─── draft ────────────────────────────────────────────────────────────────
def draft_email(name: str, domain: str, personalization: str = "", drive_link: str = "") -> tuple[str, str]:
    clinic = (name or domain or "your clinic").strip()
    # short personal line from linkup
    hook = ""
    if personalization:
        # first sentence-ish
        first = re.split(r"(?<=[.!?])\s+", personalization.strip())
        hook = (first[0] if first else personalization)[:220]
        if hook and not hook.endswith("."):
            hook += "."
    subj = f"Quick idea for {clinic}"
    parts = [f"Hi there,\n"]
    if hook:
        parts.append(f"I was looking at {clinic} — {hook}\n")
    else:
        parts.append(f"I came across {clinic} and liked what you're building.\n")
    parts.append(
        "I help skin and aesthetic clinics turn more website visitors into booked "
        "consultations with a simple chatbot + follow-up system (no extra ad spend).\n"
    )
    if drive_link:
        parts.append(f"Here's a short look at how it works:\n{drive_link}\n")
    parts.append(
        "Would a quick 10-minute chat this week be useful?\n\nThanks,\nJeet"
    )
    return subj, "\n".join(parts)


# ─── domain helper ────────────────────────────────────────────────────────
def domain_from_website(website: str) -> str:
    if not website:
        return ""
    w = website.strip()
    w = re.sub(r"^https?://", "", w)
    w = w.split("/")[0].split("?")[0]
    if w.startswith("www."):
        w = w[4:]
    return w.lower()


# ─── stats / list ─────────────────────────────────────────────────────────
def list_leads(limit: int = 200) -> list[dict]:
    c = db()
    rows = c.execute(
        "SELECT * FROM leads ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    c.close()
    return [row_to_dict(r) for r in rows]


def stats() -> dict:
    c = db()
    total = c.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    stages = {
        r["stage"]: r["n"]
        for r in c.execute(
            "SELECT stage, COUNT(*) AS n FROM leads GROUP BY stage"
        ).fetchall()
    }
    replies = {
        r["reply_status"]: r["n"]
        for r in c.execute(
            "SELECT COALESCE(reply_status,'none') AS reply_status, COUNT(*) AS n FROM leads GROUP BY reply_status"
        ).fetchall()
    }
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    sent_today = c.execute(
        "SELECT COUNT(*) FROM leads WHERE stage='sent' AND sent_at LIKE ?",
        (f"{today}%",),
    ).fetchone()[0]
    # also match local timestamps without Z
    if sent_today == 0:
        sent_today = c.execute(
            "SELECT COUNT(*) FROM leads WHERE stage='sent' AND date(sent_at)=date('now','localtime')"
        ).fetchone()[0]
    c.close()
    return {
        "total": total,
        "stages": stages,
        "replies": replies,
        "sent_today": sent_today,
        "daily_cap": DAILY_SEND_CAP,
        "remaining_today": max(0, DAILY_SEND_CAP - sent_today),
    }


def recent_runs(limit: int = 30) -> list[dict]:
    c = db()
    rows = c.execute(
        "SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    c.close()
    return [row_to_dict(r) for r in rows]


def health() -> dict:
    checks = {}
    checks["gmail"] = bool(cfg("GMAIL_USER") and cfg("GMAIL_APP_PASSWORD"))
    checks["apify"] = bool(cfg("APIFY_TOKEN"))
    checks["snov"] = bool(cfg("SNOV_CLIENT_ID") and cfg("SNOV_CLIENT_SECRET"))
    checks["linkup"] = bool(cfg("LINKUP_API_KEY"))
    checks["airtable"] = bool(
        cfg("AIRTABLE_TOKEN") and cfg("AIRTABLE_BASE_ID") and cfg("AIRTABLE_TABLE_ID")
    )
    checks["google_oauth"] = (Path.home() / ".hermes" / "google_token.json").exists()
    # live snov balance (non-fatal)
    bal = snov_balance()
    return {
        "ok": True,
        "checks": checks,
        "snov_balance": bal,
        "drive_link": cfg("DRIVE_LINK", DRIVE_DEFAULT),
        "daily_cap": DAILY_SEND_CAP,
        "time": datetime.now().isoformat(timespec="seconds"),
    }


# ─── pipeline ─────────────────────────────────────────────────────────────
def process_one_place(r: dict, dry_run: bool, drive_link: str) -> dict:
    """Process a single place dict from Apify or manual input."""
    name = r.get("title") or r.get("name") or "Unknown"
    website = r.get("website") or ""
    address = r.get("address") or r.get("formattedAddress") or ""
    phone = r.get("phone") or ""
    domain = domain_from_website(website)
    result = {
        "name": name,
        "website": website,
        "stage": "found",
        "email": None,
        "error": None,
    }

    c = db()
    cur = c.execute(
        """INSERT INTO leads (name, address, website, phone, source, stage, drive_link)
           VALUES (?,?,?,?,?,?,?)""",
        (name, address, website, phone, r.get("source") or "apify", "found", drive_link),
    )
    lid = cur.lastrowid
    c.commit()

    if not domain:
        result["error"] = "no website/domain"
        c.close()
        return result

    # find email
    ok, emails = snov_find(domain)
    if not ok:
        result["error"] = f"snov find: {emails}"
        c.execute("UPDATE leads SET stage='found' WHERE id=?", (lid,))
        c.commit()
        c.close()
        return result
    if not emails:
        result["error"] = "no emails found"
        c.commit()
        c.close()
        return result

    # prefer info@ / hello@ / contact@ then first
    prefer = ["info@", "hello@", "contact@", "dublin@", "admin@", "office@"]
    email = emails[0]
    for p in prefer:
        for e in emails:
            if e.lower().startswith(p):
                email = e
                break
    result["email"] = email
    c.execute(
        "UPDATE leads SET email=?, stage='email_found' WHERE id=?", (email, lid)
    )
    c.commit()

    # verify
    ok, valid = snov_verify(email)
    if not ok:
        valid = "unknown"
    c.execute(
        "UPDATE leads SET email_valid=?, stage='verified' WHERE id=?", (valid, lid)
    )
    c.commit()
    result["email_valid"] = valid

    # personalize
    pok, perso = linkup_personalize(name, website, address)
    if not pok:
        perso = ""
    c.execute("UPDATE leads SET personalization=? WHERE id=?", (perso, lid))
    c.commit()

    # draft
    subj, body = draft_email(name, domain, perso, drive_link)
    stage = "drafted"
    c.execute(
        "UPDATE leads SET subject=?, body=?, stage=?, drive_link=? WHERE id=?",
        (subj, body, stage, drive_link, lid),
    )
    c.commit()

    # send
    if (not dry_run) and valid == "valid":
        # daily cap
        st = stats()
        if st["remaining_today"] <= 0:
            result["error"] = "daily send cap reached"
        else:
            sok, smsg = gmail_send(email, subj, body)
            if sok:
                stage = "sent"
                now = datetime.now().isoformat(timespec="seconds")
                c.execute(
                    "UPDATE leads SET stage='sent', sent_at=?, reply_status='none' WHERE id=?",
                    (now, lid),
                )
                c.commit()
            else:
                result["error"] = f"send failed: {smsg}"

    # airtable
    lead_payload = {
        "name": name,
        "email": email,
        "website": website,
        "phone": phone,
        "address": address,
        "stage": stage,
        "email_valid": valid,
        "subject": subj,
        "body": body,
        "drive_link": drive_link,
        "reply_status": "none",
        "source": r.get("source") or "apify",
        "personalization": perso,
    }
    aok, aid = airtable_upsert(lead_payload)
    if aok:
        c.execute("UPDATE leads SET airtable_id=? WHERE id=?", (aid, lid))
        c.commit()

    result["stage"] = stage
    result["id"] = lid
    c.close()
    return result


def run_pipeline(
    query: str = "skin clinic",
    location: str = "Dublin, Ireland",
    limit: int = 3,
    dry_run: bool = True,
) -> dict:
    drive_link = cfg("DRIVE_LINK", DRIVE_DEFAULT)
    try:
        ok, rows = apify_pull(query, location, limit)
        if not ok:
            log_run("pipeline", "error", str(rows))
            return {"ok": False, "error": str(rows), "added": 0, "results": []}

        results = []
        for r in rows:
            try:
                results.append(process_one_place(r, dry_run=dry_run, drive_link=drive_link))
            except Exception as e:
                results.append(
                    {
                        "name": r.get("title") or r.get("name"),
                        "error": str(e),
                        "trace": traceback.format_exc()[-300:],
                    }
                )
        log_run(
            "pipeline",
            "ok",
            {"query": query, "location": location, "limit": limit, "dry_run": dry_run, "n": len(results)},
        )
        return {
            "ok": True,
            "added": len(results),
            "dry_run": dry_run,
            "results": results,
        }
    except Exception as e:
        log_run("pipeline", "error", str(e))
        return {"ok": False, "error": str(e), "added": 0, "results": []}


def run_manual_lead(
    name: str,
    website: str,
    email: str = "",
    address: str = "",
    dry_run: bool = True,
) -> dict:
    """Process one known lead by name + website (does not run Apify scrape)."""
    drive_link = cfg("DRIVE_LINK", DRIVE_DEFAULT)
    place = {
        "title": name,
        "website": website,
        "address": address,
        "source": "manual",
    }
    # if email provided, short-circuit snov find by injecting into process via website domain
    try:
        res = process_one_place(place, dry_run=dry_run, drive_link=drive_link)
        # if user gave email and we didn't find one, force it
        if email and not res.get("email"):
            c = db()
            lid = res.get("id")
            if lid:
                c.execute(
                    "UPDATE leads SET email=?, stage='email_found' WHERE id=?",
                    (email, lid),
                )
                c.commit()
            c.close()
        log_run("manual", "ok", res)
        return {"ok": True, "result": res}
    except Exception as e:
        log_run("manual", "error", str(e))
        return {"ok": False, "error": str(e)}


def check_replies() -> dict:
    """
    Every 4h job: look at Gmail inbox for replies matching sent leads.
    Updates local DB + Airtable.
    """
    try:
        c = db()
        sent = c.execute(
            "SELECT * FROM leads WHERE stage='sent' AND COALESCE(reply_status,'none')='none'"
        ).fetchall()
        sent = [row_to_dict(r) for r in sent]
        if not sent:
            c.close()
            log_run("replies", "ok", {"checked": 0, "matched": 0})
            return {"ok": True, "checked": 0, "matched": 0, "note": "no pending sent leads"}

        ok, messages = gmail_search_replies_via_oauth(21)
        if not ok:
            # still mark last_checked
            now = datetime.now().isoformat(timespec="seconds")
            c.execute(
                "UPDATE leads SET last_checked_at=? WHERE stage='sent'", (now,)
            )
            c.commit()
            c.close()
            log_run("replies", "warn", str(messages))
            return {
                "ok": True,
                "checked": len(sent),
                "matched": 0,
                "warning": f"gmail search issue: {messages}",
            }

        matched = 0
        now = datetime.now().isoformat(timespec="seconds")
        # build lookup by email domain / address
        for lead in sent:
            email = (lead.get("email") or "").lower()
            if not email:
                continue
            hit = None
            for m in messages or []:
                frm = (m.get("from") or "").lower()
                snip = (m.get("snippet") or "").lower()
                subj = (m.get("subject") or "").lower()
                if email in frm or email.split("@")[0] in frm:
                    hit = m
                    break
                # domain match in from
                domain = email.split("@")[-1]
                if domain and domain in frm:
                    hit = m
                    break
            c.execute(
                "UPDATE leads SET last_checked_at=? WHERE id=?",
                (now, lead["id"]),
            )
            if hit:
                # crude interest keywords
                text = f"{hit.get('subject','')} {hit.get('snippet','')}".lower()
                status = "replied"
                if any(
                    w in text
                    for w in (
                        "interested",
                        "let's talk",
                        "lets talk",
                        "book a call",
                        "sounds good",
                        "tell me more",
                    )
                ):
                    status = "interested"
                if any(
                    w in text
                    for w in ("not interested", "unsubscribe", "remove me", "no thanks")
                ):
                    status = "not_interested"
                stage = "replied" if status == "replied" else status
                c.execute(
                    "UPDATE leads SET reply_status=?, stage=? WHERE id=?",
                    (status, stage if stage in ("replied", "interested", "not_interested") else "replied", lead["id"]),
                )
                if lead.get("airtable_id"):
                    airtable_update(
                        lead["airtable_id"],
                        {"Reply Status": status, "Stage": "replied" if status == "replied" else status},
                    )
                matched += 1
        c.commit()
        c.close()
        log_run("replies", "ok", {"checked": len(sent), "matched": matched})
        return {"ok": True, "checked": len(sent), "matched": matched}
    except Exception as e:
        log_run("replies", "error", str(e))
        return {"ok": False, "error": str(e)}


def send_drafted(lead_id: int) -> dict:
    """Send one drafted lead if under cap."""
    try:
        st = stats()
        if st["remaining_today"] <= 0:
            return {"ok": False, "error": "daily cap reached (100)"}
        c = db()
        row = c.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
        if not row:
            c.close()
            return {"ok": False, "error": "lead not found"}
        lead = row_to_dict(row)
        if lead.get("stage") == "sent":
            c.close()
            return {"ok": False, "error": "already sent"}
        if not lead.get("email"):
            c.close()
            return {"ok": False, "error": "no email"}
        if lead.get("email_valid") and lead["email_valid"] != "valid":
            c.close()
            return {"ok": False, "error": f"email not valid ({lead.get('email_valid')})"}
        ok, msg = gmail_send(lead["email"], lead.get("subject") or "Hello", lead.get("body") or "")
        if not ok:
            c.close()
            return {"ok": False, "error": msg}
        now = datetime.now().isoformat(timespec="seconds")
        c.execute(
            "UPDATE leads SET stage='sent', sent_at=?, reply_status='none' WHERE id=?",
            (now, lead_id),
        )
        c.commit()
        if lead.get("airtable_id"):
            airtable_update(lead["airtable_id"], {"Stage": "sent", "Reply Status": "none"})
        c.close()
        return {"ok": True, "id": lead_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# init db on import
try:
    db().close()
except Exception:
    pass
