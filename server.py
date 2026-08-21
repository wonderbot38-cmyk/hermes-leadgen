"""
Hermes Lead-Gen Platform — FastAPI dashboard + API
Real-data-only dashboard with leads, usage, and API key management.
"""
from __future__ import annotations

import asyncio
import json
import re
import threading
import traceback
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import engine
from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

PORT = 8731
BASE = Path(__file__).resolve().parent
ENV_PATH = BASE / ".env"
AIRTABLE_ENV_PATH = BASE / "airtable.env"

_jobs: dict[str, Any] = {
    "pipeline_running": False,
    "pipeline_last": None,
    "replies_running": False,
    "replies_last": None,
    "last_error": None,
    "last_log": [],
}
_lock = threading.Lock()

# keys managed in UI
MANAGED_KEYS = [
    ("APIFY_TOKEN", "Apify", "Lead scraping"),
    ("SNOV_CLIENT_ID", "Snov Client ID", "Email find + verify"),
    ("SNOV_CLIENT_SECRET", "Snov Client Secret", "Email find + verify"),
    ("LINKUP_API_KEY", "Linkup", "Personalization scrape"),
    ("GMAIL_USER", "Gmail address", "Sending"),
    ("GMAIL_APP_PASSWORD", "Gmail App Password", "Sending"),
    ("AIRTABLE_TOKEN", "Airtable Token", "CRM"),
    ("AIRTABLE_BASE_ID", "Airtable Base ID", "CRM"),
    ("AIRTABLE_TABLE_ID", "Airtable Table ID", "CRM"),
    ("DRIVE_LINK", "Drive media link", "Video/image in emails"),
]


def _safe(fn, *a, **kw):
    try:
        return fn(*a, **kw)
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-400:]}


def _log(msg: str):
    with _lock:
        _jobs["last_log"] = (_jobs["last_log"] + [f"{datetime.now().strftime('%H:%M:%S')} {msg}"])[-80:]


def _read_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def _write_env_file(path: Path, updates: dict[str, str], keys_order: list[str] | None = None):
    existing = _read_env_file(path)
    existing.update({k: v for k, v in updates.items() if v is not None})
    # preserve comments header
    lines = [
        "# Hermes Lead-Gen secrets — editable from dashboard APIs tab",
        f"# updated {datetime.now().isoformat(timespec='seconds')}",
    ]
    order = keys_order or list(existing.keys())
    seen = set()
    for k in order:
        if k in existing:
            lines.append(f"{k}={existing[k]}")
            seen.add(k)
    for k, v in existing.items():
        if k not in seen:
            lines.append(f"{k}={v}")
    path.write_text("\n".join(lines) + "\n")


def _mask(val: str) -> str:
    if not val:
        return ""
    if "@" in val and "." in val and " " not in val:
        return val
    if val.startswith("http"):
        return val
    if len(val) <= 8:
        return "••••••••"
    return val[:4] + "••••" + val[-4:]


def reload_engine_env():
    engine.ENV = engine.load_env()
    # clear snov token cache
    if hasattr(engine, "_snov_tok"):
        engine._snov_tok["token"] = None
        engine._snov_tok["exp"] = 0.0


class RunBody(BaseModel):
    query: str = "skin clinic"
    location: str = "Dublin, Ireland"
    limit: int = Field(default=3, ge=1, le=50)
    dry_run: bool = True


class ManualBody(BaseModel):
    name: str
    website: str
    email: str = ""
    address: str = ""
    dry_run: bool = True


class SendBody(BaseModel):
    lead_id: int


class ConfigBody(BaseModel):
    keys: dict[str, str]


def _run_pipeline_bg(body: dict):
    with _lock:
        if _jobs["pipeline_running"]:
            return
        _jobs["pipeline_running"] = True
        _jobs["last_error"] = None
    _log(f"pipeline start q={body.get('query')} loc={body.get('location')} dry={body.get('dry_run')}")
    try:
        res = engine.run_pipeline(
            query=body.get("query", "skin clinic"),
            location=body.get("location", "Dublin, Ireland"),
            limit=int(body.get("limit", 3)),
            dry_run=bool(body.get("dry_run", True)),
        )
        with _lock:
            _jobs["pipeline_last"] = {"at": datetime.now().isoformat(timespec="seconds"), "result": res}
            if not res.get("ok"):
                _jobs["last_error"] = res.get("error")
                _log(f"pipeline ERROR: {res.get('error')}")
            else:
                _log(f"pipeline done added={res.get('added')}")
    except Exception as e:
        with _lock:
            _jobs["last_error"] = str(e)
            _jobs["pipeline_last"] = {"at": datetime.now().isoformat(timespec="seconds"), "result": {"ok": False, "error": str(e)}}
        _log(f"pipeline exception: {e}")
    finally:
        with _lock:
            _jobs["pipeline_running"] = False


def _run_replies_bg():
    with _lock:
        if _jobs["replies_running"]:
            return
        _jobs["replies_running"] = True
    _log("reply check start")
    try:
        res = engine.check_replies()
        with _lock:
            _jobs["replies_last"] = {"at": datetime.now().isoformat(timespec="seconds"), "result": res}
        _log(f"reply check done: {res}")
    except Exception as e:
        with _lock:
            _jobs["replies_last"] = {"at": datetime.now().isoformat(timespec="seconds"), "result": {"ok": False, "error": str(e)}}
        _log(f"reply check error: {e}")
    finally:
        with _lock:
            _jobs["replies_running"] = False


async def _reply_loop():
    await asyncio.sleep(45)
    while True:
        try:
            await asyncio.to_thread(_run_replies_bg)
        except Exception:
            pass
        await asyncio.sleep(4 * 60 * 60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        engine.db().close()
    except Exception:
        pass
    task = asyncio.create_task(_reply_loop())
    _log("server started")
    yield
    task.cancel()


app = FastAPI(title="Hermes Lead-Gen", lifespan=lifespan)


@app.exception_handler(Exception)
async def all_errors(request: Request, exc: Exception):
    return JSONResponse(status_code=200, content={"ok": False, "error": str(exc)})


@app.get("/api/health")
def api_health():
    h = _safe(engine.health)
    if isinstance(h, dict):
        h["jobs"] = {
            "pipeline_running": _jobs["pipeline_running"],
            "replies_running": _jobs["replies_running"],
            "pipeline_last": _jobs["pipeline_last"],
            "replies_last": _jobs["replies_last"],
            "last_error": _jobs["last_error"],
            "log": list(_jobs["last_log"][-30:]),
        }
    return h


@app.get("/api/stats")
def api_stats():
    return _safe(engine.stats)


@app.get("/api/leads")
def api_leads():
    rows = _safe(engine.list_leads, 300)
    if isinstance(rows, dict) and rows.get("ok") is False:
        return rows
    return {"ok": True, "leads": rows}


@app.get("/api/runs")
def api_runs():
    rows = _safe(engine.recent_runs, 40)
    if isinstance(rows, dict) and rows.get("ok") is False:
        return rows
    return {"ok": True, "runs": rows}


@app.get("/api/config")
def api_config_get():
    """Return managed keys (masked) + status."""
    reload_engine_env()
    env = engine.ENV
    items = []
    for key, label, purpose in MANAGED_KEYS:
        val = env.get(key, "")
        items.append({
            "key": key,
            "label": label,
            "purpose": purpose,
            "set": bool(val),
            "masked": _mask(val),
            "preview": val[:6] + "…" if val and len(val) > 10 and key not in ("GMAIL_USER", "DRIVE_LINK", "AIRTABLE_BASE_ID", "AIRTABLE_TABLE_ID") else (val if key in ("GMAIL_USER", "DRIVE_LINK", "AIRTABLE_BASE_ID", "AIRTABLE_TABLE_ID") else _mask(val)),
        })
    return {"ok": True, "items": items}


@app.post("/api/config")
def api_config_set(body: ConfigBody):
    """Update only provided non-empty keys. Empty string = skip (keep old)."""
    try:
        allowed = {k for k, _, _ in MANAGED_KEYS}
        updates = {}
        for k, v in (body.keys or {}).items():
            if k not in allowed:
                continue
            if v is None:
                continue
            v = str(v).strip()
            if v == "" or v.startswith("••••"):
                continue  # don't overwrite with mask
            updates[k] = v
        if not updates:
            return {"ok": False, "error": "No valid keys to update (empty or masked values ignored)"}

        # split airtable vs main
        air_keys = {"AIRTABLE_TOKEN", "AIRTABLE_BASE_ID", "AIRTABLE_TABLE_ID", "AIRTABLE_TABLE_NAME"}
        main_u = {k: v for k, v in updates.items() if k not in air_keys}
        air_u = {k: v for k, v in updates.items() if k in air_keys}

        if main_u:
            _write_env_file(ENV_PATH, main_u, [k for k, _, _ in MANAGED_KEYS if k not in air_keys])
        if air_u:
            # also mirror into main .env
            _write_env_file(ENV_PATH, air_u)
            _write_env_file(AIRTABLE_ENV_PATH, air_u, list(air_keys))

        reload_engine_env()
        _log(f"config updated: {', '.join(updates.keys())}")
        return {"ok": True, "updated": list(updates.keys())}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/config/test")
def api_config_test():
    """Live-test each integration and return usage where possible."""
    reload_engine_env()
    out: dict[str, Any] = {"ok": True, "tests": {}}

    # Gmail creds present
    out["tests"]["gmail"] = {
        "ok": bool(engine.cfg("GMAIL_USER") and engine.cfg("GMAIL_APP_PASSWORD")),
        "detail": engine.cfg("GMAIL_USER") or "missing",
    }

    # Snov
    bal = engine.snov_balance()
    out["tests"]["snov"] = bal if isinstance(bal, dict) else {"ok": False, "error": str(bal)}

    # Apify — user endpoint
    tok = engine.cfg("APIFY_TOKEN")
    if tok:
        ok, res = engine.http_json(
            "GET",
            "https://api.apify.com/v2/users/me",
            headers={"Authorization": f"Bearer {tok}"},
        )
        if ok and isinstance(res, dict):
            data = res.get("data") or res
            out["tests"]["apify"] = {
                "ok": True,
                "username": data.get("username"),
                "email": data.get("email"),
                "detail": "token valid",
            }
        else:
            out["tests"]["apify"] = {"ok": False, "error": res}
    else:
        out["tests"]["apify"] = {"ok": False, "error": "token missing"}

    # Linkup — tiny search
    key = engine.cfg("LINKUP_API_KEY")
    if key:
        ok, res = engine.http_json(
            "POST",
            "https://api.linkup.so/v1/search",
            headers={"Authorization": f"Bearer {key}"},
            body={"q": "ping test one word", "depth": "standard", "outputType": "searchResults"},
            timeout=45,
        )
        out["tests"]["linkup"] = {"ok": ok, "detail": "search ok" if ok else res}
    else:
        out["tests"]["linkup"] = {"ok": False, "error": "key missing"}

    # Airtable
    at = engine.cfg("AIRTABLE_TOKEN")
    base = engine.cfg("AIRTABLE_BASE_ID")
    if at and base:
        ok, res = engine.http_json(
            "GET",
            f"https://api.airtable.com/v0/meta/bases/{base}/tables",
            headers={"Authorization": f"Bearer {at}"},
        )
        if ok and isinstance(res, dict):
            tables = [t.get("name") for t in res.get("tables") or []]
            out["tests"]["airtable"] = {"ok": True, "tables": tables}
        else:
            out["tests"]["airtable"] = {"ok": False, "error": res}
    else:
        out["tests"]["airtable"] = {"ok": False, "error": "missing token/base"}

    out["tests"]["drive_link"] = {
        "ok": bool(engine.cfg("DRIVE_LINK", engine.DRIVE_DEFAULT)),
        "detail": engine.cfg("DRIVE_LINK", engine.DRIVE_DEFAULT),
    }
    return out


@app.get("/api/usage")
def api_usage():
    """Usage + balances for dashboard."""
    reload_engine_env()
    st = engine.stats()
    snov = engine.snov_balance()
    usage = {
        "ok": True,
        "sends": {
            "today": st.get("sent_today", 0) if isinstance(st, dict) else 0,
            "cap": st.get("daily_cap", 100) if isinstance(st, dict) else 100,
            "remaining": st.get("remaining_today", 0) if isinstance(st, dict) else 0,
        },
        "leads": st if isinstance(st, dict) else {},
        "snov": snov,
        "apify": {},
        "linkup": {"note": "Linkup bills per search; balance shown in Linkup dashboard"},
        "airtable": {},
    }
    # apify user
    tok = engine.cfg("APIFY_TOKEN")
    if tok:
        ok, res = engine.http_json(
            "GET",
            "https://api.apify.com/v2/users/me",
            headers={"Authorization": f"Bearer {tok}"},
        )
        if ok and isinstance(res, dict):
            d = res.get("data") or {}
            usage["apify"] = {
                "ok": True,
                "username": d.get("username"),
                "email": d.get("email"),
            }
        else:
            usage["apify"] = {"ok": False, "error": res}
    # airtable count
    try:
        rows = engine.list_leads(500)
        usage["airtable"] = {
            "ok": True,
            "local_leads": len(rows),
            "base_id": engine.cfg("AIRTABLE_BASE_ID"),
            "table_id": engine.cfg("AIRTABLE_TABLE_ID"),
        }
    except Exception as e:
        usage["airtable"] = {"ok": False, "error": str(e)}
    return usage


@app.post("/api/run")
def api_run(body: RunBody, bg: BackgroundTasks):
    if _jobs["pipeline_running"]:
        return {"ok": True, "started": False, "note": "pipeline already running"}
    bg.add_task(_run_pipeline_bg, body.model_dump())
    return {"ok": True, "started": True, "dry_run": body.dry_run, "note": "started"}


@app.post("/api/manual")
def api_manual(body: ManualBody):
    _log(f"manual lead {body.name}")
    res = _safe(
        engine.run_manual_lead,
        body.name,
        body.website,
        body.email,
        body.address,
        body.dry_run,
    )
    _log(f"manual result {res}")
    return res


@app.post("/api/check-replies")
def api_check_replies(bg: BackgroundTasks):
    if _jobs["replies_running"]:
        return {"ok": True, "started": False, "note": "already checking"}
    bg.add_task(_run_replies_bg)
    return {"ok": True, "started": True}


@app.post("/api/send")
def api_send(body: SendBody):
    res = _safe(engine.send_drafted, body.lead_id)
    _log(f"send {body.lead_id} -> {res}")
    return res


# ───────────────────────── DASHBOARD HTML ─────────────────────────
DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Hermes Lead-Gen</title>
<style>
:root{
  --bg:#f6f5f2; --ink:#121212; --muted:#6a6a64; --line:#e2dfd8;
  --card:#ffffff; --ok:#0f7a3a; --warn:#9a5b00; --bad:#c62828; --blue:#1a56db;
  --soft:#f0eeea; --accent:#121212;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);
  font:14px/1.45 Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
a{color:var(--blue);text-decoration:none}
.wrap{max-width:1120px;margin:0 auto;padding:20px 18px 48px}
.top{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:16px;flex-wrap:wrap}
.top h1{margin:0;font-size:22px;font-weight:700;letter-spacing:-.02em}
.top p{margin:4px 0 0;color:var(--muted);font-size:13px}
.status{display:flex;align-items:center;gap:8px;background:var(--card);border:1px solid var(--line);
  border-radius:999px;padding:8px 12px;font-size:12.5px}
.dot{width:8px;height:8px;border-radius:50%;background:#bbb}
.dot.on{background:var(--ok);box-shadow:0 0 0 3px rgba(15,122,58,.15)}
.dot.off{background:var(--bad)}
.tabs{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px;background:var(--soft);
  padding:6px;border-radius:12px;border:1px solid var(--line)}
.tab{border:0;background:transparent;padding:9px 14px;border-radius:9px;cursor:pointer;
  font-weight:600;font-size:13px;color:var(--muted)}
.tab.active{background:var(--card);color:var(--ink);box-shadow:0 1px 2px rgba(0,0,0,.06)}
.grid{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:14px}
@media(max-width:900px){.grid{grid-template-columns:repeat(3,1fr)}}
@media(max-width:560px){.grid{grid-template-columns:repeat(2,1fr)}}
.stat{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px}
.stat .n{font-size:24px;font-weight:750;letter-spacing:-.03em}
.stat .l{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em;margin-top:2px}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px;margin-bottom:12px}
.card h2{margin:0 0 12px;font-size:14px;font-weight:700}
.row{display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end}
label{display:flex;flex-direction:column;gap:5px;font-size:11.5px;color:var(--muted);font-weight:600}
input[type=text],input[type=number],input[type=password],textarea{
  padding:10px 12px;border:1px solid var(--line);border-radius:10px;font-size:13.5px;
  background:#fafaf8;color:var(--ink);min-width:150px;outline:none}
input:focus,textarea:focus{border-color:#bbb;background:#fff}
textarea{min-height:72px;width:100%;font-family:ui-monospace,Menlo,monospace;font-size:12px}
.chk{flex-direction:row;align-items:center;gap:8px;padding-bottom:10px;font-weight:500}
button{border:1px solid var(--accent);background:var(--accent);color:#fff;border-radius:10px;
  padding:10px 14px;font-size:13px;font-weight:650;cursor:pointer}
button.secondary{background:#fff;color:var(--ink)}
button.danger{background:#fff;color:var(--bad);border-color:#e7b8b8}
button:disabled{opacity:.45;cursor:not-allowed}
button.small{padding:7px 10px;font-size:12px}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:11px 10px;border-bottom:1px solid var(--line);vertical-align:top;font-size:13px}
th{font-size:11px;text-transform:uppercase;letter-spacing:.04em;color:var(--muted);background:#f7f6f3}
.badge{display:inline-block;padding:3px 8px;border-radius:999px;font-size:11px;font-weight:700;background:var(--soft)}
.b-sent{background:#e7f6ec;color:var(--ok)}
.b-drafted{background:#eee8ff;color:#5b35b0}
.b-verified{background:#fff3dd;color:var(--warn)}
.b-email_found{background:#e8efff;color:var(--blue)}
.b-found{background:#f0f0ec;color:#555}
.b-replied,.b-interested{background:#e3f2fd;color:#1565c0}
.b-not_interested{background:#fdecea;color:var(--bad)}
.mono{font-family:ui-monospace,Menlo,monospace;font-size:12px}
.log{background:#141414;color:#d7d7d1;border-radius:12px;padding:12px;font:12px/1.5 ui-monospace,Menlo,monospace;
  max-height:200px;overflow:auto;white-space:pre-wrap}
.err{display:none;background:#fdecea;color:var(--bad);border:1px solid #f0c2be;border-radius:12px;padding:10px 12px;margin-bottom:12px}
.okmsg{display:none;background:#e7f6ec;color:var(--ok);border:1px solid #bfe5cb;border-radius:12px;padding:10px 12px;margin-bottom:12px}
.api-row{display:grid;grid-template-columns:180px 1fr 120px;gap:10px;align-items:center;padding:12px 0;border-bottom:1px solid var(--line)}
@media(max-width:720px){.api-row{grid-template-columns:1fr}}
.api-row:last-child{border-bottom:0}
.api-meta .name{font-weight:700}
.api-meta .purpose{font-size:12px;color:var(--muted)}
.tag{font-size:11px;font-weight:700;padding:3px 8px;border-radius:999px}
.tag.yes{background:#e7f6ec;color:var(--ok)}
.tag.no{background:#fdecea;color:var(--bad)}
.usage-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}
.usage-card{background:var(--soft);border-radius:12px;padding:14px}
.usage-card h3{margin:0 0 8px;font-size:13px}
.usage-card .big{font-size:28px;font-weight:750}
.muted{color:var(--muted);font-size:12px}
.hidden{display:none !important}
.empty{text-align:center;color:var(--muted);padding:28px}
.spinner{display:inline-block;width:12px;height:12px;border:2px solid #ddd;border-top-color:var(--ink);border-radius:50%;animation:sp .7s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes sp{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div>
      <h1>Lead-Gen Control</h1>
      <p>Apify → Snov → Linkup → Drive → Gmail → Airtable · 100/day · replies every 4h</p>
    </div>
    <div class="status"><span class="dot" id="dot"></span><span id="live">connecting…</span></div>
  </div>

  <div class="err" id="err"></div>
  <div class="okmsg" id="okmsg"></div>

  <div class="grid" id="stats"></div>

  <div class="tabs">
    <button class="tab active" data-tab="pipeline">Outreach</button>
    <button class="tab" data-tab="leads">Leads</button>
    <button class="tab" data-tab="apis">APIs</button>
    <button class="tab" data-tab="usage">Usage</button>
  </div>

  <!-- PIPELINE -->
  <section id="tab-pipeline">
    <div class="card">
      <h2>Run pipeline</h2>
      <div class="row">
        <label>Lead type<input id="q" type="text" value="skin clinic"></label>
        <label>Location<input id="loc" type="text" value="Dublin, Ireland"></label>
        <label>Limit<input id="lim" type="number" min="1" max="50" value="3" style="min-width:80px"></label>
        <label class="chk"><input id="dry" type="checkbox" checked> Dry run (no send)</label>
        <button id="btnRun">Run pipeline</button>
        <button class="secondary" id="btnRefresh">Refresh</button>
        <button class="secondary" id="btnReplies">Check replies</button>
      </div>
      <p class="muted" style="margin:10px 0 0">Configure Apify, Snov, Linkup, Gmail, Drive, and Airtable in the APIs tab before running. Dry run drafts only — uncheck to send (max 100/day).</p>
    </div>

    <div class="card">
      <h2>Manual lead</h2>
      <div class="row">
        <label>Name<input id="mName" type="text" placeholder="Clinic name"></label>
        <label>Website<input id="mWeb" type="text" placeholder="https://clinic.ie"></label>
        <label>Email optional<input id="mEmail" type="text" placeholder="info@clinic.ie"></label>
        <label>Address<input id="mAddr" type="text" placeholder="Dublin"></label>
        <label class="chk"><input id="mDry" type="checkbox" checked> Dry run</label>
        <button id="btnManual">Process lead</button>
      </div>
    </div>

    <div class="card">
      <h2>Live outreach activity</h2>
      <div class="log" id="log">ready.</div>
    </div>
  </section>

  <!-- LEADS -->
  <section id="tab-leads" class="hidden">
    <div class="card" style="padding:0;overflow:auto">
      <div style="padding:16px 16px 0;display:flex;justify-content:space-between;align-items:center">
        <h2 style="margin:0">Outreach queue</h2>
        <button class="secondary small" id="btnRefresh2">Refresh</button>
      </div>
      <table>
        <thead><tr>
          <th>#</th><th>Clinic</th><th>Email</th><th>Email status</th><th>Stage</th><th></th>
        </tr></thead>
        <tbody id="rows"><tr><td colspan="6" class="empty">Loading…</td></tr></tbody>
      </table>
    </div>
  </section>

  <!-- APIS -->
  <section id="tab-apis" class="hidden">
    <div class="card">
      <h2>API keys</h2>
      <p class="muted" style="margin-top:-6px">Paste a new value only where you want to change. Leave blank to keep current. Then Save + Test.</p>
      <div id="apiList"></div>
      <div class="row" style="margin-top:14px">
        <button id="btnSaveKeys">Save keys</button>
        <button class="secondary" id="btnTestKeys">Test all APIs</button>
      </div>
      <div id="testOut" class="muted" style="margin-top:12px"></div>
    </div>
  </section>

  <!-- USAGE -->
  <section id="tab-usage" class="hidden">
    <div class="card">
      <h2>Usage & balances</h2>
      <div class="usage-grid" id="usageGrid"><div class="muted">Loading…</div></div>
      <div class="row" style="margin-top:12px">
        <button class="secondary" id="btnUsage">Refresh usage</button>
      </div>
    </div>
  </section>
</div>

<script>
const $ = id => document.getElementById(id);
let apiItems = [];
let busy = false;

function showErr(m){ const e=$('err'); if(!m){e.style.display='none';e.textContent='';return;} e.style.display='block'; e.textContent=m; }
function showOk(m){ const e=$('okmsg'); if(!m){e.style.display='none';e.textContent='';return;} e.style.display='block'; e.textContent=m; setTimeout(()=>showOk(''), 4000); }
function log(m){ const el=$('log'); el.textContent += '\\n'+m; el.scrollTop = el.scrollHeight; }
function esc(s){ return String(s??'').replace(/[&<>"']/g,c=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c])); }

async function api(path, opts){
  try{
    const r = await fetch('/api/'+path, Object.assign({headers:{'Content-Type':'application/json'}}, opts||{}));
    const text = await r.text();
    try { return JSON.parse(text); } catch { return {ok:false, error:'Bad JSON: '+text.slice(0,200)}; }
  }catch(e){ return {ok:false, error:String(e)}; }
}

// tabs
document.querySelectorAll('.tab').forEach(btn=>{
  btn.onclick = () => {
    document.querySelectorAll('.tab').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    ['pipeline','leads','apis','usage'].forEach(t=>{
      const el = $('tab-'+t);
      if(t===btn.dataset.tab) el.classList.remove('hidden'); else el.classList.add('hidden');
    });
    if(btn.dataset.tab==='apis') loadConfig();
    if(btn.dataset.tab==='usage') loadUsage();
    if(btn.dataset.tab==='leads') refresh();
  };
});

function badge(s){ return `<span class="badge b-${esc(s||'found')}">${esc(s||'-')}</span>`; }

function renderStats(s){
  if(!s || s.error){ return; }
  const st = s.stages||{};
  const replies = s.replies || {};
  const replied = replies.replied || 0;
  const items=[
    ['Total', s.total||0],
    ['Sent today', `${s.sent_today||0}/${s.daily_cap||100}`],
    ['Left today', s.remaining_today??0],
    ['Drafted', st.drafted||0],
    ['Sent', st.sent||0],
    ['Replied', replied],
  ];
  $('stats').innerHTML = items.map(([l,n])=>`<div class="stat"><div class="n">${n}</div><div class="l">${l}</div></div>`).join('');
}

function renderLeads(payload){
  const leads = (payload&&payload.leads)||[];
  if(!leads.length){
    $('rows').innerHTML = `<tr><td colspan="6" class="empty">No leads yet</td></tr>`;
    return;
  }
  $('rows').innerHTML = leads.map(r=>`<tr>
    <td>${r.id}</td>
    <td><strong>${esc(r.name||'')}</strong><div class="muted">${esc(r.address||'')}</div>
      ${r.website?`<div class="muted"><a href="${esc(r.website)}" target="_blank">${esc((r.website||'').replace(/^https?:\\/\\//,''))}</a></div>`:''}
    </td>
    <td class="mono">${esc(r.email||'')}</td>
    <td>${esc(r.email_valid||'-')}</td>
    <td>${badge(r.stage)}</td>
    <td>${r.stage==='drafted'&&r.email?`<button class="secondary small" onclick="sendOne(${r.id})">Send</button>`:''}</td>
  </tr>`).join('');
}

function renderHealth(h){
  if(!h || h.ok===false){
    $('dot').className='dot off';
    $('live').textContent = 'offline' + (h&&h.error?': '+h.error:'');
    return;
  }
  $('dot').className='dot on';
  $('live').textContent = 'online · '+(h.time||'');
  const j=h.jobs||{};
  if(j.last_error) showErr(j.last_error);
  if(j.pipeline_running) log('… pipeline still running');
}

async function refresh(){
  const [h,s,l] = await Promise.all([api('health'), api('stats'), api('leads')]);
  renderHealth(h);
  renderStats(s);
  renderLeads(l);
  if(h && h.jobs && h.jobs.log){
    const serverLog = h.jobs.log.slice(-15).join('\\n');
    if(serverLog) {
      // don't wipe user logs entirely — append marker
    }
  }
}

async function loadConfig(){
  const d = await api('config');
  if(!d.ok){ showErr(d.error||'config failed'); return; }
  apiItems = d.items||[];
  $('apiList').innerHTML = apiItems.map((it,i)=>`
    <div class="api-row">
      <div class="api-meta">
        <div class="name">${esc(it.label)}</div>
        <div class="purpose">${esc(it.purpose)} · <span class="mono">${esc(it.key)}</span></div>
      </div>
      <div>
        <div class="muted" style="margin-bottom:4px">Current: <span class="mono">${esc(it.preview||'(empty)')}</span>
          <span class="tag ${it.set?'yes':'no'}">${it.set?'set':'missing'}</span>
        </div>
        <input type="password" id="k_${i}" data-key="${esc(it.key)}" placeholder="Paste new value to change" autocomplete="off" style="width:100%"/>
      </div>
      <div></div>
    </div>
  `).join('');
}

async function saveKeys(){
  const keys = {};
  apiItems.forEach((it,i)=>{
    const el = $('k_'+i);
    if(el && el.value.trim()) keys[it.key] = el.value.trim();
  });
  if(!Object.keys(keys).length){ showErr('Paste at least one new key value'); return; }
  $('btnSaveKeys').disabled=true;
  const r = await api('config', {method:'POST', body: JSON.stringify({keys})});
  $('btnSaveKeys').disabled=false;
  if(!r.ok){ showErr(r.error||'save failed'); return; }
  showOk('Saved: '+(r.updated||[]).join(', '));
  showErr('');
  log('keys updated: '+(r.updated||[]).join(', '));
  // clear inputs
  apiItems.forEach((it,i)=>{ const el=$('k_'+i); if(el) el.value=''; });
  loadConfig();
  refresh();
}

async function testKeys(){
  $('btnTestKeys').disabled=true;
  $('testOut').innerHTML = '<span class="spinner"></span>Testing APIs…';
  const r = await api('config/test', {method:'POST', body:'{}'});
  $('btnTestKeys').disabled=false;
  if(!r.ok && !r.tests){ $('testOut').textContent = r.error||'test failed'; return; }
  const tests = r.tests||{};
  $('testOut').innerHTML = Object.entries(tests).map(([k,v])=>{
    const ok = v && v.ok;
    const detail = ok ? (v.detail || v.username || JSON.stringify(v.data||v).slice(0,120)) : (v.error ? JSON.stringify(v.error).slice(0,160) : JSON.stringify(v).slice(0,160));
    return `<div style="margin:6px 0"><span class="tag ${ok?'yes':'no'}">${ok?'OK':'FAIL'}</span> <strong>${esc(k)}</strong> — <span class="mono">${esc(detail)}</span></div>`;
  }).join('');
  log('API test done');
}

async function loadUsage(){
  $('usageGrid').innerHTML = '<div class="muted"><span class="spinner"></span>Loading usage…</div>';
  const u = await api('usage');
  if(!u.ok && u.error){ $('usageGrid').innerHTML = esc(u.error); return; }
  const snov = u.snov||{};
  const snovData = snov.data || snov;
  const snovBal = (snovData && snovData.balance) != null ? snovData.balance : (snov.ok? '—' : 'error');
  const apify = u.apify||{};
  const sends = u.sends||{};
  const leads = u.leads||{};
  const air = u.airtable||{};
  $('usageGrid').innerHTML = `
    <div class="usage-card"><h3>Email sends today</h3><div class="big">${sends.today||0} / ${sends.cap||100}</div><div class="muted">${sends.remaining||0} remaining</div></div>
    <div class="usage-card"><h3>Snov credits</h3><div class="big">${esc(snovBal)}</div><div class="muted">${snov.ok===false?'check key':('resets/expires in '+(snovData.expires_in||snovData.limit_resets_in||'—')+' days')}</div></div>
    <div class="usage-card"><h3>Apify</h3><div class="big">${apify.ok?'Connected':'—'}</div><div class="muted">${esc(apify.username||apify.error||'configure APIFY_TOKEN in APIs tab')}</div></div>
    <div class="usage-card"><h3>Leads in DB</h3><div class="big">${leads.total||0}</div><div class="muted">drafted ${(leads.stages||{}).drafted||0} · sent ${(leads.stages||{}).sent||0}</div></div>
    <div class="usage-card"><h3>Airtable</h3><div class="big">${air.ok?'OK':'—'}</div><div class="muted">local leads ${air.local_leads||0}<br><span class="mono">${esc(air.base_id||'')}</span></div></div>
    <div class="usage-card"><h3>Linkup</h3><div class="big">Active</div><div class="muted">${esc((u.linkup||{}).note||'per-search billing')}</div></div>
  `;
}

$('btnRefresh').onclick = ()=>{ log('refresh'); refresh(); };
$('btnRefresh2').onclick = ()=>refresh();
$('btnUsage').onclick = ()=>loadUsage();
$('btnSaveKeys').onclick = saveKeys;
$('btnTestKeys').onclick = testKeys;

$('btnRun').onclick = async ()=>{
  if(busy) return;
  busy=true; $('btnRun').disabled=true;
  const body = {
    query: $('q').value.trim()||'skin clinic',
    location: $('loc').value.trim()||'Dublin, Ireland',
    limit: Math.max(1, Math.min(50, +$('lim').value||3)),
    dry_run: $('dry').checked
  };
  showErr(''); log('>> run '+JSON.stringify(body));
  const r = await api('run', {method:'POST', body: JSON.stringify(body)});
  log('<< '+JSON.stringify(r));
  if(r.error) showErr(r.error); else showOk(r.note||'Pipeline started');
  $('btnRun').disabled=false; busy=false;
  let n=0;
  const t=setInterval(async()=>{
    n++; await refresh();
    const h=await api('health');
    if(!h.jobs||!h.jobs.pipeline_running||n>90){
      clearInterval(t);
      if(h.jobs&&h.jobs.pipeline_last) log('pipeline finished: '+JSON.stringify(h.jobs.pipeline_last.result||{}).slice(0,300));
    }
  }, 3000);
};

$('btnReplies').onclick = async ()=>{
  log('>> check replies');
  const r = await api('check-replies', {method:'POST', body:'{}'});
  log('<< '+JSON.stringify(r));
  showOk('Reply check started');
  setTimeout(refresh, 2500);
};

$('btnManual').onclick = async ()=>{
  const body = {
    name: $('mName').value.trim(),
    website: $('mWeb').value.trim(),
    email: $('mEmail').value.trim(),
    address: $('mAddr').value.trim(),
    dry_run: $('mDry').checked
  };
  if(!body.name||!body.website){ showErr('Name and website required'); return; }
  showErr(''); $('btnManual').disabled=true;
  log('>> manual '+JSON.stringify(body));
  const r = await api('manual', {method:'POST', body: JSON.stringify(body)});
  log('<< '+JSON.stringify(r));
  $('btnManual').disabled=false;
  if(r.error) showErr(r.error);
  else if(r.ok) showOk('Lead processed: '+(r.result&&r.result.email?r.result.email:r.result&&r.result.stage));
  else showErr(JSON.stringify(r));
  refresh();
};

async function sendOne(id){
  if(!confirm('Send email for lead #'+id+'?')) return;
  log('>> send '+id);
  const r = await api('send', {method:'POST', body: JSON.stringify({lead_id:id})});
  log('<< '+JSON.stringify(r));
  if(r.error||r.ok===false) showErr(r.error||'send failed'); else showOk('Sent #'+id);
  refresh();
}

refresh();
setInterval(refresh, 12000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home():
    return HTMLResponse(DASHBOARD_HTML, headers={"Cache-Control": "no-store"})


if __name__ == "__main__":
    import uvicorn

    print(f"Hermes Lead-Gen Platform -> http://localhost:{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
