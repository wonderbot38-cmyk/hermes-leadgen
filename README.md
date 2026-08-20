# Hermes Lead-Gen Platform

Local outreach automation + dashboard.

## Pipeline

```
Apify (leads)
  → Snov (find + verify email)
  → Linkup (personalize)
  → Draft + Google Drive media link
  → Gmail send (max 100/day)
  → Airtable CRM
  → Reply check every 4 hours
```

## Quick start

```bash
cd leadgen
cp .env.example .env
# fill keys in .env OR start the app and use the APIs tab

./start.sh
# open http://localhost:8731
```

Requires Python 3.11+ with `fastapi` and `uvicorn`:

```bash
pip install fastapi uvicorn
```

## Dashboard tabs

| Tab | What it does |
|-----|----------------|
| **Pipeline** | Run scrape / manual lead / reply check |
| **Leads** | Table + send drafted emails |
| **APIs** | Edit API keys without code |
| **Usage** | Snov balance, send caps, connection status |

## Important files

| File | Role |
|------|------|
| `server.py` | FastAPI app + dashboard UI |
| `engine.py` | Pipeline logic |
| `airtable_crm.py` | Airtable helper |
| `start.sh` | Start server |
| `.env` | Secrets (not committed) |

## Safety

- Never commit `.env`, `airtable.env`, or Google client secrets
- Dry-run is on by default (drafts only, no send)
- Daily send cap: 100

## Notes

- Apify needs account credit for Maps scrapes
- If Apify is empty, use **Manual lead** on the dashboard
- Reply watcher runs every 4 hours while the server is up
