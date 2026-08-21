# Hermes Lead-Gen Platform

Local outreach automation + dashboard for clinic lead generation.

## Pipeline

```
Apify (lead discovery)
  → Snov (email find + verify)
  → Linkup (personalization)
  → Draft + Google Drive media link
  → Gmail send (max 100/day)
  → Airtable CRM
  → Reply check every 4 hours
```

## Requirements

You need active accounts and API credentials for:

| Service | Purpose |
|---------|---------|
| **Apify** | Lead discovery / Maps scraping |
| **Snov.io** | Email finding and verification |
| **Linkup** | Personalization research |
| **Gmail** | Outbound email sending (App Password) |
| **Google Drive** | Hosted media link used in emails |
| **Airtable** | CRM storage |

Create an Apify account, fund it as needed for actor runs, and add your `APIFY_TOKEN` in `.env` or the dashboard **APIs** tab. The same applies to Snov, Linkup, Gmail, Drive, and Airtable — configure each service before running the pipeline.

## Quick start

```bash
cd leadgen
cp .env.example .env
# add your API keys to .env, or start the app and use the APIs tab

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
| **Usage** | Balances, send caps, connection status |

## Important files

| File | Role |
|------|------|
| `server.py` | FastAPI app + dashboard UI |
| `engine.py` | Pipeline logic |
| `airtable_crm.py` | Airtable helper |
| `start.sh` | Start server |
| `.env` | Secrets (not committed) |
| `.env.example` | Key template |

## Safety

- Never commit `.env`, `airtable.env`, or Google client secrets
- Dry-run is on by default (drafts only, no send)
- Daily send cap: 100

## Notes

- Configure Apify, Snov, Linkup, Gmail, Drive, and Airtable before production runs
- Manual lead is available when you already have a clinic name + website
- Reply watcher runs every 4 hours while the server is up
