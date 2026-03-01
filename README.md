# Driver Management Team Web Service

This app now supports two storage modes:
- JSON file mode (existing): `data/db.json`
- Postgres mode (recommended): via `DATABASE_URL`

When `DATABASE_URL` is set, the service uses Postgres as the source of truth.

## Run locally

1. Install deps:
   ```bash
   npm install
   ```
2. Start the server:
   ```bash
   npm start
   ```
3. Open:
   - `http://localhost:3000`

## Current API

- `GET /api/data`
- `PUT /api/data`
- `GET /api/presence`
- `POST /api/presence/heartbeat`
- `GET /api/data/backup-latest`

## Render setup (Postgres)

1. In Render, create a **PostgreSQL** instance.
2. In your web service, add env var:
   - `DATABASE_URL` = Internal connection string from that Postgres service.
3. Redeploy web service.

Health check now reports storage mode:
- `GET /api/health` -> `storage: "postgres"` or `"json-file"`

## One-time migration (JSON -> Postgres)

After `DATABASE_URL` is set:

```bash
npm run migrate:postgres
```

This copies current `data/db.json` (or `seed-db.json` fallback) into Postgres tables:
- `app_state` (current snapshot)
- `app_snapshots` (history snapshot)

## Reliability recommendations

1. Keep using one shared backend URL for all users.
2. In Render Postgres, enable backups/retention.
3. Keep web service and DB in same region.
4. Do not edit `seed-db.json` for live data.

## Google Sheets backup export (secondary backup/reporting)

Use Google Sheet as a backup/reporting sink, not your primary DB.

Required env vars for export script:
- `GOOGLE_SHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON` (full JSON string of service account key)
- `DATABASE_URL` (optional if you want export from Postgres; otherwise uses local JSON file)

Run backup export manually:

```bash
npm run backup:sheets
```

What it writes:
- One sheet tab per data tab (`driversSep`, `terminatedRemovals`, etc.)
- A `summary` tab with counts.

## Scheduling backups on Render

Create a **Cron Job** on Render (same repo):
- Command: `npm run backup:sheets`
- Schedule example: every hour or every day
- Set the same env vars (`DATABASE_URL`, `GOOGLE_SHEET_ID`, `GOOGLE_SERVICE_ACCOUNT_JSON`).

## Notes

- If `DATABASE_URL` is missing, service falls back to JSON mode.
- JSON mode still uses local backup files in `data/backups/`.
- Postgres mode stores snapshots in `app_snapshots`.
