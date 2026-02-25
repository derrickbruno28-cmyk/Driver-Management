# Driver Management Team Web Service

This project is now a live-service-ready web app:
- Frontend: `index.html`
- Backend API: `server.js`
- Shared data store: `data/db.json`

## Run locally

1. Start the server:
   ```bash
   npm start
   ```
2. Open:
   - `http://localhost:3000`

## How team sharing works

- On load, the app fetches shared data from `GET /api/data`.
- Any add/edit/delete writes the full dataset to `PUT /api/data`.
- All teammates using the same deployed URL share the same backend data file.

## Deploy (Render example)

1. Push this folder to a Git repo.
2. Create a new **Web Service** in Render.
3. Configure:
   - Build command: (leave empty)
   - Start command: `npm start`
4. Add a persistent disk and mount it at:
   - `/opt/render/project/src/data`
5. Deploy.

Why the disk matters: without persistent storage, `data/db.json` resets on redeploy/restart.

## Optional hardening for production

- Add app authentication (Google SSO or password auth).
- Add role permissions (view-only vs editor).
- Add automated backups for `data/db.json`.
- Add audit logging endpoint/history for compliance.
