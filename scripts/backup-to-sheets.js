const fs = require('fs/promises');
const path = require('path');
const { Pool } = require('pg');
const { google } = require('googleapis');

const ROOT = path.join(__dirname, '..');
const DATA_FILE = path.join(ROOT, 'data', 'db.json');
const STATE_KEY = 'main';

function env(name, required = true) {
  const v = process.env[name];
  if (required && !v) throw new Error(`${name} is required`);
  return v;
}

async function loadState() {
  if (process.env.DATABASE_URL) {
    const ssl = String(process.env.PGSSL_DISABLE || '').toLowerCase() === 'true'
      ? false
      : { rejectUnauthorized: false };
    const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl });
    try {
      const rs = await pool.query('SELECT payload FROM app_state WHERE id = $1', [STATE_KEY]);
      if (rs.rowCount) return rs.rows[0].payload || {};
    } finally {
      await pool.end();
    }
  }

  const raw = await fs.readFile(DATA_FILE, 'utf8');
  return JSON.parse(raw || '{}');
}

function asRows(records) {
  if (!Array.isArray(records) || records.length === 0) {
    return { headers: ['id'], rows: [] };
  }
  const keys = [...new Set(records.flatMap((r) => Object.keys(r || {})))].sort();
  const headers = keys.length ? keys : ['id'];
  const rows = records.map((r) => headers.map((h) => {
    const val = r && r[h] !== undefined && r[h] !== null ? String(r[h]) : '';
    return val;
  }));
  return { headers, rows };
}

async function ensureSheetTab(sheets, spreadsheetId, title) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const found = (meta.data.sheets || []).find((s) => s.properties && s.properties.title === title);
  if (found) return;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [{ addSheet: { properties: { title } } }],
    },
  });
}

async function writeTab(sheets, spreadsheetId, title, headers, rows) {
  await sheets.spreadsheets.values.clear({ spreadsheetId, range: `${title}!A:ZZ` });
  const values = [headers, ...rows];
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${title}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values },
  });
}

async function main() {
  const spreadsheetId = env('GOOGLE_SHEET_ID');
  const serviceAccountJson = env('GOOGLE_SERVICE_ACCOUNT_JSON');

  const creds = JSON.parse(serviceAccountJson);
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const sheets = google.sheets({ version: 'v4', auth });

  const state = await loadState();
  const tabs = Object.keys(state || {}).filter((k) => Array.isArray(state[k]));

  for (const tabName of tabs) {
    const title = tabName.slice(0, 99);
    const { headers, rows } = asRows(state[tabName]);
    await ensureSheetTab(sheets, spreadsheetId, title);
    await writeTab(sheets, spreadsheetId, title, headers, rows);
  }

  const summaryRows = tabs.map((t) => [t, String(Array.isArray(state[t]) ? state[t].length : 0)]);
  await ensureSheetTab(sheets, spreadsheetId, 'summary');
  await writeTab(sheets, spreadsheetId, 'summary', ['tab', 'count'], summaryRows);

  console.log(`Backed up ${tabs.length} tabs to Google Sheet ${spreadsheetId}.`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
