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

function parseTabs() {
  const arg = process.argv[2] || process.env.RESTORE_TAB || 'roadTestScheduling';
  return String(arg)
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
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
      return {};
    } finally {
      await pool.end();
    }
  }

  try {
    const raw = await fs.readFile(DATA_FILE, 'utf8');
    return JSON.parse(raw || '{}');
  } catch {
    return {};
  }
}

async function saveState(state) {
  if (process.env.DATABASE_URL) {
    const ssl = String(process.env.PGSSL_DISABLE || '').toLowerCase() === 'true'
      ? false
      : { rejectUnauthorized: false };
    const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl });
    try {
      await pool.query(
        `INSERT INTO app_state (id, payload)
         VALUES ($1, $2::jsonb)
         ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload`,
        [STATE_KEY, JSON.stringify(state)]
      );
      return;
    } finally {
      await pool.end();
    }
  }

  await fs.mkdir(path.dirname(DATA_FILE), { recursive: true });
  await fs.writeFile(DATA_FILE, JSON.stringify(state, null, 2));
}

function rowHasData(row) {
  return row.some((v) => String(v || '').trim().length > 0);
}

function parseSheetRecords(values, tabName) {
  const rows = Array.isArray(values) ? values : [];
  if (rows.length === 0) return [];
  const headers = (rows[0] || []).map((h) => String(h || '').trim()).filter(Boolean);
  if (!headers.length) return [];

  const records = [];
  for (let i = 1; i < rows.length; i += 1) {
    const row = rows[i] || [];
    if (!rowHasData(row)) continue;
    const rec = {};
    headers.forEach((h, idx) => {
      rec[h] = row[idx] !== undefined ? String(row[idx]) : '';
    });
    if (!rec.id || !String(rec.id).trim()) {
      rec.id = `${tabName}_${Date.now()}_${i}`;
    }
    records.push(rec);
  }
  return records;
}

async function fetchTabRecords(sheets, spreadsheetId, tabName) {
  const title = tabName.slice(0, 99);
  const rs = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${title}!A:ZZ`,
  });
  return parseSheetRecords(rs.data.values || [], tabName);
}

async function main() {
  const spreadsheetId = env('GOOGLE_SHEET_ID');
  const serviceAccountJson = env('GOOGLE_SERVICE_ACCOUNT_JSON');
  const tabs = parseTabs();

  const creds = JSON.parse(serviceAccountJson);
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });
  const sheets = google.sheets({ version: 'v4', auth });

  const state = await loadState();
  for (const tabName of tabs) {
    const records = await fetchTabRecords(sheets, spreadsheetId, tabName);
    state[tabName] = records;
    console.log(`Restored ${records.length} records into ${tabName}.`);
  }
  await saveState(state);
  console.log('Restore complete.');
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
