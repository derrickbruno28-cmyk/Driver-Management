const http = require('http');
const fs = require('fs/promises');
const path = require('path');

let Pool = null;
try {
  ({ Pool } = require('pg'));
} catch (_) {
  // pg is optional at runtime unless DATABASE_URL is configured.
}

const PORT = Number(process.env.PORT || 3000);
const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, 'data');
const DATA_FILE = path.join(DATA_DIR, 'db.json');
const BACKUP_DIR = path.join(DATA_DIR, 'backups');
const SEED_FILE = path.join(ROOT, 'seed-db.json');
const PRESENCE_TTL_MS = 45 * 1000;
const USE_POSTGRES = !!process.env.DATABASE_URL;
const PG_SSL_DISABLED = String(process.env.PGSSL_DISABLE || '').toLowerCase() === 'true';
const STATE_KEY = 'main';

const REQUIRED_TABS = ['driversSep', 'leads', 'otrHires', 'ag4Hires', 'ag4Sep', 'historical'];
const EXTRA_TABS = ['terminatedRemovals', 'notMovingForward', 'roadTestScheduling'];

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.ico': 'image/x-icon',
};

let writeQueue = Promise.resolve();
let pool = null;
const activeSessions = new Map();

function hasRequiredTabs(data) {
  return !!data && typeof data === 'object' && !Array.isArray(data) && REQUIRED_TABS.every((tab) => Array.isArray(data[tab]));
}

function normalizeDatabaseShape(data) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return {};
  [...REQUIRED_TABS, ...EXTRA_TABS].forEach((tab) => {
    if (!Array.isArray(data[tab])) data[tab] = [];
  });
  return data;
}

async function readJsonFileSafe(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    const parsed = JSON.parse(raw || '{}');
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
    return parsed;
  } catch {
    return null;
  }
}

async function loadInitialStateFromFiles() {
  const fromDb = await readJsonFileSafe(DATA_FILE);
  if (hasRequiredTabs(fromDb)) return normalizeDatabaseShape(fromDb);

  const fromSeed = await readJsonFileSafe(SEED_FILE);
  if (fromSeed) return normalizeDatabaseShape(fromSeed);

  return normalizeDatabaseShape({});
}

async function ensureDataFile() {
  await fs.mkdir(DATA_DIR, { recursive: true });
  await fs.mkdir(BACKUP_DIR, { recursive: true });
  const existing = await readJsonFileSafe(DATA_FILE);
  if (hasRequiredTabs(existing)) return;
  const initial = await loadInitialStateFromFiles();
  await fs.writeFile(DATA_FILE, JSON.stringify(initial, null, 2), 'utf8');
}

function getPool() {
  if (!pool) {
    if (!Pool) throw new Error('pg package is not installed');
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: PG_SSL_DISABLED ? false : { rejectUnauthorized: false },
    });
  }
  return pool;
}

async function ensurePostgres() {
  const pg = getPool();
  await pg.query(`
    CREATE TABLE IF NOT EXISTS app_state (
      id TEXT PRIMARY KEY,
      payload JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await pg.query(`
    CREATE TABLE IF NOT EXISTS app_snapshots (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      source TEXT NOT NULL DEFAULT 'put',
      payload JSONB NOT NULL
    );
  `);

  const existing = await pg.query('SELECT id FROM app_state WHERE id = $1', [STATE_KEY]);
  if (existing.rowCount > 0) return;

  const initial = await loadInitialStateFromFiles();
  await pg.query('INSERT INTO app_state (id, payload) VALUES ($1, $2::jsonb)', [STATE_KEY, JSON.stringify(initial)]);
  await pg.query('INSERT INTO app_snapshots (source, payload) VALUES ($1, $2::jsonb)', ['bootstrap', JSON.stringify(initial)]);
}

async function readData() {
  if (USE_POSTGRES) {
    const pg = getPool();
    const rs = await pg.query('SELECT payload FROM app_state WHERE id = $1', [STATE_KEY]);
    if (!rs.rowCount) throw new Error('No app_state row found');
    const payload = normalizeDatabaseShape(rs.rows[0].payload || {});
    return payload;
  }

  const raw = await fs.readFile(DATA_FILE, 'utf8');
  const parsed = JSON.parse(raw || '{}');
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('Invalid data format');
  }
  return normalizeDatabaseShape(parsed);
}

function writeData(nextData) {
  writeQueue = writeQueue.then(async () => {
    const normalized = normalizeDatabaseShape(nextData);

    if (USE_POSTGRES) {
      const pg = getPool();
      await pg.query('BEGIN');
      try {
        await pg.query('INSERT INTO app_snapshots (source, payload) VALUES ($1, $2::jsonb)', ['put', JSON.stringify(normalized)]);
        await pg.query(
          'INSERT INTO app_state (id, payload, updated_at) VALUES ($1, $2::jsonb, NOW()) ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()',
          [STATE_KEY, JSON.stringify(normalized)]
        );
        await pg.query('COMMIT');
      } catch (err) {
        await pg.query('ROLLBACK');
        throw err;
      }
      return;
    }

    await fs.mkdir(DATA_DIR, { recursive: true });
    await fs.mkdir(BACKUP_DIR, { recursive: true });
    const tmpFile = `${DATA_FILE}.tmp`;
    const payload = JSON.stringify(normalized, null, 2);

    try {
      const current = await fs.readFile(DATA_FILE, 'utf8');
      const stamp = new Date().toISOString().replace(/[:.]/g, '-');
      await fs.writeFile(path.join(BACKUP_DIR, `db-${stamp}.json`), current, 'utf8');
      await fs.writeFile(path.join(BACKUP_DIR, 'latest.json'), current, 'utf8');
    } catch (_) {
      // No previous file to backup.
    }

    try {
      await fs.writeFile(tmpFile, payload, 'utf8');
      await fs.rename(tmpFile, DATA_FILE);
    } catch (err) {
      await fs.writeFile(DATA_FILE, payload, 'utf8');
      console.warn('Atomic rename failed, fallback write used:', err && err.message ? err.message : err);
    }
  });
  return writeQueue;
}

function sendJson(res, statusCode, body, extraHeaders = {}) {
  res.writeHead(statusCode, { 'Content-Type': MIME_TYPES['.json'], ...extraHeaders });
  res.end(JSON.stringify(body));
}

function getSafePath(urlPath) {
  const decoded = decodeURIComponent(urlPath.split('?')[0]);
  const requested = decoded === '/' ? '/index.html' : decoded;
  const normalized = path.normalize(requested).replace(/^\.+/, '');
  return path.join(ROOT, normalized);
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => {
      raw += chunk;
      if (raw.length > 15 * 1024 * 1024) {
        reject(new Error('Payload too large'));
        req.destroy();
      }
    });
    req.on('end', () => resolve(raw));
    req.on('error', reject);
  });
}

function prunePresence(now = Date.now()) {
  for (const [sessionId, session] of activeSessions.entries()) {
    if (!session || session.expiresAt <= now) {
      activeSessions.delete(sessionId);
    }
  }
}

function touchPresence(sessionId, tab = '') {
  const now = Date.now();
  if (!sessionId) return;
  activeSessions.set(sessionId, { tab, updatedAt: now, expiresAt: now + PRESENCE_TTL_MS });
  prunePresence(now);
}

async function getLastSavedAt() {
  if (USE_POSTGRES) {
    const pg = getPool();
    const rs = await pg.query('SELECT updated_at FROM app_state WHERE id = $1', [STATE_KEY]);
    return rs.rowCount ? new Date(rs.rows[0].updated_at).toISOString() : null;
  }
  try {
    const stat = await fs.stat(DATA_FILE);
    return stat.mtime.toISOString();
  } catch {
    return null;
  }
}

async function getDataVersion() {
  if (USE_POSTGRES) {
    const pg = getPool();
    const rs = await pg.query('SELECT updated_at FROM app_state WHERE id = $1', [STATE_KEY]);
    return rs.rowCount ? String(new Date(rs.rows[0].updated_at).toISOString()) : '0';
  }
  try {
    const stat = await fs.stat(DATA_FILE);
    return String(Math.floor(stat.mtimeMs));
  } catch {
    return '0';
  }
}

async function readLatestBackup() {
  if (USE_POSTGRES) {
    const pg = getPool();
    const rs = await pg.query('SELECT payload FROM app_snapshots ORDER BY id DESC LIMIT 1');
    if (!rs.rowCount) throw new Error('No backup found');
    return normalizeDatabaseShape(rs.rows[0].payload || {});
  }

  const raw = await fs.readFile(path.join(BACKUP_DIR, 'latest.json'), 'utf8');
  const parsed = JSON.parse(raw || '{}');
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('Invalid backup format');
  }
  return normalizeDatabaseShape(parsed);
}

const server = http.createServer(async (req, res) => {
  try {
    const requestUrl = new URL(req.url || '/', 'http://localhost');
    const pathname = requestUrl.pathname;

    if (req.method === 'GET' && pathname === '/api/health') {
      sendJson(res, 200, {
        ok: true,
        service: 'driver-management',
        storage: USE_POSTGRES ? 'postgres' : 'json-file',
        now: new Date().toISOString(),
      });
      return;
    }

    if (req.method === 'GET' && pathname === '/api/data') {
      const data = await readData();
      const version = await getDataVersion();
      sendJson(res, 200, data, { ETag: `"${version}"` });
      return;
    }

    if (req.method === 'GET' && pathname === '/api/data/backup-latest') {
      try {
        const data = await readLatestBackup();
        sendJson(res, 200, data);
      } catch (err) {
        sendJson(res, 404, { error: 'No backup found', detail: err && err.message ? err.message : 'unknown' });
      }
      return;
    }

    if (req.method === 'GET' && pathname === '/api/presence') {
      prunePresence();
      const lastSavedAt = await getLastSavedAt();
      sendJson(res, 200, { onlineUsers: activeSessions.size, lastSavedAt, now: new Date().toISOString() });
      return;
    }

    if (req.method === 'POST' && pathname === '/api/presence/heartbeat') {
      const rawBody = await readRequestBody(req);
      let incoming;
      try {
        incoming = JSON.parse(rawBody || '{}');
      } catch {
        sendJson(res, 400, { error: 'Invalid JSON payload' });
        return;
      }
      const sessionId = typeof incoming.sessionId === 'string' ? incoming.sessionId.trim() : '';
      const tab = typeof incoming.tab === 'string' ? incoming.tab.trim() : '';
      if (!sessionId) {
        sendJson(res, 400, { error: 'sessionId is required' });
        return;
      }
      touchPresence(sessionId, tab);
      sendJson(res, 200, { ok: true, onlineUsers: activeSessions.size, now: new Date().toISOString() });
      return;
    }

    if (req.method === 'PUT' && pathname === '/api/data') {
      const currentVersion = await getDataVersion();
      const ifMatchRaw = typeof req.headers['if-match'] === 'string' ? req.headers['if-match'] : '';
      const ifMatch = ifMatchRaw.trim().replace(/^W\//, '').replace(/^"|"$/g, '');
      if (!ifMatch) {
        sendJson(res, 428, { error: 'Missing If-Match header. Please refresh and try again.' });
        return;
      }
      if (ifMatch !== currentVersion) {
        sendJson(res, 409, {
          error: 'Data has changed on the server. Please refresh before saving.',
          currentVersion,
        });
        return;
      }

      const rawBody = await readRequestBody(req);
      let incoming;
      try {
        incoming = JSON.parse(rawBody || '{}');
      } catch {
        sendJson(res, 400, { error: 'Invalid JSON payload' });
        return;
      }

      if (!incoming || typeof incoming !== 'object' || Array.isArray(incoming)) {
        sendJson(res, 400, { error: 'Payload must be a JSON object' });
        return;
      }

      await writeData(incoming);
      const nextVersion = await getDataVersion();
      sendJson(
        res,
        200,
        { ok: true, savedAt: new Date().toISOString(), storage: USE_POSTGRES ? 'postgres' : 'json-file' },
        { ETag: `"${nextVersion}"` }
      );
      return;
    }

    if (req.method !== 'GET' && req.method !== 'HEAD') {
      sendJson(res, 405, { error: 'Method not allowed' });
      return;
    }

    const filePath = getSafePath(pathname);
    if (!filePath.startsWith(ROOT)) {
      res.writeHead(403);
      res.end('Forbidden');
      return;
    }

    let finalPath = filePath;
    try {
      const stat = await fs.stat(finalPath);
      if (stat.isDirectory()) {
        finalPath = path.join(finalPath, 'index.html');
      }
    } catch {
      finalPath = path.join(ROOT, 'index.html');
    }

    const ext = path.extname(finalPath).toLowerCase();
    const mime = MIME_TYPES[ext] || 'application/octet-stream';

    try {
      const data = await fs.readFile(finalPath);
      res.writeHead(200, { 'Content-Type': mime });
      if (req.method === 'HEAD') {
        res.end();
        return;
      }
      res.end(data);
    } catch {
      const fallback = await fs.readFile(path.join(ROOT, 'index.html'));
      res.writeHead(200, { 'Content-Type': MIME_TYPES['.html'] });
      res.end(fallback);
    }
  } catch (err) {
    console.error('Request failed:', err);
    sendJson(res, 500, { error: 'Internal server error', detail: err && err.message ? err.message : 'unknown' });
  }
});

async function initializeStorage() {
  if (USE_POSTGRES) {
    await ensurePostgres();
    console.log('Storage mode: postgres');
    return;
  }
  await ensureDataFile();
  console.log('Storage mode: json-file');
}

initializeStorage()
  .then(() => {
    server.listen(PORT, () => {
      console.log(`Driver Management service listening on port ${PORT}`);
    });
  })
  .catch((err) => {
    console.error('Failed to initialize service:', err);
    process.exit(1);
  });
