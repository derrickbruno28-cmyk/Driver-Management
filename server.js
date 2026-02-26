const http = require('http');
const fs = require('fs/promises');
const path = require('path');

const PORT = Number(process.env.PORT || 3000);
const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, 'data');
const DATA_FILE = path.join(DATA_DIR, 'db.json');
const SEED_FILE = path.join(ROOT, 'seed-db.json');
const PRESENCE_TTL_MS = 45 * 1000;

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
const activeSessions = new Map();

async function ensureDataFile() {
  await fs.mkdir(DATA_DIR, { recursive: true });
  try {
    await fs.access(DATA_FILE);
    const raw = await fs.readFile(DATA_FILE, 'utf8');
    const parsed = JSON.parse(raw || '{}');
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed) && Object.keys(parsed).length > 0) {
      return;
    }
  } catch {
    // Continue to seed write below.
  }
  try {
    const seedRaw = await fs.readFile(SEED_FILE, 'utf8');
    const seedParsed = JSON.parse(seedRaw || '{}');
    await fs.writeFile(DATA_FILE, JSON.stringify(seedParsed, null, 2), 'utf8');
  } catch {
    await fs.writeFile(DATA_FILE, JSON.stringify({}, null, 2), 'utf8');
  }
}

async function readData() {
  const raw = await fs.readFile(DATA_FILE, 'utf8');
  const parsed = JSON.parse(raw || '{}');
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('Invalid data format');
  }
  return parsed;
}

function writeData(nextData) {
  writeQueue = writeQueue.then(async () => {
    const tmpFile = `${DATA_FILE}.tmp`;
    const payload = JSON.stringify(nextData, null, 2);
    await fs.writeFile(tmpFile, payload, 'utf8');
    await fs.rename(tmpFile, DATA_FILE);
  });
  return writeQueue;
}

function sendJson(res, statusCode, body) {
  res.writeHead(statusCode, { 'Content-Type': MIME_TYPES['.json'] });
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
  try {
    const stat = await fs.stat(DATA_FILE);
    return stat.mtime.toISOString();
  } catch {
    return null;
  }
}

const server = http.createServer(async (req, res) => {
  try {
    const requestUrl = new URL(req.url || '/', 'http://localhost');
    const pathname = requestUrl.pathname;

    if (req.method === 'GET' && pathname === '/api/health') {
      sendJson(res, 200, { ok: true, service: 'driver-management', now: new Date().toISOString() });
      return;
    }

    if (req.method === 'GET' && pathname === '/api/data') {
      const data = await readData();
      sendJson(res, 200, data);
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
      sendJson(res, 200, { ok: true, savedAt: new Date().toISOString() });
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
    sendJson(res, 500, { error: 'Internal server error' });
  }
});

ensureDataFile()
  .then(() => {
    server.listen(PORT, () => {
      console.log(`Driver Management service listening on port ${PORT}`);
    });
  })
  .catch((err) => {
    console.error('Failed to initialize service:', err);
    process.exit(1);
  });
