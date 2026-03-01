const fs = require('fs/promises');
const path = require('path');
const { Pool } = require('pg');

const ROOT = path.join(__dirname, '..');
const DATA_FILE = path.join(ROOT, 'data', 'db.json');
const SEED_FILE = path.join(ROOT, 'seed-db.json');
const STATE_KEY = 'main';

async function readJsonSafe(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw || '{}');
  } catch {
    return null;
  }
}

async function main() {
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required');
  }

  const ssl = String(process.env.PGSSL_DISABLE || '').toLowerCase() === 'true'
    ? false
    : { rejectUnauthorized: false };

  const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl });

  const candidate = (await readJsonSafe(DATA_FILE)) || (await readJsonSafe(SEED_FILE)) || {};

  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_state (
      id TEXT PRIMARY KEY,
      payload JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_snapshots (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      source TEXT NOT NULL DEFAULT 'migration',
      payload JSONB NOT NULL
    );
  `);

  await pool.query('BEGIN');
  try {
    await pool.query(
      'INSERT INTO app_state (id, payload, updated_at) VALUES ($1, $2::jsonb, NOW()) ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()',
      [STATE_KEY, JSON.stringify(candidate)]
    );
    await pool.query('INSERT INTO app_snapshots (source, payload) VALUES ($1, $2::jsonb)', ['migration', JSON.stringify(candidate)]);
    await pool.query('COMMIT');
  } catch (err) {
    await pool.query('ROLLBACK');
    throw err;
  } finally {
    await pool.end();
  }

  console.log('Migrated JSON data to Postgres app_state successfully.');
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
