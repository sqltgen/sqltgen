import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import mysql from 'mysql2/promise';

import * as queries from './gen/queries/index.js';

const FIXTURES = join(import.meta.dirname, '..');
const MYSQL_HOST = process.env['MYSQL_HOST'] ?? '127.0.0.1';
const MYSQL_PORT = parseInt(process.env['MYSQL_PORT'] ?? '13306', 10);

const ROOT_CONFIG = {
  host: MYSQL_HOST,
  port: MYSQL_PORT,
  user: 'root',
  password: 'sqltgen',
  database: 'sqltgen_e2e',
};

const TEST_CONFIG = {
  host: MYSQL_HOST,
  port: MYSQL_PORT,
  user: 'sqltgen',
  password: 'sqltgen',
  // Required so the driver returns BIGINT UNSIGNED as a JS bigint instead of
  // a string or a lossy Number; without this the round-trip silently truncates.
  supportBigNumbers: true,
  bigNumberStrings: false,
};

async function makeConn() {
  const dbName = 'test_' + randomBytes(16).toString('hex');
  const admin = await mysql.createConnection(ROOT_CONFIG);
  await admin.execute(`CREATE DATABASE \`${dbName}\``);
  await admin.execute(`GRANT ALL ON \`${dbName}\`.* TO 'sqltgen'@'%'`);
  await admin.end();
  const db = await mysql.createConnection({ ...TEST_CONFIG, database: dbName });
  const schemaSql = readFileSync(join(FIXTURES, 'schema.sql'), 'utf8');
  for (const stmt of schemaSql.split(';').map(s => s.trim()).filter(Boolean)) {
    await db.execute(stmt);
  }
  return { db, dbName };
}

async function teardown(db, dbName) {
  await db.end();
  const admin = await mysql.createConnection(ROOT_CONFIG);
  await admin.execute(`DROP DATABASE IF EXISTS \`${dbName}\``);
  await admin.end();
}

describe('UNSIGNED integers', () => {
  it('round-trip through full unsigned range', async () => {
    const { db, dbName } = await makeConn();
    try {
      await queries.insertUnsignedRow(db, 0, 0, 0, 0, 0n);
      await queries.insertUnsignedRow(db, 1, 1, 1, 1, 1n);
      const u64Max = (1n << 64n) - 1n;
      await queries.insertUnsignedRow(db, 255, 65535, 16777215, 4294967295, u64Max);

      const rows = await queries.getUnsignedRows(db);
      assert.equal(rows.length, 3);

      assert.deepEqual([rows[0].u8_val, rows[0].u16_val, rows[0].u24_val, rows[0].u32_val], [0, 0, 0, 0]);
      assert.equal(rows[0].u64_val, 0n);

      assert.deepEqual([rows[1].u8_val, rows[1].u16_val, rows[1].u24_val, rows[1].u32_val], [1, 1, 1, 1]);
      assert.equal(rows[1].u64_val, 1n);

      assert.equal(rows[2].u8_val, 255);
      assert.equal(rows[2].u16_val, 65535);
      assert.equal(rows[2].u24_val, 16777215);
      assert.equal(rows[2].u32_val, 4294967295);
      // The critical correctness gate: 2^64-1 must round-trip without truncation.
      assert.equal(rows[2].u64_val, u64Max);

      assert.equal(rows[0].id, 1n);
      assert.equal(rows[2].id, 3n);
    } finally { await teardown(db, dbName); }
  });
});
