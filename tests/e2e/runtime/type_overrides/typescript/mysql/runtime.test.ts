/**
 * End-to-end runtime tests for type overrides: TypeScript/MySQL.
 *
 * Each test creates a dedicated MySQL database named test_<uuid> for full
 * isolation. Requires the docker-compose MySQL service on port 13306.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { readFileSync } from 'node:fs';
import { randomBytes } from 'node:crypto';
import mysql from 'mysql2/promise';

import * as queries from './gen/queries';

const FIXTURES = join(__dirname, '../../../../fixtures/type_overrides/mysql');

const MYSQL_HOST = process.env['MYSQL_HOST'] ?? '127.0.0.1';
const MYSQL_PORT = parseInt(process.env['MYSQL_PORT'] ?? '13306', 10);

const ROOT_CONFIG = {
  host: MYSQL_HOST,
  port: MYSQL_PORT,
  user: 'sqltgen',
  password: 'sqltgen',
  database: 'sqltgen_e2e',
};

const TEST_CONFIG = {
  host: MYSQL_HOST,
  port: MYSQL_PORT,
  user: 'sqltgen',
  password: 'sqltgen',
};

// ─── Setup helpers ────────────────────────────────────────────────────────────

async function makeConn(): Promise<{ conn: mysql.Connection; dbName: string }> {
  const dbName = 'test_' + randomBytes(16).toString('hex');

  const admin = await mysql.createConnection(ROOT_CONFIG);
  await admin.execute(`CREATE DATABASE \`${dbName}\``);
  await admin.execute(`GRANT ALL ON \`${dbName}\`.* TO 'sqltgen'@'%'`);
  await admin.end();

  const conn = await mysql.createConnection({ ...TEST_CONFIG, database: dbName });
  const schemaSql = readFileSync(join(FIXTURES, 'schema.sql'), 'utf8');
  for (const stmt of schemaSql.split(';').map(s => s.trim()).filter(Boolean)) {
    await conn.execute(stmt);
  }
  return { conn, dbName };
}

async function teardown(conn: mysql.Connection, dbName: string): Promise<void> {
  await conn.end();
  const admin = await mysql.createConnection(ROOT_CONFIG);
  await admin.execute(`DROP DATABASE IF EXISTS \`${dbName}\``);
  await admin.end();
}

// ─── :one tests ───────────────────────────────────────────────────────────────

describe(':one queries', () => {
  it('insertEvent and getEvent round-trip', async () => {
    const { conn, dbName } = await makeConn();
    try {
      const payload = { type: 'click', x: 10 };
      const meta = { source: 'web' };
      await queries.insertEvent(conn, 'login',
        payload,
        meta,
        'doc-001',
        new Date('2024-06-01T12:00:00Z'),
        new Date('2024-06-01T14:00:00Z'),
        '2024-06-01',
        new Date('1970-01-01T09:00:00Z'));

      const ev = await queries.getEvent(conn, 1);
      assert.ok(ev);
      assert.equal(ev.name, 'login');
      assert.equal(ev.doc_id, 'doc-001');
      assert.deepEqual(ev.payload, payload);
      assert.deepEqual(ev.meta, meta);
      assert.ok(ev.scheduled_at);
    } finally { await teardown(conn, dbName); }
  });

  it('getEvent returns null for unknown id', async () => {
    const { conn, dbName } = await makeConn();
    try {
      assert.equal(await queries.getEvent(conn, 999), null);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── :many tests ──────────────────────────────────────────────────────────────

describe(':many queries', () => {
  it('listEvents returns all events ordered by id', async () => {
    const { conn, dbName } = await makeConn();
    try {
      const ts = new Date('2024-06-01T12:00:00Z');
      await queries.insertEvent(conn, 'alpha', {}, null, 'doc-1', ts, null, null, null);
      await queries.insertEvent(conn, 'beta',  {}, null, 'doc-2', ts, null, null, null);
      await queries.insertEvent(conn, 'gamma', {}, null, 'doc-3', ts, null, null, null);

      const events = await queries.listEvents(conn);
      assert.equal(events.length, 3);
      assert.equal(events[0].name, 'alpha');
      assert.equal(events[1].name, 'beta');
      assert.equal(events[2].name, 'gamma');
    } finally { await teardown(conn, dbName); }
  });

  it('getEventsByDateRange filters correctly', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await queries.insertEvent(conn, 'early', {}, null, 'doc-1', new Date('2024-01-01T10:00:00Z'), null, null, null);
      await queries.insertEvent(conn, 'mid',   {}, null, 'doc-2', new Date('2024-06-01T12:00:00Z'), null, null, null);
      await queries.insertEvent(conn, 'late',  {}, null, 'doc-3', new Date('2024-12-01T15:00:00Z'), null, null, null);

      const events = await queries.getEventsByDateRange(conn,
        new Date('2024-01-01T00:00:00Z'), new Date('2024-07-01T00:00:00Z'));

      assert.equal(events.length, 2);
      assert.equal(events[0].name, 'early');
      assert.equal(events[1].name, 'mid');
    } finally { await teardown(conn, dbName); }
  });
});

// ─── :exec tests ──────────────────────────────────────────────────────────────

describe(':exec queries', () => {
  it('updatePayload changes payload and meta', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await queries.insertEvent(conn, 'test', { v: 1 }, { source: 'web' }, 'doc-1',
        new Date('2024-06-01T12:00:00Z'), null, null, null);

      const updated = { v: 2, changed: true };
      await queries.updatePayload(conn, updated, null, 1);

      const ev = await queries.getEvent(conn, 1);
      assert.ok(ev);
      assert.deepEqual(ev.payload, updated);
      assert.equal(ev.meta, null);
    } finally { await teardown(conn, dbName); }
  });

  it('updateEventDate updates the date', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await queries.insertEvent(conn, 'dated', {}, null, 'doc-1',
        new Date('2024-06-01T12:00:00Z'), null, '2024-01-01', null);

      await queries.updateEventDate(conn, '2024-12-31', 1);

      const ev = await queries.getEvent(conn, 1);
      assert.ok(ev);
      assert.ok(ev.event_date);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── :execrows tests ──────────────────────────────────────────────────────────

describe(':execrows queries', () => {
  it('insertEventRows returns row count', async () => {
    const { conn, dbName } = await makeConn();
    try {
      const n = await queries.insertEventRows(conn, 'rowtest', {}, null, 'doc-1',
        new Date('2024-06-01T12:00:00Z'), null, null, null);
      assert.equal(n, 1);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── projection tests ─────────────────────────────────────────────────────────

describe('projection queries', () => {
  it('findByDate returns the matching event', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await queries.insertEvent(conn, 'dated', {}, null, 'doc-1',
        new Date('2024-06-01T12:00:00Z'), null, '2024-06-15', null);

      const row = await queries.findByDate(conn, '2024-06-15');
      assert.ok(row);
      assert.equal(row.name, 'dated');
    } finally { await teardown(conn, dbName); }
  });

  it('findByDocId returns the matching event', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await queries.insertEvent(conn, 'doctest', {}, null, 'unique-doc-id',
        new Date('2024-06-01T12:00:00Z'), null, null, null);

      const row = await queries.findByDocId(conn, 'unique-doc-id');
      assert.ok(row);
      assert.equal(row.name, 'doctest');
    } finally { await teardown(conn, dbName); }
  });
});

// ─── count tests ──────────────────────────────────────────────────────────────

describe('count queries', () => {
  it('countEvents counts correctly', async () => {
    const { conn, dbName } = await makeConn();
    try {
      for (let i = 1; i <= 3; i++) {
        await queries.insertEvent(conn, `ev${i}`, {}, null, `doc-${i}`,
          new Date(`2024-06-0${i}T00:00:00Z`), null, null, null);
      }

      const row = await queries.countEvents(conn, new Date('2024-01-01T00:00:00Z'));
      assert.ok(row);
      assert.equal(Number(row.total), 3);
    } finally { await teardown(conn, dbName); }
  });
});
