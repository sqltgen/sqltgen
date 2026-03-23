/**
 * End-to-end runtime tests for type overrides: TypeScript/PostgreSQL.
 *
 * Each test runs in its own PostgreSQL schema for isolation.
 * Requires the docker-compose postgres service on port 15432.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { Client } from 'pg';
import { randomBytes, randomUUID } from 'node:crypto';

import * as queries from './gen/queries';

const FIXTURES = join(__dirname, '../../../../fixtures/type_overrides');
const DATABASE_URL = process.env['DATABASE_URL']
  ?? 'postgresql://sqltgen:sqltgen@localhost:15432/sqltgen_e2e';

// ─── Setup helpers ────────────────────────────────────────────────────────────

async function makeClient(): Promise<{ client: Client; schema: string }> {
  const schema = 'test_' + randomBytes(16).toString('hex');
  const schemaSql = readFileSync(join(FIXTURES, 'schema.sql'), 'utf8');

  const client = new Client({ connectionString: DATABASE_URL });
  await client.connect();
  await client.query(`CREATE SCHEMA "${schema}"`);
  await client.query(`SET search_path TO "${schema}"`);
  await client.query(schemaSql);
  return { client, schema };
}

async function teardown(client: Client, schema: string): Promise<void> {
  await client.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await client.end();
}

// ─── :one tests ───────────────────────────────────────────────────────────────

describe(':one queries', () => {
  it('insertEvent and getEvent round-trip', async () => {
    const { client, schema } = await makeClient();
    try {
      const payload = { type: 'click', x: 10 };
      const meta = { source: 'web' };
      const docId = randomUUID();
      await queries.insertEvent(client, 'login', payload, meta, docId,
        new Date('2024-06-01T12:00:00Z'), new Date('2024-06-01T14:00:00Z'), new Date('2024-06-01T00:00:00Z'), null);

      const ev = await queries.getEvent(client, 1);
      assert.ok(ev);
      assert.equal(ev.name, 'login');
      assert.deepEqual(ev.payload, payload);
      assert.deepEqual(ev.meta, meta);
      assert.ok(ev.scheduled_at);
    } finally { await teardown(client, schema); }
  });

  it('getEvent returns null for unknown id', async () => {
    const { client, schema } = await makeClient();
    try {
      assert.equal(await queries.getEvent(client, 999), null);
    } finally { await teardown(client, schema); }
  });
});

// ─── :many tests ──────────────────────────────────────────────────────────────

describe(':many queries', () => {
  it('listEvents returns all events ordered by id', async () => {
    const { client, schema } = await makeClient();
    try {
      const ts = new Date('2024-06-01T12:00:00Z');
      await queries.insertEvent(client, 'alpha', {}, null, randomUUID(), ts, null, null, null);
      await queries.insertEvent(client, 'beta',  {}, null, randomUUID(), ts, null, null, null);
      await queries.insertEvent(client, 'gamma', {}, null, randomUUID(), ts, null, null, null);

      const events = await queries.listEvents(client);
      assert.equal(events.length, 3);
      assert.equal(events[0].name, 'alpha');
      assert.equal(events[1].name, 'beta');
      assert.equal(events[2].name, 'gamma');
    } finally { await teardown(client, schema); }
  });

  it('getEventsByDateRange filters correctly', async () => {
    const { client, schema } = await makeClient();
    try {
      await queries.insertEvent(client, 'early', {}, null, randomUUID(), new Date('2024-01-01T10:00:00Z'), null, null, null);
      await queries.insertEvent(client, 'mid',   {}, null, randomUUID(), new Date('2024-06-01T12:00:00Z'), null, null, null);
      await queries.insertEvent(client, 'late',  {}, null, randomUUID(), new Date('2024-12-01T15:00:00Z'), null, null, null);

      const events = await queries.getEventsByDateRange(client,
        new Date('2024-01-01T00:00:00Z'),
        new Date('2024-07-01T00:00:00Z'));

      assert.equal(events.length, 2);
      assert.equal(events[0].name, 'early');
      assert.equal(events[1].name, 'mid');
    } finally { await teardown(client, schema); }
  });
});

// ─── :exec tests ──────────────────────────────────────────────────────────────

describe(':exec queries', () => {
  it('updatePayload changes payload and meta', async () => {
    const { client, schema } = await makeClient();
    try {
      await queries.insertEvent(client, 'test', { v: 1 }, { source: 'web' }, randomUUID(),
        new Date('2024-06-01T12:00:00Z'), null, null, null);

      const updated = { v: 2, changed: true };
      await queries.updatePayload(client, updated, null, 1);

      const ev = await queries.getEvent(client, 1);
      assert.ok(ev);
      assert.deepEqual(ev.payload, updated);
      assert.equal(ev.meta, null);
    } finally { await teardown(client, schema); }
  });

  it('updateEventDate updates the date', async () => {
    const { client, schema } = await makeClient();
    try {
      await queries.insertEvent(client, 'dated', {}, null, randomUUID(),
        new Date('2024-06-01T12:00:00Z'), null, new Date('2024-01-01T00:00:00Z'), null);

      await queries.updateEventDate(client, new Date('2024-12-31T00:00:00Z'), 1);

      const ev = await queries.getEvent(client, 1);
      assert.ok(ev);
      assert.ok(ev.event_date);
    } finally { await teardown(client, schema); }
  });
});

// ─── :execrows tests ──────────────────────────────────────────────────────────

describe(':execrows queries', () => {
  it('insertEventRows returns row count', async () => {
    const { client, schema } = await makeClient();
    try {
      const n = await queries.insertEventRows(client, 'rowtest', {}, null, randomUUID(),
        new Date('2024-06-01T12:00:00Z'), null, null, null);
      assert.equal(n, 1);
    } finally { await teardown(client, schema); }
  });
});

// ─── projection tests ─────────────────────────────────────────────────────────

describe('projection queries', () => {
  it('findByDate returns the matching event', async () => {
    const { client, schema } = await makeClient();
    try {
      await queries.insertEvent(client, 'dated', {}, null, randomUUID(),
        new Date('2024-06-01T12:00:00Z'), null, new Date('2024-06-15T00:00:00Z'), null);

      const row = await queries.findByDate(client, new Date('2024-06-15T00:00:00Z'));
      assert.ok(row);
      assert.equal(row.name, 'dated');
    } finally { await teardown(client, schema); }
  });

  it('findByUuid returns the matching event', async () => {
    const { client, schema } = await makeClient();
    try {
      const docId = randomUUID();
      await queries.insertEvent(client, 'uuid-test', {}, null, docId,
        new Date('2024-06-01T12:00:00Z'), null, null, null);

      const row = await queries.findByUuid(client, docId);
      assert.ok(row);
      assert.equal(row.name, 'uuid-test');
    } finally { await teardown(client, schema); }
  });
});

// ─── count tests ──────────────────────────────────────────────────────────────

describe('count queries', () => {
  it('countEvents counts correctly', async () => {
    const { client, schema } = await makeClient();
    try {
      for (let i = 1; i <= 3; i++) {
        await queries.insertEvent(client, `ev${i}`, {}, null, randomUUID(),
          new Date(`2024-06-0${i}T00:00:00Z`), null, null, null);
      }

      const row = await queries.countEvents(client, new Date('2024-01-01T00:00:00Z'));
      assert.ok(row);
      assert.equal(Number(row.total), 3);
    } finally { await teardown(client, schema); }
  });
});
