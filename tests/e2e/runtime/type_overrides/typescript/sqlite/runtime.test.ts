/**
 * End-to-end runtime tests for type overrides: TypeScript/SQLite.
 *
 * Uses an in-memory better-sqlite3 database — no external services required.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import Database from 'better-sqlite3';

import * as queries from './gen/queries';

const FIXTURES = join(__dirname, '../../../../fixtures/type_overrides/sqlite');

// ─── Setup helpers ────────────────────────────────────────────────────────────

function makeDb(): Database.Database {
  const db = new Database(':memory:');
  db.exec(readFileSync(join(FIXTURES, 'schema.sql'), 'utf8'));
  return db;
}

// ─── :one tests ───────────────────────────────────────────────────────────────

describe(':one queries', () => {
  it('insertEvent and getEvent round-trip', async () => {
    const db = makeDb();
    await queries.insertEvent(db, 'login',
      JSON.stringify({ type: 'click', x: 10 }),
      JSON.stringify({ source: 'web' }),
      'doc-001',
      '2024-06-01 12:00:00',
      '2024-06-01 14:00:00',
      '2024-06-01',
      '09:00:00');

    const ev = await queries.getEvent(db, 1);
    assert.ok(ev);
    assert.equal(ev.name, 'login');
    assert.equal(ev.doc_id, 'doc-001');
    assert.equal(ev.event_date, '2024-06-01');
    assert.equal(ev.event_time, '09:00:00');
  });

  it('getEvent returns null for unknown id', async () => {
    const db = makeDb();
    assert.equal(await queries.getEvent(db, 999), null);
  });
});

// ─── :many tests ──────────────────────────────────────────────────────────────

describe(':many queries', () => {
  it('listEvents returns all events ordered by id', async () => {
    const db = makeDb();
    const ts = '2024-06-01 12:00:00';
    await queries.insertEvent(db, 'alpha', '{}', null, 'doc-1', ts, null, null, null);
    await queries.insertEvent(db, 'beta',  '{}', null, 'doc-2', ts, null, null, null);
    await queries.insertEvent(db, 'gamma', '{}', null, 'doc-3', ts, null, null, null);

    const events = await queries.listEvents(db);
    assert.equal(events.length, 3);
    assert.equal(events[0].name, 'alpha');
    assert.equal(events[1].name, 'beta');
    assert.equal(events[2].name, 'gamma');
  });

  it('getEventsByDateRange filters correctly', async () => {
    const db = makeDb();
    await queries.insertEvent(db, 'early', '{}', null, 'doc-1', '2024-01-01 10:00:00', null, null, null);
    await queries.insertEvent(db, 'mid',   '{}', null, 'doc-2', '2024-06-01 12:00:00', null, null, null);
    await queries.insertEvent(db, 'late',  '{}', null, 'doc-3', '2024-12-01 15:00:00', null, null, null);

    const events = await queries.getEventsByDateRange(db,
      '2024-01-01 00:00:00', '2024-07-01 00:00:00');

    assert.equal(events.length, 2);
    assert.equal(events[0].name, 'early');
    assert.equal(events[1].name, 'mid');
  });
});

// ─── :exec tests ──────────────────────────────────────────────────────────────

describe(':exec queries', () => {
  it('updatePayload changes payload', async () => {
    const db = makeDb();
    await queries.insertEvent(db, 'test', '{"v":1}', null, 'doc-1',
      '2024-06-01 12:00:00', null, null, null);

    await queries.updatePayload(db, '{"v":2}', null, 1);

    const ev = await queries.getEvent(db, 1);
    assert.ok(ev);
    assert.equal(ev.payload, '{"v":2}');
    assert.equal(ev.meta, null);
  });

  it('updateEventDate updates the date', async () => {
    const db = makeDb();
    await queries.insertEvent(db, 'dated', '{}', null, 'doc-1',
      '2024-06-01 12:00:00', null, '2024-01-01', null);

    await queries.updateEventDate(db, '2024-12-31', 1);

    const ev = await queries.getEvent(db, 1);
    assert.ok(ev);
    assert.equal(ev.event_date, '2024-12-31');
  });
});

// ─── :execrows tests ──────────────────────────────────────────────────────────

describe(':execrows queries', () => {
  it('insertEventRows returns row count', async () => {
    const db = makeDb();
    const n = await queries.insertEventRows(db, 'rowtest', '{}', null, 'doc-1',
      '2024-06-01 12:00:00', null, null, null);
    assert.equal(n, 1);
  });
});

// ─── projection tests ─────────────────────────────────────────────────────────

describe('projection queries', () => {
  it('findByDate returns the matching event', async () => {
    const db = makeDb();
    await queries.insertEvent(db, 'dated', '{}', null, 'doc-1',
      '2024-06-01 12:00:00', null, '2024-06-15', null);

    const row = await queries.findByDate(db, '2024-06-15');
    assert.ok(row);
    assert.equal(row.name, 'dated');
  });

  it('findByDocId returns the matching event', async () => {
    const db = makeDb();
    await queries.insertEvent(db, 'doctest', '{}', null, 'unique-doc-id',
      '2024-06-01 12:00:00', null, null, null);

    const row = await queries.findByDocId(db, 'unique-doc-id');
    assert.ok(row);
    assert.equal(row.name, 'doctest');
  });
});

// ─── count tests ──────────────────────────────────────────────────────────────

describe('count queries', () => {
  it('countEvents counts correctly', async () => {
    const db = makeDb();
    for (let i = 1; i <= 3; i++) {
      await queries.insertEvent(db, `ev${i}`, '{}', null, `doc-${i}`,
        `2024-06-0${i} 00:00:00`, null, null, null);
    }

    const row = await queries.countEvents(db, '2024-01-01 00:00:00');
    assert.ok(row);
    assert.equal(Number(row.total), 3);
  });
});
