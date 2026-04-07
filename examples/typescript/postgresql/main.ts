import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';

import pg from 'pg';

import * as queries from './gen/queries';

// Parse BIGINT (OID 20) and BIGSERIAL as JS numbers rather than strings.
pg.types.setTypeParser(20, (val: string) => parseInt(val, 10));
// Parse NUMERIC/DECIMAL (OID 1700) as JS numbers rather than strings.
pg.types.setTypeParser(1700, (val: string) => parseFloat(val));

const HOST = 'localhost';
const PORT = 5433;
const USER = 'sqltgen';
const PASS = 'sqltgen';

async function seed(db: pg.Client): Promise<void> {
  const leGuin  = await queries.createAuthor(db, 'Ursula K. Le Guin', 'Science fiction and fantasy author', 1929);
  const herbert = await queries.createAuthor(db, 'Frank Herbert',     'Author of the Dune series',          1920);
  const asimov  = await queries.createAuthor(db, 'Isaac Asimov',      null,                                 1920);
  console.log(`[pg] inserted 3 authors (ids: ${leGuin!.id}, ${herbert!.id}, ${asimov!.id})`);

  const lhod  = await queries.createBook(db, leGuin!.id,  'The Left Hand of Darkness', 'fiction', 12.99, null);
  const disp  = await queries.createBook(db, leGuin!.id,  'The Dispossessed',           'fiction', 11.50, null);
  const dune  = await queries.createBook(db, herbert!.id, 'Dune',                       'science', 14.99, null);
  const found = await queries.createBook(db, asimov!.id,  'Foundation',                 'science', 10.99, null);
  await queries.createBook(db, asimov!.id, 'The Caves of Steel', 'fiction', 9.99, null);
  console.log('[pg] inserted 5 books');

  // Suppress "declared but never read" warnings for unused variables.
  void disp;

  const alice = await queries.createCustomer(db, 'Alice', 'alice@example.com');
  const bob   = await queries.createCustomer(db, 'Bob',   'bob@example.com');
  console.log('[pg] inserted 2 customers');

  const sale1 = await queries.createSale(db, alice!.id);
  await queries.addSaleItem(db, sale1!.id, dune!.id,  2, 14.99);
  await queries.addSaleItem(db, sale1!.id, found!.id, 1, 10.99);
  const sale2 = await queries.createSale(db, bob!.id);
  await queries.addSaleItem(db, sale2!.id, dune!.id, 1, 14.99);
  await queries.addSaleItem(db, sale2!.id, lhod!.id, 1, 12.99);
  console.log('[pg] inserted 2 sales with items');
}

async function query(db: pg.Client): Promise<void> {
  const authors = await queries.listAuthors(db);
  console.log(`[pg] listAuthors: ${authors.length} row(s)`);

  // Book IDs are BIGSERIAL starting at 1 on a fresh DB; 1=Left Hand, 3=Dune.
  const byIds = await queries.getBooksByIds(db, [1, 3]);
  console.log(`[pg] getBooksByIds([1,3]): ${byIds.length} row(s)`);
  for (const b of byIds) console.log(`  "${b.title}"`);

  const scifi = await queries.listBooksByGenre(db, 'science');
  console.log(`[pg] listBooksByGenre(science): ${scifi.length} row(s)`);

  const allBooks = await queries.listBooksByGenreOrAll(db, null);
  console.log(`[pg] listBooksByGenreOrAll(null): ${allBooks.length} row(s) (nullable-param demo)`);
  const scifi2 = await queries.listBooksByGenreOrAll(db, 'science');
  console.log(`[pg] listBooksByGenreOrAll(science): ${scifi2.length} row(s)`);

  console.log('[pg] listBooksWithAuthor:');
  for (const r of await queries.listBooksWithAuthor(db)) {
    console.log(`  "${r.title}" by ${r.author_name}`);
  }

  const neverOrdered = await queries.getBooksNeverOrdered(db);
  console.log(`[pg] getBooksNeverOrdered: ${neverOrdered.length} book(s)`);
  for (const b of neverOrdered) console.log(`  "${b.title}"`);

  console.log('[pg] getTopSellingBooks:');
  for (const r of await queries.getTopSellingBooks(db)) {
    console.log(`  "${r.title}" sold ${r.units_sold}`);
  }

  console.log('[pg] getBestCustomers:');
  for (const r of await queries.getBestCustomers(db)) {
    console.log(`  ${r.name} spent ${r.total_spent}`);
  }

  // Demonstrate UPDATE RETURNING and DELETE RETURNING with a transient author.
  const temp    = await queries.createAuthor(db, 'Temp Author', null, null);
  const updated = await queries.updateAuthorBio(db, 'Updated via UPDATE RETURNING', temp!.id);
  if (updated) console.log(`[pg] updateAuthorBio: updated "${updated.name}" — bio: ${updated.bio}`);
  const deleted = await queries.deleteAuthor(db, temp!.id);
  if (deleted) console.log(`[pg] deleteAuthor: deleted "${deleted.name}" (id=${deleted.id})`);
}

async function run(client: pg.Client): Promise<void> {
  await seed(client);
  await query(client);
}

async function main(): Promise<void> {
  const migrationsDir = process.env['MIGRATIONS_DIR'];

  if (!migrationsDir) {
    const dbUrl = process.env['DATABASE_URL'] ?? `postgresql://${USER}:${PASS}@${HOST}:${PORT}/sqltgen`;
    const client = new pg.Client(dbUrl);
    await client.connect();
    try {
      await run(client);
    } finally {
      await client.end();
    }
    return;
  }

  const dbName   = `sqltgen_${crypto.randomBytes(4).toString('hex')}`;
  const adminUrl = `postgresql://${USER}:${PASS}@${HOST}:${PORT}/postgres`;
  const dbUrl    = `postgresql://${USER}:${PASS}@${HOST}:${PORT}/${dbName}`;

  const admin = new pg.Client(adminUrl);
  await admin.connect();
  await admin.query(`CREATE DATABASE "${dbName}"`);
  await admin.end();

  const client = new pg.Client(dbUrl);
  await client.connect();
  try {
    const migrationFiles = fs.readdirSync(migrationsDir)
      .filter(f => f.endsWith('.sql'))
      .sort()
      .map(f => path.join(migrationsDir, f));
    for (const f of migrationFiles) {
      const sql = fs.readFileSync(f, 'utf8');
      await client.query(sql);
    }
    await run(client);
  } finally {
    await client.end();
    const dropAdmin = new pg.Client(adminUrl);
    await dropAdmin.connect();
    try {
      await dropAdmin.query(`DROP DATABASE IF EXISTS "${dbName}"`);
    } catch (e) {
      console.warn(`[pg] warning: could not drop database ${dbName}: ${e}`);
    } finally {
      await dropAdmin.end();
    }
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
