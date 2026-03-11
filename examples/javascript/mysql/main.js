import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';

import mysql from 'mysql2/promise';

import * as queries from './gen/queries.js';

const HOST      = process.env['MYSQL_HOST']     ?? '127.0.0.1';
const PORT      = parseInt(process.env['MYSQL_PORT'] ?? '3307', 10);
const USER      = process.env['MYSQL_USER']     ?? 'sqltgen';
const PASS      = process.env['MYSQL_PASSWORD'] ?? 'sqltgen';
const ROOT_USER = 'root';
const ROOT_PASS = 'sqltgen_root';

async function seed(db) {
  await queries.createAuthor(db, 'Ursula K. Le Guin', 'Science fiction and fantasy author', 1929);
  await queries.createAuthor(db, 'Frank Herbert',     'Author of the Dune series',          1920);
  await queries.createAuthor(db, 'Isaac Asimov',      null,                                 1920);
  console.log('[mysql] inserted 3 authors');

  // MySQL has no RETURNING — use known auto-increment IDs (1-based, fresh schema).
  await queries.createBook(db, 1, 'The Left Hand of Darkness', 'sci-fi', 12.99, null);
  await queries.createBook(db, 1, 'The Dispossessed',           'sci-fi', 11.50, null);
  await queries.createBook(db, 2, 'Dune',                       'sci-fi', 14.99, null);
  await queries.createBook(db, 3, 'Foundation',                 'sci-fi', 10.99, null);
  await queries.createBook(db, 3, 'The Caves of Steel',         'sci-fi', 9.99,  null);
  console.log('[mysql] inserted 5 books');

  await queries.createCustomer(db, 'Eve',   'eve@example.com');
  await queries.createCustomer(db, 'Frank', 'frank@example.com');
  console.log('[mysql] inserted 2 customers');

  await queries.createSale(db, 1);
  await queries.addSaleItem(db, 1, 3, 2, 14.99);
  await queries.addSaleItem(db, 1, 4, 1, 10.99);
  await queries.createSale(db, 2);
  await queries.addSaleItem(db, 2, 3, 1, 14.99);
  await queries.addSaleItem(db, 2, 1, 1, 12.99);
  console.log('[mysql] inserted 2 sales with items');
}

async function query(db) {
  const authors = await queries.listAuthors(db);
  console.log(`[mysql] listAuthors: ${authors.length} row(s)`);

  // Books inserted in seed have IDs 1–5; 1=Left Hand, 3=Dune.
  const byIds = await queries.getBooksByIds(db, [1, 3]);
  console.log(`[mysql] getBooksByIds([1,3]): ${byIds.length} row(s)`);
  for (const b of byIds) console.log(`  "${b.title}"`);

  const scifi = await queries.listBooksByGenre(db, 'sci-fi');
  console.log(`[mysql] listBooksByGenre(sci-fi): ${scifi.length} row(s)`);

  const allBooks = await queries.listBooksByGenreOrAll(db, 'all');
  console.log(`[mysql] listBooksByGenreOrAll(all): ${allBooks.length} row(s) (repeated-param demo)`);
  const scifi2 = await queries.listBooksByGenreOrAll(db, 'sci-fi');
  console.log(`[mysql] listBooksByGenreOrAll(sci-fi): ${scifi2.length} row(s)`);

  console.log('[mysql] listBooksWithAuthor:');
  for (const r of await queries.listBooksWithAuthor(db)) {
    console.log(`  "${r.title}" by ${r.author_name}`);
  }

  const neverOrdered = await queries.getBooksNeverOrdered(db);
  console.log(`[mysql] getBooksNeverOrdered: ${neverOrdered.length} book(s)`);
  for (const b of neverOrdered) console.log(`  "${b.title}"`);

  console.log('[mysql] getTopSellingBooks:');
  for (const r of await queries.getTopSellingBooks(db)) {
    console.log(`  "${r.title}" sold ${r.units_sold}`);
  }

  console.log('[mysql] getBestCustomers:');
  for (const r of await queries.getBestCustomers(db)) {
    console.log(`  ${r.name} spent ${r.total_spent}`);
  }

  // Demonstrate UPDATE and DELETE with a transient author (id=4, inserted last).
  await queries.updateAuthorBio(db, 'Updated bio', 4);
  console.log('[mysql] updateAuthorBio: updated author id=4');
  await queries.deleteAuthor(db, 4);
  console.log('[mysql] deleteAuthor: deleted author id=4');
}

async function run(dbName) {
  const db = await mysql.createConnection({
    host: HOST, port: PORT,
    user: USER, password: PASS,
    database: dbName,
  });
  try {
    await seed(db);
    await query(db);
  } finally {
    await db.end();
  }
}

async function main() {
  const migrationsDir = process.env['MIGRATIONS_DIR'];

  if (!migrationsDir) {
    await run(process.env['MYSQL_DATABASE'] ?? 'sqltgen');
    return;
  }

  const dbName = `sqltgen_${crypto.randomBytes(4).toString('hex')}`;

  const admin = await mysql.createConnection({
    host: HOST, port: PORT,
    user: ROOT_USER, password: ROOT_PASS,
    multipleStatements: false,
  });
  try {
    await admin.execute(`CREATE DATABASE \`${dbName}\``);
    await admin.execute(`GRANT ALL ON \`${dbName}\`.* TO '${USER}'@'%'`);

    const migConn = await mysql.createConnection({
      host: HOST, port: PORT,
      user: USER, password: PASS,
      database: dbName,
      multipleStatements: true,
    });
    try {
      const migrationFiles = fs.readdirSync(migrationsDir)
        .filter(f => f.endsWith('.sql'))
        .sort()
        .map(f => path.join(migrationsDir, f));
      for (const f of migrationFiles) {
        await migConn.query(fs.readFileSync(f, 'utf8'));
      }
    } finally {
      await migConn.end();
    }

    await run(dbName);
  } finally {
    try {
      await admin.execute(`DROP DATABASE IF EXISTS \`${dbName}\``);
    } catch (e) {
      console.warn(`[mysql] warning: could not drop database ${dbName}: ${e}`);
    }
    await admin.end();
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
