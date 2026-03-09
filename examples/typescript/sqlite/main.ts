import * as fs from 'fs';
import * as path from 'path';

import Database from 'better-sqlite3';

import * as queries from './gen/queries';

const MIGRATIONS_DIR = path.join(__dirname, '../../common/sqlite/migrations');

function applyMigrations(db: Database.Database): void {
  const files = fs.readdirSync(MIGRATIONS_DIR)
    .filter(f => f.endsWith('.sql'))
    .sort()
    .map(f => path.join(MIGRATIONS_DIR, f));
  for (const f of files) {
    const sql = fs.readFileSync(f, 'utf8');
    for (const stmt of sql.split(';')) {
      const s = stmt.trim();
      if (s) db.exec(s);
    }
  }
}

async function seed(db: Database.Database): Promise<void> {
  await queries.createAuthor(db, 'Ursula K. Le Guin', 'Science fiction and fantasy author', 1929);
  await queries.createAuthor(db, 'Frank Herbert',     'Author of the Dune series',          1920);
  await queries.createAuthor(db, 'Isaac Asimov',      null,                                 1920);
  console.log('[sqlite] inserted 3 authors');

  // SQLite has no RETURNING — use known auto-increment IDs (1-based, fresh DB).
  await queries.createBook(db, 1, 'The Left Hand of Darkness', 'sci-fi', 12.99, null);
  await queries.createBook(db, 1, 'The Dispossessed',           'sci-fi', 11.50, null);
  await queries.createBook(db, 2, 'Dune',                       'sci-fi', 14.99, null);
  await queries.createBook(db, 3, 'Foundation',                 'sci-fi', 10.99, null);
  await queries.createBook(db, 3, 'The Caves of Steel',         'sci-fi', 9.99,  null);
  console.log('[sqlite] inserted 5 books');

  await queries.createCustomer(db, 'Carol', 'carol@example.com');
  await queries.createCustomer(db, 'Dave',  'dave@example.com');
  console.log('[sqlite] inserted 2 customers');

  await queries.createSale(db, 1);
  await queries.addSaleItem(db, 1, 3, 2, 14.99);
  await queries.addSaleItem(db, 1, 4, 1, 10.99);
  await queries.createSale(db, 2);
  await queries.addSaleItem(db, 2, 3, 1, 14.99);
  await queries.addSaleItem(db, 2, 1, 1, 12.99);
  console.log('[sqlite] inserted 2 sales with items');
}

async function query(db: Database.Database): Promise<void> {
  const authors = await queries.listAuthors(db);
  console.log(`[sqlite] listAuthors: ${authors.length} row(s)`);

  // Books inserted in seed have IDs 1–5; 1=Left Hand, 3=Dune.
  const byIds = await queries.getBooksByIds(db, [1, 3]);
  console.log(`[sqlite] getBooksByIds([1,3]): ${byIds.length} row(s)`);
  for (const b of byIds) console.log(`  "${b.title}"`);

  const scifi = await queries.listBooksByGenre(db, 'sci-fi');
  console.log(`[sqlite] listBooksByGenre(sci-fi): ${scifi.length} row(s)`);

  const allBooks = await queries.listBooksByGenreOrAll(db, 'all');
  console.log(`[sqlite] listBooksByGenreOrAll(all): ${allBooks.length} row(s) (repeated-param demo)`);
  const scifi2 = await queries.listBooksByGenreOrAll(db, 'sci-fi');
  console.log(`[sqlite] listBooksByGenreOrAll(sci-fi): ${scifi2.length} row(s)`);

  console.log('[sqlite] listBooksWithAuthor:');
  for (const r of await queries.listBooksWithAuthor(db)) {
    console.log(`  "${r.title}" by ${r.author_name}`);
  }

  const neverOrdered = await queries.getBooksNeverOrdered(db);
  console.log(`[sqlite] getBooksNeverOrdered: ${neverOrdered.length} book(s)`);
  for (const b of neverOrdered) console.log(`  "${b.title}"`);

  console.log('[sqlite] getTopSellingBooks:');
  for (const r of await queries.getTopSellingBooks(db)) {
    console.log(`  "${r.title}" sold ${r.units_sold}`);
  }

  console.log('[sqlite] getBestCustomers:');
  for (const r of await queries.getBestCustomers(db)) {
    console.log(`  ${r.name} spent ${r.total_spent}`);
  }
}

async function main(): Promise<void> {
  const db = new Database(':memory:');
  applyMigrations(db);
  await seed(db);
  await query(db);
  db.close();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
