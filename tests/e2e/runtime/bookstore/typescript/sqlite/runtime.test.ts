/**
 * End-to-end runtime tests for the generated TypeScript/SQLite queries.
 *
 * Uses an in-memory better-sqlite3 database — no external services required.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import Database from 'better-sqlite3';

import * as queries from './gen/queries';

const FIXTURES = join(__dirname, '../../../../fixtures/bookstore/sqlite');

// ─── Setup helpers ────────────────────────────────────────────────────────────

function makeDb(): Database.Database {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  db.exec(readFileSync(join(FIXTURES, 'schema.sql'), 'utf8'));
  return db;
}

async function seed(db: Database.Database): Promise<void> {
  await queries.createAuthor(db, 'Asimov', 'Sci-fi master', 1920);
  await queries.createAuthor(db, 'Herbert', null, 1920);
  await queries.createAuthor(db, 'Le Guin', 'Earthsea', 1929);

  await queries.createBook(db, 1, 'Foundation', 'sci-fi', 9.99, '1951-01-01');
  await queries.createBook(db, 1, 'I Robot', 'sci-fi', 7.99, '1950-01-01');
  await queries.createBook(db, 2, 'Dune', 'sci-fi', 12.99, '1965-01-01');
  await queries.createBook(db, 3, 'Earthsea', 'fantasy', 8.99, '1968-01-01');

  await queries.createCustomer(db, 'Alice', 'alice@example.com');
  await queries.createSale(db, 1);
  await queries.addSaleItem(db, 1, 1, 2, 9.99);   // Foundation qty 2
  await queries.addSaleItem(db, 1, 3, 1, 12.99);  // Dune qty 1
}

// ─── :one tests ───────────────────────────────────────────────────────────────

describe(':one queries', () => {
  it('getAuthor returns the correct author', async () => {
    const db = makeDb();
    await seed(db);
    const author = await queries.getAuthor(db, 1);
    assert.ok(author);
    assert.equal(author.name, 'Asimov');
    assert.equal(author.bio, 'Sci-fi master');
    assert.equal(author.birth_year, 1920);
  });

  it('getAuthor returns null for unknown id', async () => {
    const db = makeDb();
    const author = await queries.getAuthor(db, 999);
    assert.equal(author, null);
  });

  it('getBook returns the correct book', async () => {
    const db = makeDb();
    await seed(db);
    const book = await queries.getBook(db, 1);
    assert.ok(book);
    assert.equal(book.title, 'Foundation');
    assert.equal(book.genre, 'sci-fi');
    assert.equal(book.author_id, 1);
  });
});

// ─── :many tests ──────────────────────────────────────────────────────────────

describe(':many queries', () => {
  it('listAuthors returns all authors sorted by name', async () => {
    const db = makeDb();
    await seed(db);
    const authors = await queries.listAuthors(db);
    assert.equal(authors.length, 3);
    assert.equal(authors[0].name, 'Asimov');
    assert.equal(authors[1].name, 'Herbert');
    assert.equal(authors[2].name, 'Le Guin');
  });

  it('listBooksByGenre filters correctly', async () => {
    const db = makeDb();
    await seed(db);
    const sciFi = await queries.listBooksByGenre(db, 'sci-fi');
    assert.equal(sciFi.length, 3);
    const fantasy = await queries.listBooksByGenre(db, 'fantasy');
    assert.equal(fantasy.length, 1);
    assert.equal(fantasy[0].title, 'Earthsea');
  });

  it('listBooksByGenreOrAll returns all when given "all"', async () => {
    const db = makeDb();
    await seed(db);
    const all = await queries.listBooksByGenreOrAll(db, 'all');
    assert.equal(all.length, 4);
    const sciFi = await queries.listBooksByGenreOrAll(db, 'sci-fi');
    assert.equal(sciFi.length, 3);
  });
});

// ─── CreateBook tests ─────────────────────────────────────────────────────────

describe('createBook queries', () => {
  it('createBook inserts a row', async () => {
    const db = makeDb();
    await seed(db);
    await queries.createBook(db, 1, 'New Book', 'mystery', 14.50, null);
    const book = await queries.getBook(db, 5);
    assert.ok(book);
    assert.equal(book.title, 'New Book');
    assert.equal(book.genre, 'mystery');
  });
});

// ─── CreateCustomer / CreateSale / AddSaleItem tests ──────────────────────────

describe('createCustomer / createSale / addSaleItem queries', () => {
  it('createCustomer inserts a row', async () => {
    const db = makeDb();
    await queries.createCustomer(db, 'Bob', 'bob@example.com');
    const count = db.prepare('SELECT COUNT(*) as c FROM customer WHERE name = ?').get('Bob') as { c: number };
    assert.equal(count.c, 1);
  });

  it('createSale inserts a sale row', async () => {
    const db = makeDb();
    await seed(db);
    await queries.createSale(db, 1);
    const count = db.prepare('SELECT COUNT(*) as c FROM sale WHERE customer_id = ?').get(1) as { c: number };
    assert.equal(count.c, 2);
  });

  it('addSaleItem inserts without error', async () => {
    const db = makeDb();
    await seed(db);
    await queries.addSaleItem(db, 1, 4, 1, 8.99);
    const count = db.prepare('SELECT COUNT(*) as c FROM sale_item WHERE sale_id = ?').get(1) as { c: number };
    assert.equal(count.c, 3);
  });
});

// ─── CASE / COALESCE tests ────────────────────────────────────────────────────

describe('CASE / COALESCE queries', () => {
  it('getBookPriceLabel returns price label for each book', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.getBookPriceLabel(db, 10);
    assert.equal(rows.length, 4);
    const dune = rows.find(r => r.title === 'Dune');
    assert.ok(dune);
    assert.equal(dune.price_label, 'expensive');
    const earthsea = rows.find(r => r.title === 'Earthsea');
    assert.ok(earthsea);
    assert.equal(earthsea.price_label, 'affordable');
  });

  it('getBookPriceOrDefault returns effective price', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.getBookPriceOrDefault(db, 0);
    assert.equal(rows.length, 4);
    assert.ok(rows.every(r => (r.effective_price as number) > 0));
  });
});

// ─── Product type coverage ────────────────────────────────────────────────────

describe('product queries', () => {
  it('getProduct returns the inserted product', async () => {
    const db = makeDb();
    db.prepare('INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, ?, ?)')
      .run('prod-001', 'SKU-001', 'Widget', 1, 5);
    const row = await queries.getProduct(db, 'prod-001');
    assert.ok(row);
    assert.equal(row.id, 'prod-001');
    assert.equal(row.name, 'Widget');
    assert.equal(row.stock_count, 5);
  });

  it('listActiveProducts filters by active flag', async () => {
    const db = makeDb();
    db.prepare('INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, ?, ?)')
      .run('act-1', 'ACT-1', 'Active', 1, 10);
    db.prepare('INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, ?, ?)')
      .run('inact-1', 'INACT-1', 'Inactive', 0, 0);
    const active = await queries.listActiveProducts(db, 1);
    assert.equal(active.length, 1);
    assert.equal(active[0].name, 'Active');
    const inactive = await queries.listActiveProducts(db, 0);
    assert.equal(inactive.length, 1);
    assert.equal(inactive[0].name, 'Inactive');
  });
});

// ─── UpdateAuthorBio / DeleteAuthor tests (new fixture queries) ────────────────

describe('updateAuthorBio / deleteAuthor queries', () => {
  it('updateAuthorBio updates the row', async () => {
    const db = makeDb();
    await seed(db);
    await queries.updateAuthorBio(db, 'Updated bio', 1);
    const author = await queries.getAuthor(db, 1);
    assert.ok(author);
    assert.equal(author.bio, 'Updated bio');
  });

  it('deleteAuthor removes the row', async () => {
    const db = makeDb();
    await queries.createAuthor(db, 'Temp', null, null);
    await queries.deleteAuthor(db, 1);
    assert.equal(await queries.getAuthor(db, 1), null);
  });
});

// ─── InsertProduct / UpsertProduct tests (new fixture queries) ────────────────

describe('insertProduct / upsertProduct queries', () => {
  it('insertProduct inserts a product row', async () => {
    const db = makeDb();
    const pid = 'test-insert-product-1';
    await queries.insertProduct(db, pid, 'SKU-NEW', 'Gadget', 1, null, null, null, null, 7);
    const row = await queries.getProduct(db, pid);
    assert.ok(row);
    assert.equal(row.name, 'Gadget');
    assert.equal(row.stock_count, 7);
  });

  it('upsertProduct inserts then updates on conflict', async () => {
    const db = makeDb();
    const pid = 'test-upsert-product-1';
    await queries.upsertProduct(db, pid, 'SKU-UP', 'Thing', 1, null, 10);
    const row = await queries.getProduct(db, pid);
    assert.ok(row);
    assert.equal(row.name, 'Thing');
    assert.equal(row.stock_count, 10);

    await queries.upsertProduct(db, pid, 'SKU-UP', 'Thing Pro', 1, null, 20);
    const updated = await queries.getProduct(db, pid);
    assert.ok(updated);
    assert.equal(updated.name, 'Thing Pro');
    assert.equal(updated.stock_count, 20);
  });
});

// ─── :exec tests ──────────────────────────────────────────────────────────────

describe(':exec queries', () => {
  it('createAuthor inserts a row', async () => {
    const db = makeDb();
    await queries.createAuthor(db, 'Test', null, null);
    const author = await queries.getAuthor(db, 1);
    assert.ok(author);
    assert.equal(author.name, 'Test');
    assert.equal(author.bio, null);
    assert.equal(author.birth_year, null);
  });
});

// ─── :execrows tests ──────────────────────────────────────────────────────────

describe(':execrows queries', () => {
  it('deleteBookById returns affected row count', async () => {
    const db = makeDb();
    await seed(db);
    // Book 2 (I Robot) has no sale_items so can be deleted
    const affected = await queries.deleteBookById(db, 2);
    assert.equal(affected, 1);
    const notFound = await queries.deleteBookById(db, 999);
    assert.equal(notFound, 0);
  });
});

// ─── JOIN tests ───────────────────────────────────────────────────────────────

describe('JOIN queries', () => {
  it('listBooksWithAuthor returns joined rows', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.listBooksWithAuthor(db);
    assert.equal(rows.length, 4);

    const dune = rows.find(r => r.title === 'Dune');
    assert.ok(dune);
    assert.equal(dune.author_name, 'Herbert');
    assert.equal(dune.author_bio, null);

    const foundation = rows.find(r => r.title === 'Foundation');
    assert.ok(foundation);
    assert.equal(foundation.author_name, 'Asimov');
    assert.equal(foundation.author_bio, 'Sci-fi master');
  });

  it('getBooksNeverOrdered returns books with no sale_items', async () => {
    const db = makeDb();
    await seed(db);
    const books = await queries.getBooksNeverOrdered(db);
    assert.equal(books.length, 2);
    const titles = new Set(books.map(b => b.title));
    assert.ok(titles.has('I Robot'));
    assert.ok(titles.has('Earthsea'));
  });
});

// ─── CTE tests ────────────────────────────────────────────────────────────────

describe('CTE queries', () => {
  it('getTopSellingBooks ranks Foundation first', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.getTopSellingBooks(db);
    assert.ok(rows.length > 0);
    assert.equal(rows[0].title, 'Foundation');
  });

  it('getBestCustomers returns Alice', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.getBestCustomers(db);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, 'Alice');
  });

  it('getAuthorStats returns one row per author', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.getAuthorStats(db);
    assert.equal(rows.length, 3);
    const asimov = rows.find(r => r.name === 'Asimov');
    assert.ok(asimov);
    assert.equal(asimov.num_books, 2);
  });
});

// ─── Aggregate tests ──────────────────────────────────────────────────────────

describe('aggregate queries', () => {
  it('countBooksByGenre returns correct counts', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.countBooksByGenre(db);
    assert.equal(rows.length, 2);
    const fantasy = rows.find(r => r.genre === 'fantasy');
    assert.ok(fantasy);
    assert.equal(fantasy.book_count, 1);
    const sciFi = rows.find(r => r.genre === 'sci-fi');
    assert.ok(sciFi);
    assert.equal(sciFi.book_count, 3);
  });
});

// ─── LIMIT/OFFSET tests ───────────────────────────────────────────────────────

describe('LIMIT/OFFSET queries', () => {
  it('listBooksWithLimit paginates correctly', async () => {
    const db = makeDb();
    await seed(db);
    const page1 = await queries.listBooksWithLimit(db, 2, 0);
    assert.equal(page1.length, 2);
    const page2 = await queries.listBooksWithLimit(db, 2, 2);
    assert.equal(page2.length, 2);
    const titles1 = new Set(page1.map(r => r.title));
    const titles2 = new Set(page2.map(r => r.title));
    for (const t of titles1) assert.ok(!titles2.has(t));
  });
});

// ─── LIKE tests ───────────────────────────────────────────────────────────────

describe('LIKE queries', () => {
  it('searchBooksByTitle filters correctly', async () => {
    const db = makeDb();
    await seed(db);
    const results = await queries.searchBooksByTitle(db, '%ound%');
    assert.equal(results.length, 1);
    assert.equal(results[0].title, 'Foundation');
    const none = await queries.searchBooksByTitle(db, 'NOPE%');
    assert.equal(none.length, 0);
  });
});

// ─── BETWEEN tests ────────────────────────────────────────────────────────────

describe('BETWEEN queries', () => {
  it('getBooksByPriceRange returns books in range', async () => {
    const db = makeDb();
    await seed(db);
    // Foundation (9.99) and Earthsea (8.99) are in [8, 10]
    const results = await queries.getBooksByPriceRange(db, 8, 10);
    assert.equal(results.length, 2);
  });
});

// ─── IN list tests ────────────────────────────────────────────────────────────

describe('IN list queries', () => {
  it('getBooksInGenres returns matching books', async () => {
    const db = makeDb();
    await seed(db);
    const results = await queries.getBooksInGenres(db, 'sci-fi', 'fantasy', 'horror');
    assert.equal(results.length, 4);
  });
});

// ─── HAVING tests ─────────────────────────────────────────────────────────────

describe('HAVING queries', () => {
  it('getGenresWithManyBooks filters by count', async () => {
    const db = makeDb();
    await seed(db);
    const results = await queries.getGenresWithManyBooks(db, 1);
    assert.equal(results.length, 1);
    assert.equal(results[0].genre, 'sci-fi');
    assert.equal(results[0].book_count, 3);
  });
});

// ─── Subquery tests ───────────────────────────────────────────────────────────

describe('subquery queries', () => {
  it('getBooksNotByAuthor excludes the named author', async () => {
    const db = makeDb();
    await seed(db);
    const results = await queries.getBooksNotByAuthor(db, 'Asimov');
    assert.equal(results.length, 2);
    const titles = results.map(r => r.title);
    assert.ok(!titles.includes('Foundation'));
    assert.ok(!titles.includes('I Robot'));
  });

  it('getBooksWithRecentSales returns books with recent sale_items', async () => {
    const db = makeDb();
    await seed(db);
    const results = await queries.getBooksWithRecentSales(db, '2000-01-01');
    assert.equal(results.length, 2);
  });
});

// ─── Scalar subquery test ─────────────────────────────────────────────────────

describe('scalar subquery queries', () => {
  it('getBookWithAuthorName joins author name via subquery', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.getBookWithAuthorName(db);
    assert.equal(rows.length, 4);
    const dune = rows.find(r => r.title === 'Dune');
    assert.ok(dune);
    assert.equal(dune.author_name, 'Herbert');
  });
});

// ─── JOIN with param tests ────────────────────────────────────────────────────

describe('JOIN with param queries', () => {
  it('getBooksByAuthorParam filters by birth_year', async () => {
    const db = makeDb();
    await seed(db);
    // birth_year > 1925 → only Le Guin (1929)
    const results = await queries.getBooksByAuthorParam(db, 1925);
    assert.equal(results.length, 1);
    assert.equal(results[0].title, 'Earthsea');
  });
});

// ─── Qualified wildcard tests ─────────────────────────────────────────────────

describe('wildcard queries', () => {
  it('getAllBookFields returns all book columns', async () => {
    const db = makeDb();
    await seed(db);
    const books = await queries.getAllBookFields(db);
    assert.equal(books.length, 4);
    assert.equal(books[0].id, 1);
    assert.equal(books[0].title, 'Foundation');
  });
});

// ─── List param tests ─────────────────────────────────────────────────────────

describe('list param queries', () => {
  it('getBooksByIds returns the requested books', async () => {
    const db = makeDb();
    await seed(db);
    const books = await queries.getBooksByIds(db, [1, 3]);
    assert.equal(books.length, 2);
    const titles = new Set(books.map(b => b.title));
    assert.ok(titles.has('Foundation'));
    assert.ok(titles.has('Dune'));
  });
});

// ─── IS NULL / IS NOT NULL tests ──────────────────────────────────────────────

describe('NULL tests', () => {
  it('getAuthorsWithNullBio returns authors without bio', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.getAuthorsWithNullBio(db);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, 'Herbert');
  });

  it('getAuthorsWithBio returns authors with bio', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.getAuthorsWithBio(db);
    assert.equal(rows.length, 2);
    const names = new Set(rows.map(r => r.name));
    assert.ok(names.has('Asimov'));
    assert.ok(names.has('Le Guin'));
  });
});

// ─── Date range tests ─────────────────────────────────────────────────────────

describe('date range queries', () => {
  it('getBooksPublishedBetween filters by date range', async () => {
    const db = makeDb();
    await seed(db);
    // 1951 to 1966 → Foundation (1951) and Dune (1965)
    const rows = await queries.getBooksPublishedBetween(db, '1951-01-01', '1966-01-01');
    assert.equal(rows.length, 2);
    const titles = new Set(rows.map(r => r.title));
    assert.ok(titles.has('Foundation'));
    assert.ok(titles.has('Dune'));
  });
});

// ─── DISTINCT tests ───────────────────────────────────────────────────────────

describe('DISTINCT queries', () => {
  it('getDistinctGenres returns unique genres', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.getDistinctGenres(db);
    assert.equal(rows.length, 2);
    const genres = new Set(rows.map(r => r.genre));
    assert.ok(genres.has('sci-fi'));
    assert.ok(genres.has('fantasy'));
  });
});

// ─── LEFT JOIN aggregate tests ────────────────────────────────────────────────

describe('LEFT JOIN aggregate queries', () => {
  it('getBooksWithSalesCount returns total quantity per book', async () => {
    const db = makeDb();
    await seed(db);
    const rows = await queries.getBooksWithSalesCount(db);
    assert.equal(rows.length, 4);

    const foundation = rows.find(r => r.title === 'Foundation');
    assert.ok(foundation);
    assert.equal(foundation.total_quantity, 2);

    const dune = rows.find(r => r.title === 'Dune');
    assert.ok(dune);
    assert.equal(dune.total_quantity, 1);

    const earthsea = rows.find(r => r.title === 'Earthsea');
    assert.ok(earthsea);
    assert.equal(earthsea.total_quantity, 0);
  });
});

// ─── Scalar aggregate tests ───────────────────────────────────────────────────

describe('scalar aggregate queries', () => {
  it('countSaleItems returns item count for a sale', async () => {
    const db = makeDb();
    await seed(db);
    const row = await queries.countSaleItems(db, 1);
    assert.ok(row);
    assert.equal(row.item_count, 2);
  });

  it('countSaleItems returns 0 for non-existent sale', async () => {
    const db = makeDb();
    await seed(db);
    const row = await queries.countSaleItems(db, 999);
    assert.ok(row);
    assert.equal(row.item_count, 0);
  });
});

// ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────────────

describe('MIN/MAX/SUM/AVG aggregate queries', () => {
  it('getSaleItemQuantityAggregates returns correct aggregates', async () => {
    const db = makeDb();
    await seed(db);
    // Seed: Foundation qty 2 + Dune qty 1 → min=1, max=2, sum=3, avg=1.5
    const row = await queries.getSaleItemQuantityAggregates(db);
    assert.ok(row !== null);
    assert.equal(row!.min_qty, 1);
    assert.equal(row!.max_qty, 2);
    assert.equal(row!.sum_qty, 3);
    assert.ok(Math.abs((row!.avg_qty as number) - 1.5) < 0.01);
  });

  it('getBookPriceAggregates returns correct aggregates', async () => {
    const db = makeDb();
    await seed(db);
    // Book prices: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96, avg=9.99
    const row = await queries.getBookPriceAggregates(db);
    assert.ok(row !== null);
    assert.ok(Math.abs((row!.min_price as number) - 7.99) < 0.01);
    assert.ok(Math.abs((row!.max_price as number) - 12.99) < 0.01);
    assert.ok(Math.abs((row!.sum_price as number) - 39.96) < 0.01);
    assert.ok(Math.abs((row!.avg_price as number) - 9.99) < 0.01);
  });
});
