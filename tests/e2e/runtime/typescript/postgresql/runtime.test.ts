/**
 * End-to-end runtime tests for the generated TypeScript/PostgreSQL queries.
 *
 * Each test runs in its own PostgreSQL schema for isolation.
 * Requires the docker-compose postgres service on port 15432.
 */
import { describe, it, before, after, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { Client } from 'pg';
import { randomBytes } from 'node:crypto';

import * as queries from './gen/queries';

const FIXTURES = join(__dirname, '../../../fixtures/postgresql');
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

async function seed(client: Client): Promise<void> {
  const a1 = await queries.createAuthor(client, 'Asimov', 'Sci-fi master', 1920);
  const a2 = await queries.createAuthor(client, 'Herbert', null, 1920);
  const a3 = await queries.createAuthor(client, 'Le Guin', 'Earthsea', 1929);
  assert.ok(a1 && a2 && a3);

  const b1 = await queries.createBook(client, a1.id, 'Foundation', 'sci-fi', 9.99, '1951-01-01');
  const b2 = await queries.createBook(client, a1.id, 'I Robot', 'sci-fi', 7.99, '1950-01-01');
  const b3 = await queries.createBook(client, a2.id, 'Dune', 'sci-fi', 12.99, '1965-01-01');
  const b4 = await queries.createBook(client, a3.id, 'Earthsea', 'fantasy', 8.99, '1968-01-01');
  assert.ok(b1 && b2 && b3 && b4);

  const alice = await queries.createCustomer(client, 'Alice', 'alice@example.com');
  const bob = await queries.createCustomer(client, 'Bob', 'bob@example.com');
  assert.ok(alice && bob);

  const sale1 = await queries.createSale(client, alice.id);
  assert.ok(sale1);
  await queries.addSaleItem(client, sale1.id, b1.id, 2, 9.99);
  await queries.addSaleItem(client, sale1.id, b3.id, 1, 12.99);

  const sale2 = await queries.createSale(client, bob.id);
  assert.ok(sale2);
  await queries.addSaleItem(client, sale2.id, b4.id, 1, 8.99);
}

// ─── :one tests ───────────────────────────────────────────────────────────────

describe(':one queries', () => {
  it('getAuthor returns the correct author', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const author = await queries.getAuthor(client, 1);
      assert.ok(author);
      assert.equal(author.name, 'Asimov');
      assert.equal(author.bio, 'Sci-fi master');
      assert.equal(author.birth_year, 1920);
    } finally { await teardown(client, schema); }
  });

  it('getAuthor returns null for unknown id', async () => {
    const { client, schema } = await makeClient();
    try {
      assert.equal(await queries.getAuthor(client, 999), null);
    } finally { await teardown(client, schema); }
  });

  it('getBook returns the correct book', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const book = await queries.getBook(client, 1);
      assert.ok(book);
      assert.equal(book.title, 'Foundation');
      assert.equal(book.genre, 'sci-fi');
    } finally { await teardown(client, schema); }
  });
});

// ─── :many tests ──────────────────────────────────────────────────────────────

describe(':many queries', () => {
  it('listAuthors returns all authors sorted by name', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const authors = await queries.listAuthors(client);
      assert.equal(authors.length, 3);
      assert.equal(authors[0].name, 'Asimov');
      assert.equal(authors[1].name, 'Herbert');
      assert.equal(authors[2].name, 'Le Guin');
    } finally { await teardown(client, schema); }
  });

  it('listBooksByGenre filters correctly', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      assert.equal((await queries.listBooksByGenre(client, 'sci-fi')).length, 3);
      const fantasy = await queries.listBooksByGenre(client, 'fantasy');
      assert.equal(fantasy.length, 1);
      assert.equal(fantasy[0].title, 'Earthsea');
    } finally { await teardown(client, schema); }
  });

  it('listBooksByGenreOrAll returns all when given "all"', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      assert.equal((await queries.listBooksByGenreOrAll(client, 'all')).length, 4);
      assert.equal((await queries.listBooksByGenreOrAll(client, 'sci-fi')).length, 3);
    } finally { await teardown(client, schema); }
  });
});

// ─── UpdateAuthorBio / DeleteAuthor tests ─────────────────────────────────────

describe('updateAuthorBio / deleteAuthor queries', () => {
  it('updateAuthorBio updates and returns the row', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const updated = await queries.updateAuthorBio(client, 'Updated bio', 1);
      assert.ok(updated);
      assert.equal(updated.name, 'Asimov');
      assert.equal(updated.bio, 'Updated bio');
    } finally { await teardown(client, schema); }
  });

  it('deleteAuthor removes the row and returns it', async () => {
    const { client, schema } = await makeClient();
    try {
      const author = await queries.createAuthor(client, 'Temp', null, null);
      assert.ok(author);
      const deleted = await queries.deleteAuthor(client, author.id);
      assert.ok(deleted);
      assert.equal(deleted.name, 'Temp');
      assert.equal(await queries.getAuthor(client, author.id), null);
    } finally { await teardown(client, schema); }
  });
});

// ─── CreateBook / AddSaleItem tests ───────────────────────────────────────────

describe('createBook / addSaleItem queries', () => {
  it('createBook inserts and returns the row', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const book = await queries.createBook(client, 1, 'New Book', 'mystery', 14.50, null);
      assert.ok(book);
      assert.equal(book.title, 'New Book');
      assert.equal(book.genre, 'mystery');
      assert.equal(book.published_at, null);
    } finally { await teardown(client, schema); }
  });

  it('addSaleItem inserts without error', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      // Add Earthsea (book 4) to sale 1
      await queries.addSaleItem(client, 1, 4, 1, 8.99);
      const { rows } = await client.query('SELECT COUNT(*) FROM sale_item WHERE sale_id = 1');
      assert.equal(Number(rows[0].count), 3);
    } finally { await teardown(client, schema); }
  });
});

// ─── CASE / COALESCE tests ────────────────────────────────────────────────────

describe('CASE / COALESCE queries', () => {
  it('getBookPriceLabel returns price label for each book', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.getBookPriceLabel(client, 10);
      assert.equal(rows.length, 4);
      const dune = rows.find(r => r.title === 'Dune');
      assert.ok(dune);
      assert.equal(dune.price_label, 'expensive');
      const earthsea = rows.find(r => r.title === 'Earthsea');
      assert.ok(earthsea);
      assert.equal(earthsea.price_label, 'affordable');
    } finally { await teardown(client, schema); }
  });

  it('getBookPriceOrDefault returns effective price', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.getBookPriceOrDefault(client, 0);
      assert.equal(rows.length, 4);
      assert.ok(rows.every(r => Number(r.effective_price) > 0));
    } finally { await teardown(client, schema); }
  });
});

// ─── Product type coverage ────────────────────────────────────────────────────

describe('product queries', () => {
  it('getProduct returns the inserted product', async () => {
    const { client, schema } = await makeClient();
    try {
      const productId = '00000000-0000-0000-0000-000000000002';
      await queries.insertProduct(client, productId, 'SKU-002', 'Widget', true,
        1.5, 4.7, ['tag1'], null, null, 5);
      const row = await queries.getProduct(client, productId);
      assert.ok(row);
      assert.equal(row.id, productId);
      assert.equal(row.name, 'Widget');
      assert.equal(row.stock_count, 5);
    } finally { await teardown(client, schema); }
  });

  it('listActiveProducts filters by active flag', async () => {
    const { client, schema } = await makeClient();
    try {
      await queries.insertProduct(client, '00000000-0000-0000-0000-000000000010',
        'ACT-1', 'Active', true, null, null, [], null, null, 10);
      await queries.insertProduct(client, '00000000-0000-0000-0000-000000000011',
        'INACT-1', 'Inactive', false, null, null, [], null, null, 0);
      const active = await queries.listActiveProducts(client, true);
      assert.equal(active.length, 1);
      assert.equal(active[0].name, 'Active');
      const inactive = await queries.listActiveProducts(client, false);
      assert.equal(inactive.length, 1);
      assert.equal(inactive[0].name, 'Inactive');
    } finally { await teardown(client, schema); }
  });

  it('insertProduct inserts and returns full row', async () => {
    const { client, schema } = await makeClient();
    try {
      const productId = '00000000-0000-0000-0000-000000000003';
      const product = await queries.insertProduct(client, productId, 'SKU-003', 'Gadget', true,
        null, null, ['electronics'], null, null, 20);
      assert.ok(product);
      assert.equal(product.id, productId);
      assert.equal(product.name, 'Gadget');
      assert.equal(product.stock_count, 20);
    } finally { await teardown(client, schema); }
  });
});

// ─── :exec tests ──────────────────────────────────────────────────────────────

describe(':exec queries', () => {
  it('createAuthor inserts and returns a row', async () => {
    const { client, schema } = await makeClient();
    try {
      const author = await queries.createAuthor(client, 'Test', null, null);
      assert.ok(author);
      assert.equal(author.name, 'Test');
      assert.equal(author.bio, null);
      assert.equal(author.birth_year, null);
    } finally { await teardown(client, schema); }
  });
});

// ─── CreateCustomer / CreateSale tests ───────────────────────────────────────

describe('createCustomer / createSale queries', () => {
  it('createCustomer inserts and returns the row', async () => {
    const { client, schema } = await makeClient();
    try {
      const cust = await queries.createCustomer(client, 'Solo', 'solo@example.com');
      assert.ok(cust);
      assert.ok(cust.id > 0);
    } finally { await teardown(client, schema); }
  });

  it('createSale inserts and returns the row', async () => {
    const { client, schema } = await makeClient();
    try {
      const cust = await queries.createCustomer(client, 'Solo', 'solo@example.com');
      assert.ok(cust);
      const sale = await queries.createSale(client, cust.id);
      assert.ok(sale);
      assert.ok(sale.id > 0);
    } finally { await teardown(client, schema); }
  });
});

// ─── :execrows tests ──────────────────────────────────────────────────────────

describe(':execrows queries', () => {
  it('deleteBookById returns affected row count', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      assert.equal(await queries.deleteBookById(client, 2), 1);
      assert.equal(await queries.deleteBookById(client, 999), 0);
    } finally { await teardown(client, schema); }
  });
});

// ─── JOIN tests ───────────────────────────────────────────────────────────────

describe('JOIN queries', () => {
  it('listBooksWithAuthor returns joined rows', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.listBooksWithAuthor(client);
      assert.equal(rows.length, 4);
      const dune = rows.find(r => r.title === 'Dune');
      assert.ok(dune);
      assert.equal(dune.author_name, 'Herbert');
      assert.equal(dune.author_bio, null);
      const foundation = rows.find(r => r.title === 'Foundation');
      assert.ok(foundation);
      assert.equal(foundation.author_bio, 'Sci-fi master');
    } finally { await teardown(client, schema); }
  });

  it('getBooksNeverOrdered returns books with no sale_items', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const books = await queries.getBooksNeverOrdered(client);
      assert.equal(books.length, 1);
      assert.equal(books[0].title, 'I Robot');
    } finally { await teardown(client, schema); }
  });
});

// ─── CTE tests ────────────────────────────────────────────────────────────────

describe('CTE queries', () => {
  it('getTopSellingBooks ranks Foundation first', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.getTopSellingBooks(client);
      assert.ok(rows.length > 0);
      assert.equal(rows[0].title, 'Foundation');
    } finally { await teardown(client, schema); }
  });

  it('getBestCustomers returns Alice first', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.getBestCustomers(client);
      assert.equal(rows[0].name, 'Alice');
    } finally { await teardown(client, schema); }
  });

  it('getAuthorStats returns one row per author', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.getAuthorStats(client);
      assert.equal(rows.length, 3);
      const asimov = rows.find(r => r.name === 'Asimov');
      assert.ok(asimov);
      assert.equal(Number(asimov.num_books), 2);
    } finally { await teardown(client, schema); }
  });
});

// ─── Aggregate tests ──────────────────────────────────────────────────────────

describe('aggregate queries', () => {
  it('countBooksByGenre returns correct counts', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.countBooksByGenre(client);
      assert.equal(rows.length, 2);
      const fantasy = rows.find(r => r.genre === 'fantasy');
      assert.ok(fantasy);
      assert.equal(Number(fantasy.book_count), 1);
    } finally { await teardown(client, schema); }
  });
});

// ─── LIMIT/OFFSET tests ───────────────────────────────────────────────────────

describe('LIMIT/OFFSET queries', () => {
  it('listBooksWithLimit paginates without overlap', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const page1 = await queries.listBooksWithLimit(client, 2, 0);
      const page2 = await queries.listBooksWithLimit(client, 2, 2);
      assert.equal(page1.length, 2);
      assert.equal(page2.length, 2);
      const t1 = new Set(page1.map(r => r.title));
      for (const r of page2) assert.ok(!t1.has(r.title));
    } finally { await teardown(client, schema); }
  });
});

// ─── LIKE tests ───────────────────────────────────────────────────────────────

describe('LIKE queries', () => {
  it('searchBooksByTitle filters by pattern', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const results = await queries.searchBooksByTitle(client, '%ound%');
      assert.equal(results.length, 1);
      assert.equal(results[0].title, 'Foundation');
      assert.equal((await queries.searchBooksByTitle(client, 'NOPE%')).length, 0);
    } finally { await teardown(client, schema); }
  });
});

// ─── BETWEEN tests ────────────────────────────────────────────────────────────

describe('BETWEEN queries', () => {
  it('getBooksByPriceRange returns books in range', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const results = await queries.getBooksByPriceRange(client, 8, 10);
      assert.equal(results.length, 2);
    } finally { await teardown(client, schema); }
  });
});

// ─── IN list tests ────────────────────────────────────────────────────────────

describe('IN list queries', () => {
  it('getBooksInGenres matches all genres', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const results = await queries.getBooksInGenres(client, 'sci-fi', 'fantasy', 'horror');
      assert.equal(results.length, 4);
    } finally { await teardown(client, schema); }
  });
});

// ─── HAVING tests ─────────────────────────────────────────────────────────────

describe('HAVING queries', () => {
  it('getGenresWithManyBooks filters by count threshold', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const results = await queries.getGenresWithManyBooks(client, 1);
      assert.equal(results.length, 1);
      assert.equal(results[0].genre, 'sci-fi');
      assert.equal(Number(results[0].book_count), 3);
    } finally { await teardown(client, schema); }
  });
});

// ─── Subquery tests ───────────────────────────────────────────────────────────

describe('subquery queries', () => {
  it('getBooksNotByAuthor excludes the named author', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const results = await queries.getBooksNotByAuthor(client, 'Asimov');
      assert.equal(results.length, 2);
      assert.ok(!results.some(r => r.title === 'Foundation'));
      assert.ok(!results.some(r => r.title === 'I Robot'));
    } finally { await teardown(client, schema); }
  });

  it('getBooksWithRecentSales returns books sold after date', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const results = await queries.getBooksWithRecentSales(client, '2000-01-01');
      assert.equal(results.length, 3);
    } finally { await teardown(client, schema); }
  });
});

// ─── Scalar subquery test ─────────────────────────────────────────────────────

describe('scalar subquery queries', () => {
  it('getBookWithAuthorName resolves author via subquery', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.getBookWithAuthorName(client);
      assert.equal(rows.length, 4);
      const dune = rows.find(r => r.title === 'Dune');
      assert.ok(dune);
      assert.equal(dune.author_name, 'Herbert');
    } finally { await teardown(client, schema); }
  });
});

// ─── JOIN with param tests ────────────────────────────────────────────────────

describe('JOIN with param queries', () => {
  it('getBooksByAuthorParam filters by birth_year', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const results = await queries.getBooksByAuthorParam(client, 1925);
      assert.equal(results.length, 1);
      assert.equal(results[0].title, 'Earthsea');
    } finally { await teardown(client, schema); }
  });
});

// ─── Qualified wildcard tests ─────────────────────────────────────────────────

describe('wildcard queries', () => {
  it('getAllBookFields returns all books with full columns', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const books = await queries.getAllBookFields(client);
      assert.equal(books.length, 4);
      assert.equal(books[0].title, 'Foundation');
    } finally { await teardown(client, schema); }
  });
});

// ─── List param tests ─────────────────────────────────────────────────────────

describe('list param queries', () => {
  it('getBooksByIds returns the requested books', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const books = await queries.getBooksByIds(client, [1, 3]);
      assert.equal(books.length, 2);
      const titles = new Set(books.map(b => b.title));
      assert.ok(titles.has('Foundation'));
      assert.ok(titles.has('Dune'));
    } finally { await teardown(client, schema); }
  });
});

// ─── IS NULL / IS NOT NULL tests ──────────────────────────────────────────────

describe('NULL tests', () => {
  it('getAuthorsWithNullBio returns authors without bio', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.getAuthorsWithNullBio(client);
      assert.equal(rows.length, 1);
      assert.equal(rows[0].name, 'Herbert');
    } finally { await teardown(client, schema); }
  });

  it('getAuthorsWithBio returns authors with bio', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.getAuthorsWithBio(client);
      assert.equal(rows.length, 2);
      const names = new Set(rows.map(r => r.name));
      assert.ok(names.has('Asimov'));
      assert.ok(names.has('Le Guin'));
    } finally { await teardown(client, schema); }
  });
});

// ─── Date range tests ─────────────────────────────────────────────────────────

describe('date range queries', () => {
  it('getBooksPublishedBetween filters by date range', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.getBooksPublishedBetween(client, '1951-01-01', '1966-01-01');
      assert.equal(rows.length, 2);
      const titles = new Set(rows.map(r => r.title));
      assert.ok(titles.has('Foundation'));
      assert.ok(titles.has('Dune'));
    } finally { await teardown(client, schema); }
  });
});

// ─── DISTINCT tests ───────────────────────────────────────────────────────────

describe('DISTINCT queries', () => {
  it('getDistinctGenres returns unique genres', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.getDistinctGenres(client);
      assert.equal(rows.length, 2);
      const genres = new Set(rows.map(r => r.genre));
      assert.ok(genres.has('sci-fi'));
      assert.ok(genres.has('fantasy'));
    } finally { await teardown(client, schema); }
  });
});

// ─── LEFT JOIN aggregate tests ────────────────────────────────────────────────

describe('LEFT JOIN aggregate queries', () => {
  it('getBooksWithSalesCount returns correct totals', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const rows = await queries.getBooksWithSalesCount(client);
      assert.equal(rows.length, 4);
      const foundation = rows.find(r => r.title === 'Foundation');
      assert.ok(foundation);
      assert.equal(Number(foundation.total_quantity), 2);
      const iRobot = rows.find(r => r.title === 'I Robot');
      assert.ok(iRobot);
      assert.equal(Number(iRobot.total_quantity), 0);
    } finally { await teardown(client, schema); }
  });
});

// ─── Scalar aggregate tests ───────────────────────────────────────────────────

describe('scalar aggregate queries', () => {
  it('countSaleItems returns item count for a sale', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      const row = await queries.countSaleItems(client, 1);
      assert.ok(row);
      assert.equal(Number(row.item_count), 2);
    } finally { await teardown(client, schema); }
  });
});

// ─── Upsert tests (PostgreSQL-specific) ──────────────────────────────────────

describe('upsert queries', () => {
  it('upsertProduct inserts then updates on conflict', async () => {
    const { client, schema } = await makeClient();
    try {
      const productId = '00000000-0000-0000-0000-000000000001';

      const inserted = await queries.upsertProduct(client, productId, 'SKU-001', 'Widget', true, ['tag1'], 10);
      assert.ok(inserted);
      assert.equal(inserted.name, 'Widget');
      assert.equal(inserted.stock_count, 10);

      const updated = await queries.upsertProduct(client, productId, 'SKU-001', 'Widget Pro', true, ['tag1', 'tag2'], 25);
      assert.ok(updated);
      assert.equal(updated.name, 'Widget Pro');
      assert.equal(updated.stock_count, 25);
    } finally { await teardown(client, schema); }
  });
});

// ─── CTE DELETE tests (PostgreSQL-specific) ───────────────────────────────────

describe('CTE DELETE queries', () => {
  it('archiveAndReturnBooks deletes and returns the matching rows', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      // Archive books published before 1951-01-01 → only I Robot (no FK violation)
      const archived = await queries.archiveAndReturnBooks(client, '1951-01-01');
      assert.equal(archived.length, 1);
      assert.equal(archived[0].title, 'I Robot');
      const remaining = await queries.listBooksByGenre(client, 'sci-fi');
      assert.ok(!remaining.some(b => b.title === 'I Robot'));
    } finally { await teardown(client, schema); }
  });
});

// ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────────────

describe('MIN/MAX/SUM/AVG aggregate queries', () => {
  it('getSaleItemQuantityAggregates returns correct aggregates', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      // Sale items: Foundation qty 2 (Alice), Dune qty 1 (Alice), Earthsea qty 1 (Bob)
      // → min=1, max=2, sum=4, avg≈1.33
      const row = await queries.getSaleItemQuantityAggregates(client);
      assert.ok(row !== null);
      assert.equal(row!.min_qty, 1);
      assert.equal(row!.max_qty, 2);
      assert.equal(Number(row!.sum_qty), 4);
      assert.ok(Math.abs(Number(row!.avg_qty) - 4 / 3) < 0.01);
    } finally { await teardown(client, schema); }
  });

  it('getBookPriceAggregates returns correct aggregates', async () => {
    const { client, schema } = await makeClient();
    try {
      await seed(client);
      // Book prices: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96, avg=9.99
      const row = await queries.getBookPriceAggregates(client);
      assert.ok(row !== null);
      assert.ok(Math.abs(Number(row!.min_price) - 7.99) < 0.01);
      assert.ok(Math.abs(Number(row!.max_price) - 12.99) < 0.01);
      assert.ok(Math.abs(Number(row!.sum_price) - 39.96) < 0.01);
      assert.ok(Math.abs(Number(row!.avg_price) - 9.99) < 0.01);
    } finally { await teardown(client, schema); }
  });
});
