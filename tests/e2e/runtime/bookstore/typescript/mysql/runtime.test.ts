/**
 * End-to-end runtime tests for the generated TypeScript/MySQL queries.
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

const FIXTURES = join(__dirname, '../../../../fixtures/bookstore/mysql');

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

async function seed(conn: mysql.Connection): Promise<void> {
  await queries.createAuthor(conn, 'Asimov', 'Sci-fi master', 1920);
  await queries.createAuthor(conn, 'Herbert', null, 1920);
  await queries.createAuthor(conn, 'Le Guin', 'Earthsea', 1929);

  await queries.createBook(conn, 1, 'Foundation', 'sci-fi', 9.99, '1951-01-01');
  await queries.createBook(conn, 1, 'I Robot', 'sci-fi', 7.99, '1950-01-01');
  await queries.createBook(conn, 2, 'Dune', 'sci-fi', 12.99, '1965-01-01');
  await queries.createBook(conn, 3, 'Earthsea', 'fantasy', 8.99, '1968-01-01');

  await queries.createCustomer(conn, 'Alice', 'alice@example.com');
  await queries.createSale(conn, 1);
  await queries.addSaleItem(conn, 1, 1, 2, 9.99);   // Foundation qty 2
  await queries.addSaleItem(conn, 1, 3, 1, 12.99);  // Dune qty 1
}

// ─── :one tests ───────────────────────────────────────────────────────────────

describe(':one queries', () => {
  it('getAuthor returns the correct author', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const author = await queries.getAuthor(conn, 1);
      assert.ok(author);
      assert.equal(author.name, 'Asimov');
      assert.equal(author.bio, 'Sci-fi master');
      assert.equal(author.birth_year, 1920);
    } finally { await teardown(conn, dbName); }
  });

  it('getAuthor returns null for unknown id', async () => {
    const { conn, dbName } = await makeConn();
    try {
      assert.equal(await queries.getAuthor(conn, 999), null);
    } finally { await teardown(conn, dbName); }
  });

  it('getBook returns the correct book', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const book = await queries.getBook(conn, 1);
      assert.ok(book);
      assert.equal(book.title, 'Foundation');
      assert.equal(book.genre, 'sci-fi');
    } finally { await teardown(conn, dbName); }
  });
});

// ─── :many tests ──────────────────────────────────────────────────────────────

describe(':many queries', () => {
  it('listAuthors returns all authors sorted by name', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const authors = await queries.listAuthors(conn);
      assert.equal(authors.length, 3);
      assert.equal(authors[0].name, 'Asimov');
      assert.equal(authors[1].name, 'Herbert');
      assert.equal(authors[2].name, 'Le Guin');
    } finally { await teardown(conn, dbName); }
  });

  it('listBooksByGenre filters correctly', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      assert.equal((await queries.listBooksByGenre(conn, 'sci-fi')).length, 3);
      const fantasy = await queries.listBooksByGenre(conn, 'fantasy');
      assert.equal(fantasy.length, 1);
      assert.equal(fantasy[0].title, 'Earthsea');
    } finally { await teardown(conn, dbName); }
  });

  it('listBooksByGenreOrAll returns all when given "all"', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      assert.equal((await queries.listBooksByGenreOrAll(conn, 'all')).length, 4);
      assert.equal((await queries.listBooksByGenreOrAll(conn, 'sci-fi')).length, 3);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── UpdateAuthorBio / DeleteAuthor tests ─────────────────────────────────────

describe('updateAuthorBio / deleteAuthor queries', () => {
  it('updateAuthorBio updates the row', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      await queries.updateAuthorBio(conn, 'Updated bio', 1);
      const author = await queries.getAuthor(conn, 1);
      assert.ok(author);
      assert.equal(author.bio, 'Updated bio');
    } finally { await teardown(conn, dbName); }
  });

  it('deleteAuthor removes the row', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await queries.createAuthor(conn, 'Temp', null, null);
      await queries.deleteAuthor(conn, 1);
      assert.equal(await queries.getAuthor(conn, 1), null);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── CreateBook / AddSaleItem tests ───────────────────────────────────────────

describe('createBook / addSaleItem queries', () => {
  it('createBook inserts without error', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      await queries.createBook(conn, 1, 'New Book', 'mystery', 14.50, null);
      const book = await queries.getBook(conn, 5);
      assert.ok(book);
      assert.equal(book.title, 'New Book');
      assert.equal(book.genre, 'mystery');
    } finally { await teardown(conn, dbName); }
  });

  it('addSaleItem inserts without error', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      await queries.addSaleItem(conn, 1, 4, 1, 8.99);
      const [rows] = await conn.execute<mysql.RowDataPacket[]>(
        'SELECT COUNT(*) AS c FROM sale_item WHERE sale_id = 1'
      );
      assert.equal(rows[0].c, 3);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── CASE / COALESCE tests ────────────────────────────────────────────────────

describe('CASE / COALESCE queries', () => {
  it('getBookPriceLabel returns price label for each book', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.getBookPriceLabel(conn, 10);
      assert.equal(rows.length, 4);
      const dune = rows.find(r => r.title === 'Dune');
      assert.ok(dune);
      assert.equal(dune.price_label, 'expensive');
      const earthsea = rows.find(r => r.title === 'Earthsea');
      assert.ok(earthsea);
      assert.equal(earthsea.price_label, 'affordable');
    } finally { await teardown(conn, dbName); }
  });

  it('getBookPriceOrDefault returns effective price', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.getBookPriceOrDefault(conn, 0);
      assert.equal(rows.length, 4);
      assert.ok(rows.every(r => Number(r.effective_price) > 0));
    } finally { await teardown(conn, dbName); }
  });
});

// ─── Product type coverage ────────────────────────────────────────────────────

describe('product queries', () => {
  it('getProduct returns the inserted product', async () => {
    const { conn, dbName } = await makeConn();
    try {
      const pid = 'prod-get-001';
      await conn.execute(
        "INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, TRUE, ?)",
        [pid, 'SKU-GET', 'Widget', 5]
      );
      const row = await queries.getProduct(conn, pid);
      assert.ok(row);
      assert.equal(row.id, pid);
      assert.equal(row.name, 'Widget');
      assert.equal(row.stock_count, 5);
    } finally { await teardown(conn, dbName); }
  });

  it('listActiveProducts filters by active flag', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await conn.execute(
        "INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, TRUE, ?)",
        ['act-1', 'ACT-1', 'Active', 10]
      );
      await conn.execute(
        "INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, FALSE, ?)",
        ['inact-1', 'INACT-1', 'Inactive', 0]
      );
      const active = await queries.listActiveProducts(conn, true);
      assert.equal(active.length, 1);
      assert.equal(active[0].name, 'Active');
      const inactive = await queries.listActiveProducts(conn, false);
      assert.equal(inactive.length, 1);
      assert.equal(inactive[0].name, 'Inactive');
    } finally { await teardown(conn, dbName); }
  });
});

// ─── :exec tests ──────────────────────────────────────────────────────────────

describe(':exec queries', () => {
  it('createAuthor inserts a row', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await queries.createAuthor(conn, 'Test', null, null);
      const author = await queries.getAuthor(conn, 1);
      assert.ok(author);
      assert.equal(author.name, 'Test');
      assert.equal(author.bio, null);
      assert.equal(author.birth_year, null);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── CreateCustomer / CreateSale tests ────────────────────────────────────────

describe('createCustomer / createSale queries', () => {
  it('createCustomer inserts a row', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await queries.createCustomer(conn, 'Bob', 'bob@example.com');
      const [rows] = await conn.execute<mysql.RowDataPacket[]>(
        "SELECT COUNT(*) AS c FROM customer WHERE name = 'Bob'"
      );
      assert.equal(rows[0].c, 1);
    } finally { await teardown(conn, dbName); }
  });

  it('createSale inserts a sale row', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      await queries.createSale(conn, 1);
      const [rows] = await conn.execute<mysql.RowDataPacket[]>(
        'SELECT COUNT(*) AS c FROM sale WHERE customer_id = 1'
      );
      assert.equal(rows[0].c, 2);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── :execrows tests ──────────────────────────────────────────────────────────

describe(':execrows queries', () => {
  it('deleteBookById returns affected row count', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      // I Robot (id=2) has no sale_items
      assert.equal(await queries.deleteBookById(conn, 2), 1);
      assert.equal(await queries.deleteBookById(conn, 999), 0);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── JOIN tests ───────────────────────────────────────────────────────────────

describe('JOIN queries', () => {
  it('listBooksWithAuthor returns joined rows', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.listBooksWithAuthor(conn);
      assert.equal(rows.length, 4);
      const dune = rows.find(r => r.title === 'Dune');
      assert.ok(dune);
      assert.equal(dune.author_name, 'Herbert');
      assert.equal(dune.author_bio, null);
      const foundation = rows.find(r => r.title === 'Foundation');
      assert.ok(foundation);
      assert.equal(foundation.author_bio, 'Sci-fi master');
    } finally { await teardown(conn, dbName); }
  });

  it('getBooksNeverOrdered returns books with no sale_items', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      // Seed has only Alice buying Foundation + Dune; I Robot and Earthsea were never ordered
      const books = await queries.getBooksNeverOrdered(conn);
      assert.equal(books.length, 2);
      const titles = new Set(books.map(b => b.title));
      assert.ok(titles.has('I Robot'));
      assert.ok(titles.has('Earthsea'));
    } finally { await teardown(conn, dbName); }
  });
});

// ─── CTE tests ────────────────────────────────────────────────────────────────

describe('CTE queries', () => {
  it('getTopSellingBooks ranks Foundation first', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.getTopSellingBooks(conn);
      assert.ok(rows.length > 0);
      assert.equal(rows[0].title, 'Foundation');
    } finally { await teardown(conn, dbName); }
  });

  it('getBestCustomers returns Alice', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.getBestCustomers(conn);
      assert.equal(rows.length, 1);
      assert.equal(rows[0].name, 'Alice');
    } finally { await teardown(conn, dbName); }
  });

  it('getAuthorStats returns one row per author', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.getAuthorStats(conn);
      assert.equal(rows.length, 3);
      const asimov = rows.find(r => r.name === 'Asimov');
      assert.ok(asimov);
      assert.equal(Number(asimov.num_books), 2);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── Aggregate tests ──────────────────────────────────────────────────────────

describe('aggregate queries', () => {
  it('countBooksByGenre returns correct counts', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.countBooksByGenre(conn);
      assert.equal(rows.length, 2);
      const fantasy = rows.find(r => r.genre === 'fantasy');
      assert.ok(fantasy);
      assert.equal(Number(fantasy.book_count), 1);
      const sciFi = rows.find(r => r.genre === 'sci-fi');
      assert.ok(sciFi);
      assert.equal(Number(sciFi.book_count), 3);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── LIMIT/OFFSET tests ───────────────────────────────────────────────────────

describe('LIMIT/OFFSET queries', () => {
  it('listBooksWithLimit paginates without overlap', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const page1 = await queries.listBooksWithLimit(conn, 2, 0);
      const page2 = await queries.listBooksWithLimit(conn, 2, 2);
      assert.equal(page1.length, 2);
      assert.equal(page2.length, 2);
      const t1 = new Set(page1.map(r => r.title));
      for (const r of page2) assert.ok(!t1.has(r.title));
    } finally { await teardown(conn, dbName); }
  });
});

// ─── LIKE tests ───────────────────────────────────────────────────────────────

describe('LIKE queries', () => {
  it('searchBooksByTitle filters by pattern', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const results = await queries.searchBooksByTitle(conn, '%ound%');
      assert.equal(results.length, 1);
      assert.equal(results[0].title, 'Foundation');
      assert.equal((await queries.searchBooksByTitle(conn, 'NOPE%')).length, 0);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── BETWEEN tests ────────────────────────────────────────────────────────────

describe('BETWEEN queries', () => {
  it('getBooksByPriceRange returns books in range', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      // Foundation (9.99) and Earthsea (8.99) are in [8, 10]
      const results = await queries.getBooksByPriceRange(conn, 8, 10);
      assert.equal(results.length, 2);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── IN list tests ────────────────────────────────────────────────────────────

describe('IN list queries', () => {
  it('getBooksInGenres matches all genres', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const results = await queries.getBooksInGenres(conn, 'sci-fi', 'fantasy', 'horror');
      assert.equal(results.length, 4);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── HAVING tests ─────────────────────────────────────────────────────────────

describe('HAVING queries', () => {
  it('getGenresWithManyBooks filters by count threshold', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const results = await queries.getGenresWithManyBooks(conn, 1);
      assert.equal(results.length, 1);
      assert.equal(results[0].genre, 'sci-fi');
      assert.equal(Number(results[0].book_count), 3);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── Subquery tests ───────────────────────────────────────────────────────────

describe('subquery queries', () => {
  it('getBooksNotByAuthor excludes the named author', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const results = await queries.getBooksNotByAuthor(conn, 'Asimov');
      assert.equal(results.length, 2);
      assert.ok(!results.some(r => r.title === 'Foundation'));
      assert.ok(!results.some(r => r.title === 'I Robot'));
    } finally { await teardown(conn, dbName); }
  });

  it('getBooksWithRecentSales returns books sold after date', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      // Sales are current; use a far-past cutoff
      const results = await queries.getBooksWithRecentSales(conn, new Date('2000-01-01'));
      // Foundation and Dune have sale_items
      assert.equal(results.length, 2);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── Scalar subquery test ─────────────────────────────────────────────────────

describe('scalar subquery queries', () => {
  it('getBookWithAuthorName resolves author via subquery', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.getBookWithAuthorName(conn);
      assert.equal(rows.length, 4);
      const dune = rows.find(r => r.title === 'Dune');
      assert.ok(dune);
      assert.equal(dune.author_name, 'Herbert');
    } finally { await teardown(conn, dbName); }
  });
});

// ─── JOIN with param tests ────────────────────────────────────────────────────

describe('JOIN with param queries', () => {
  it('getBooksByAuthorParam filters by birth_year', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      // birth_year > 1925 → only Le Guin (1929) → Earthsea
      const results = await queries.getBooksByAuthorParam(conn, 1925);
      assert.equal(results.length, 1);
      assert.equal(results[0].title, 'Earthsea');
    } finally { await teardown(conn, dbName); }
  });
});

// ─── Qualified wildcard tests ─────────────────────────────────────────────────

describe('wildcard queries', () => {
  it('getAllBookFields returns all books with full columns', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const books = await queries.getAllBookFields(conn);
      assert.equal(books.length, 4);
      assert.equal(books[0].title, 'Foundation');
    } finally { await teardown(conn, dbName); }
  });
});

// ─── List param tests ─────────────────────────────────────────────────────────

describe('list param queries', () => {
  it('getBooksByIds returns the requested books', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const books = await queries.getBooksByIds(conn, [1, 3]);
      assert.equal(books.length, 2);
      const titles = new Set(books.map(b => b.title));
      assert.ok(titles.has('Foundation'));
      assert.ok(titles.has('Dune'));
    } finally { await teardown(conn, dbName); }
  });
});

// ─── IS NULL / IS NOT NULL tests ──────────────────────────────────────────────

describe('NULL tests', () => {
  it('getAuthorsWithNullBio returns authors without bio', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.getAuthorsWithNullBio(conn);
      assert.equal(rows.length, 1);
      assert.equal(rows[0].name, 'Herbert');
    } finally { await teardown(conn, dbName); }
  });

  it('getAuthorsWithBio returns authors with bio', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.getAuthorsWithBio(conn);
      assert.equal(rows.length, 2);
      const names = new Set(rows.map(r => r.name));
      assert.ok(names.has('Asimov'));
      assert.ok(names.has('Le Guin'));
    } finally { await teardown(conn, dbName); }
  });
});

// ─── Date range tests ─────────────────────────────────────────────────────────

describe('date range queries', () => {
  it('getBooksPublishedBetween filters by date range', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      // 1951-01-01 to 1966-01-01 → Foundation (1951) and Dune (1965)
      const rows = await queries.getBooksPublishedBetween(
        conn, '1951-01-01', '1966-01-01'
      );
      assert.equal(rows.length, 2);
      const titles = new Set(rows.map(r => r.title));
      assert.ok(titles.has('Foundation'));
      assert.ok(titles.has('Dune'));
    } finally { await teardown(conn, dbName); }
  });
});

// ─── DISTINCT tests ───────────────────────────────────────────────────────────

describe('DISTINCT queries', () => {
  it('getDistinctGenres returns unique genres', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.getDistinctGenres(conn);
      assert.equal(rows.length, 2);
      const genres = new Set(rows.map(r => r.genre));
      assert.ok(genres.has('sci-fi'));
      assert.ok(genres.has('fantasy'));
    } finally { await teardown(conn, dbName); }
  });
});

// ─── LEFT JOIN aggregate tests ────────────────────────────────────────────────

describe('LEFT JOIN aggregate queries', () => {
  it('getBooksWithSalesCount returns correct totals', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const rows = await queries.getBooksWithSalesCount(conn);
      assert.equal(rows.length, 4);
      const foundation = rows.find(r => r.title === 'Foundation');
      assert.ok(foundation);
      assert.equal(Number(foundation.total_quantity), 2);
      const iRobot = rows.find(r => r.title === 'I Robot');
      assert.ok(iRobot);
      assert.equal(Number(iRobot.total_quantity), 0);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── Scalar aggregate tests ───────────────────────────────────────────────────

describe('scalar aggregate queries', () => {
  it('countSaleItems returns item count for a sale', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      const row = await queries.countSaleItems(conn, 1);
      assert.ok(row);
      assert.equal(Number(row.item_count), 2);
    } finally { await teardown(conn, dbName); }
  });
});

// ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────────────

describe('MIN/MAX/SUM/AVG aggregate queries', () => {
  it('getSaleItemQuantityAggregates returns correct aggregates', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      // Seed: Foundation qty 2 + Dune qty 1 → min=1, max=2, sum=3, avg=1.5
      const row = await queries.getSaleItemQuantityAggregates(conn);
      assert.ok(row !== null);
      assert.equal(row!.min_qty, 1);
      assert.equal(row!.max_qty, 2);
      assert.equal(Number(row!.sum_qty), 3);
      assert.ok(Math.abs(Number(row!.avg_qty) - 1.5) < 0.01);
    } finally { await teardown(conn, dbName); }
  });

  it('getBookPriceAggregates returns correct aggregates', async () => {
    const { conn, dbName } = await makeConn();
    try {
      await seed(conn);
      // Book prices: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96, avg≈9.99
      const row = await queries.getBookPriceAggregates(conn);
      assert.ok(row !== null);
      assert.ok(Math.abs(Number(row!.min_price) - 7.99) < 0.01);
      assert.ok(Math.abs(Number(row!.max_price) - 12.99) < 0.01);
      assert.ok(Math.abs(Number(row!.sum_price) - 39.96) < 0.01);
      assert.ok(Math.abs(Number(row!.avg_price) - 9.99) < 0.01);
    } finally { await teardown(conn, dbName); }
  });
});
