# TypeScript

sqltgen generates TypeScript code with full type annotations. Database access
uses [pg](https://node-postgres.com) for PostgreSQL,
[better-sqlite3](https://github.com/WiseLibs/better-sqlite3) for SQLite, and
[mysql2](https://github.com/sidorares/node-mysql2) for MySQL.

> **Note:** SQLite better-sqlite3 is synchronous. Generated SQLite functions do
> not return `Promise` — they return values directly.

## Configuration

```json
"typescript": {
  "out": "src/db",
  "package": ""
}
```

| Field | Description |
|---|---|
| `out` | Output directory. |
| `package` | Unused for TypeScript — set to `""`. |
| `list_params` | `"native"` (default) or `"dynamic"`. |

## What is generated

```
src/db/
  index.ts            — barrel export
  author.ts           — Author interface
  book.ts             — Book interface
  _sqltgen.ts         — shared SqltgenAdapter type
  queries.ts          — async query functions + Querier class
```

### Model interfaces

```typescript
// src/db/author.ts
export interface Author {
  id: number;
  name: string;
  bio: string | null;
  birth_year: number | null;
}
```

- Non-null columns → bare type.
- Nullable columns → `T | null`.
- Column names are kept as `snake_case` to match the database column names exactly.

### Query functions

```typescript
// src/db/queries.ts
import type { ClientBase } from 'pg';
import type { Author } from './author';

const SQL_GET_AUTHOR    = `SELECT id, name, bio, birth_year FROM author WHERE id = $1`;
const SQL_LIST_AUTHORS  = `SELECT id, name, bio, birth_year FROM author ORDER BY name`;
const SQL_DELETE_AUTHOR = `DELETE FROM author WHERE id = $1`;

export async function getAuthor(db: ClientBase, id: number): Promise<Author | null> {
  const result = await db.query<Author>(SQL_GET_AUTHOR, [id]);
  return result.rows[0] ?? null;
}

export async function listAuthors(db: ClientBase): Promise<Author[]> {
  const result = await db.query<Author>(SQL_LIST_AUTHORS);
  return result.rows;
}

export async function deleteAuthor(db: ClientBase, id: number): Promise<void> {
  await db.query(SQL_DELETE_AUTHOR, [id]);
}

export async function countAuthors(db: ClientBase): Promise<number> {
  const result = await db.query(SQL_COUNT_AUTHORS);
  return result.rowCount ?? 0;  // :execrows
}
```

### Querier class

```typescript
export class Querier {
  constructor(private connect: () => ClientBase | Promise<ClientBase>) {}

  async getAuthor(id: number): Promise<Author | null> {
    const db = await this.connect();
    try {
      return await getAuthor(db, id);
    } finally {
      if ('end' in db) await (db as any).end();
    }
  }
  // …
}
```

## Wiring up

### PostgreSQL

```sh
npm install pg
npm install --save-dev @types/pg
```

```typescript
import { Client } from 'pg';
import { getAuthor, listAuthors } from './src/db/queries';

const client = new Client({
  connectionString: 'postgres://user:pass@localhost/mydb'
});
await client.connect();

const author = await getAuthor(client, 1);
const all    = await listAuthors(client);

await client.end();
```

### SQLite

```sh
npm install better-sqlite3
npm install --save-dev @types/better-sqlite3
```

```typescript
import Database from 'better-sqlite3';
import { getAuthor, listAuthors } from './src/db/queries';

const db = new Database('mydb.db');

// SQLite functions are synchronous — no await needed
const author = getAuthor(db, 1);
const all    = listAuthors(db);
```

### MySQL

```sh
npm install mysql2
```

```typescript
import mysql from 'mysql2/promise';
import { getAuthor } from './src/db/queries';

const conn = await mysql.createConnection({
  host: 'localhost', database: 'mydb',
  user: 'user', password: 'pass'
});

const author = await getAuthor(conn, 1);
await conn.end();
```

## Inline row types

```typescript
export interface ListBooksWithAuthorRow {
  id: number;
  title: string;
  genre: string;
  price: number;
  author_name: string;
  author_bio: string | null;
}

export async function listBooksWithAuthor(db: ClientBase): Promise<ListBooksWithAuthorRow[]> { … }
```

## Naming conventions

| SQL | TypeScript |
|---|---|
| `GetAuthor` | `getAuthor` (function) |
| `ListBooksWithAuthor` | `listBooksWithAuthor` |
| `birth_year` column | `birth_year` (interface field — unchanged) |
| `Author` table | `Author` interface |
