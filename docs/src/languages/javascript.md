# JavaScript

sqltgen generates JavaScript code with JSDoc type annotations. It uses the same
database drivers as the TypeScript backend: pg, better-sqlite3, and mysql2.

The generated JavaScript is functionally identical to the TypeScript output, but
replaces inline TypeScript syntax with JSDoc comments (`@typedef`, `@param`,
`@returns`).

## Configuration

```json
"javascript": {
  "out": "src/db",
  "package": ""
}
```

| Field | Description |
|---|---|
| `out` | Output directory. |
| `package` | Unused for JavaScript — set to `""`. |
| `list_params` | `"native"` (default) or `"dynamic"`. |

## What is generated

```
src/db/
  index.js            — barrel export
  author.js           — Author typedef
  book.js             — Book typedef
  _sqltgen.js         — shared adapter typedef
  queries.js          — async query functions + Querier class
```

### Model typedefs

```javascript
// src/db/author.js

/**
 * @typedef {Object} Author
 * @property {number} id
 * @property {string} name
 * @property {string | null} bio
 * @property {number | null} birth_year
 */
```

### Query functions

```javascript
// src/db/queries.js

const SQL_GET_AUTHOR   = `SELECT id, name, bio, birth_year FROM author WHERE id = $1`;
const SQL_LIST_AUTHORS = `SELECT id, name, bio, birth_year FROM author ORDER BY name`;

/**
 * @param {import('pg').ClientBase} db
 * @param {number} id
 * @returns {Promise<Author | null>}
 */
export async function getAuthor(db, id) {
  const result = await db.query(SQL_GET_AUTHOR, [id]);
  return result.rows[0] ?? null;
}

/**
 * @param {import('pg').ClientBase} db
 * @returns {Promise<Author[]>}
 */
export async function listAuthors(db) {
  const result = await db.query(SQL_LIST_AUTHORS);
  return result.rows;
}
```

### Querier class

```javascript
export class Querier {
  /** @param {() => import('pg').ClientBase | Promise<import('pg').ClientBase>} connect */
  constructor(connect) {
    this._connect = connect;
  }

  /** @returns {Promise<Author | null>} */
  async getAuthor(id) {
    const db = await this._connect();
    try {
      return await getAuthor(db, id);
    } finally {
      if (typeof db.end === 'function') await db.end();
    }
  }
}
```

## Wiring up

### PostgreSQL

```sh
npm install pg
```

```javascript
import { Client } from 'pg';
import { getAuthor, listAuthors } from './src/db/queries.js';

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
```

```javascript
import Database from 'better-sqlite3';
import { getAuthor } from './src/db/queries.js';

const db = new Database('mydb.db');
const author = getAuthor(db, 1);  // synchronous
```

### MySQL

```sh
npm install mysql2
```

```javascript
import mysql from 'mysql2/promise';
import { getAuthor } from './src/db/queries.js';

const conn = await mysql.createConnection({
  host: 'localhost', database: 'mydb',
  user: 'user', password: 'pass'
});
const author = await getAuthor(conn, 1);
```

## Differences from TypeScript

| TypeScript | JavaScript |
|---|---|
| `.ts` file extension | `.js` file extension |
| Inline `interface` types | `@typedef` JSDoc comments |
| Inline parameter types | `@param` JSDoc annotations |
| `index.ts` barrel | `index.js` barrel |

All drivers, driver setup, and query function semantics are identical.
See the [TypeScript guide](typescript.md) for more detail.

## Naming conventions

Identical to TypeScript: function names are `camelCase`, interface field names
follow `snake_case` database column names.
