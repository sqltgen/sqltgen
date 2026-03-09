CREATE TABLE author (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL,
    bio        TEXT,
    birth_year INTEGER
);

CREATE TABLE book (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    author_id    INTEGER NOT NULL REFERENCES author(id),
    title        TEXT    NOT NULL,
    genre        TEXT    NOT NULL,
    price        DECIMAL NOT NULL,
    published_at TEXT
);

CREATE TABLE customer (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name  TEXT    NOT NULL,
    email TEXT    NOT NULL UNIQUE
);

CREATE TABLE sale (
    id          INTEGER  PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER  NOT NULL REFERENCES customer(id),
    ordered_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sale_item (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_id    INTEGER NOT NULL REFERENCES sale(id),
    book_id    INTEGER NOT NULL REFERENCES book(id),
    quantity   INTEGER NOT NULL,
    unit_price DECIMAL NOT NULL
);
