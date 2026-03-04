CREATE TABLE book (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    author_id    INTEGER NOT NULL REFERENCES author(id),
    title        TEXT    NOT NULL,
    genre        TEXT    NOT NULL,
    price        DECIMAL NOT NULL,
    published_at TEXT
);
