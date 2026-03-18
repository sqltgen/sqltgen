CREATE TABLE author (
    id   BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE book (
    id        BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    author_id BIGINT NOT NULL,
    title     TEXT NOT NULL,
    genre     TEXT NOT NULL
);

CREATE VIEW book_summaries AS
SELECT b.id, b.title, b.genre, a.name AS author_name
FROM book b
JOIN author a ON a.id = b.author_id;

CREATE VIEW sci_fi_books AS
SELECT id, title, author_name
FROM book_summaries
WHERE genre = 'sci-fi';
