CREATE TABLE book (
    id           BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    author_id    BIGINT        NOT NULL,
    title        TEXT          NOT NULL,
    genre        TEXT          NOT NULL,
    price        DECIMAL(10,2) NOT NULL,
    published_at DATE,
    FOREIGN KEY (author_id) REFERENCES author(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
