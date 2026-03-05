CREATE TABLE author (
    id         BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name       TEXT   NOT NULL,
    bio        TEXT,
    birth_year INTEGER
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
