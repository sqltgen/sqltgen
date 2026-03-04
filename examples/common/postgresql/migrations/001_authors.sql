CREATE TABLE author (
    id         BIGSERIAL    PRIMARY KEY,
    name       TEXT         NOT NULL,
    bio        TEXT,
    birth_year INTEGER
);
