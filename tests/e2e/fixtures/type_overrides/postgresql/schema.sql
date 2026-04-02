CREATE TABLE event (
    id           BIGSERIAL    PRIMARY KEY,
    name         TEXT         NOT NULL,
    payload      JSONB        NOT NULL,
    meta         JSON,
    doc_id       UUID         NOT NULL,
    created_at   TIMESTAMP    NOT NULL,
    scheduled_at TIMESTAMPTZ,
    event_date   DATE,
    event_time   TIME
);
