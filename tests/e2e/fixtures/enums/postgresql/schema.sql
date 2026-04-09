CREATE TYPE priority AS ENUM ('low', 'medium', 'high', 'critical');
CREATE TYPE status AS ENUM ('open', 'in_progress', 'done', 'cancelled');

CREATE TABLE task (
    id          BIGSERIAL    PRIMARY KEY,
    title       TEXT         NOT NULL,
    priority    priority     NOT NULL,
    status      status       NOT NULL DEFAULT 'open',
    description TEXT,
    tags        priority[]   NOT NULL DEFAULT '{}'
);
