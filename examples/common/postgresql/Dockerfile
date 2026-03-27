FROM postgres:16-alpine

# Use a path outside the VOLUME declared by the base image (/var/lib/postgresql/data),
# otherwise Docker discards RUN writes to that path at build time.
ENV PGDATA=/pgdata

# Pre-initialize the cluster at build time so container startup skips initdb entirely.
RUN mkdir -p "$PGDATA" && \
    chown postgres:postgres "$PGDATA" && \
    su-exec postgres initdb --nosync --wal-segsize=1 && \
    echo "host all all 0.0.0.0/0 trust" >> "$PGDATA/pg_hba.conf" && \
    su-exec postgres pg_ctl start -D "$PGDATA" \
        -o "-c listen_addresses=127.0.0.1 -c fsync=off" -w && \
    su-exec postgres psql -U postgres -c "CREATE USER sqltgen WITH PASSWORD 'sqltgen' CREATEDB;" && \
    su-exec postgres psql -U postgres -c "CREATE DATABASE sqltgen OWNER sqltgen;" && \
    su-exec postgres pg_ctl stop -D "$PGDATA" -m fast -w && \
    rm -f "$PGDATA/postmaster.pid"
