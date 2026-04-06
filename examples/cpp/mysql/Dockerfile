FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    libmariadb-dev-compat \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY gen/ gen/
COPY main.cpp .

RUN g++ -std=c++20 -O2 -o demo main.cpp gen/queries/queries.cpp $(mysql_config --cflags --libs)

ENV MIGRATIONS_DIR=/app/migrations
# migrations/ is mounted as a read-only volume at runtime
CMD ["./demo"]
