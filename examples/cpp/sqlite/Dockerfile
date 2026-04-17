FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    libsqlite3-dev \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY gen/ gen/
COPY main.cpp .

RUN g++ -std=c++20 -O2 -o demo main.cpp gen/queries/queries.cpp -lsqlite3

ENV MIGRATIONS_DIR=/app/migrations
CMD ["./demo"]
