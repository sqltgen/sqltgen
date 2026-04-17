FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    libsqlite3-dev \
    libgtest-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY gen/ gen/
COPY runtime_test.cpp .

RUN g++ -std=c++20 -O0 -g \
    -DSCHEMA_PATH='"/fixtures/schema.sql"' \
    runtime_test.cpp gen/queries/queries.cpp \
    -lsqlite3 -lgtest_main -lgtest -lpthread \
    -o test_runner

CMD ["./test_runner"]
