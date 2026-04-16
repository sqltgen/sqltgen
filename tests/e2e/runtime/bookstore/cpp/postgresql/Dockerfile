FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    libpq-dev \
    libgtest-dev \
    make \
    cmake \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build libpqxx 7.10.0 from source — the apt version (7.7.4) lacks query<T...>,
# query01<T...>, and public pqxx::params which the generated code requires.
RUN git clone --branch 7.10.0 --depth 1 https://github.com/jtv/libpqxx.git /tmp/libpqxx \
    && cmake -S /tmp/libpqxx -B /tmp/libpqxx/build \
        -DCMAKE_BUILD_TYPE=Release \
        -DSKIP_BUILD_TEST=ON \
        -DBUILD_SHARED_LIBS=OFF \
    && cmake --build /tmp/libpqxx/build --parallel \
    && cmake --install /tmp/libpqxx/build \
    && rm -rf /tmp/libpqxx

WORKDIR /app

COPY gen/ gen/
COPY runtime_test.cpp .

RUN g++ -std=c++20 -O0 -g \
    -DSCHEMA_PATH='"/fixtures/schema.sql"' \
    runtime_test.cpp gen/queries/queries.cpp \
    -lpqxx -lpq -lgtest_main -lgtest -lpthread \
    -o test_runner

CMD ["./test_runner"]
