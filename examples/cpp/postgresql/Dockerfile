FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    libpq-dev \
    make \
    cmake \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build libpqxx 7.10.0 from source
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
COPY main.cpp .

RUN g++ -std=c++20 -O2 -o demo main.cpp gen/queries/queries.cpp -lpqxx -lpq

ENV MIGRATIONS_DIR=/app/migrations
# migrations/ is mounted as a read-only volume at runtime
CMD ["./demo"]
