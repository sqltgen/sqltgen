FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM alpine:3
COPY --from=builder /build/target/release/sqltgen /usr/local/bin/sqltgen
WORKDIR /workspace
ENTRYPOINT ["sqltgen"]
