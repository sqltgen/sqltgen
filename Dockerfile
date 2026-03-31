# Builder pinned to the native platform so proc-macros compile without QEMU.
FROM --platform=$BUILDPLATFORM rust:slim AS builder

ARG TARGETARCH

# Install cross-compilation tooling for each supported target arch.
RUN apt-get update && apt-get install -y --no-install-recommends \
        musl-tools \
        gcc-aarch64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*

# Select the musl target triple and linker based on the requested architecture.
RUN case "$TARGETARCH" in \
        amd64) echo x86_64-unknown-linux-musl  > /target ;; \
        arm64) echo aarch64-unknown-linux-musl > /target ;; \
        *)     echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac

RUN rustup target add "$(cat /target)"

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# CARGO_TARGET_<TRIPLE>_RUSTFLAGS applies only to the final binary, not to
# proc-macros (which run on the host), so +crt-static works without conflicts.
RUN TARGET="$(cat /target)" && \
    TRIPLE_UPPER="$(echo "$TARGET" | tr '[:lower:]-' '[:upper:]_')" && \
    export "CARGO_TARGET_${TRIPLE_UPPER}_RUSTFLAGS=-C target-feature=+crt-static" && \
    if [ "$TARGETARCH" = "arm64" ]; then \
        export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-gnu-gcc; \
    fi && \
    cargo build --release --target "$TARGET" && \
    cp "target/$TARGET/release/sqltgen" /sqltgen

FROM scratch
COPY --from=builder /sqltgen /sqltgen
WORKDIR /workspace
ENTRYPOINT ["/sqltgen"]
