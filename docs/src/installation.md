# Installation

## Build from source

The simplest install method. Requires a [Rust stable toolchain](https://rustup.rs).

```sh
git clone https://github.com/sqltgen/sqltgen.git
cd sqltgen
cargo build --release
```

The binary lands at `target/release/sqltgen`. Copy it anywhere on your `PATH`:

```sh
cp target/release/sqltgen ~/.local/bin/
```

## cargo install

If you have Rust installed, `cargo install` fetches and compiles sqltgen from
[crates.io](https://crates.io/crates/sqltgen):

```sh
cargo install sqltgen
```

The binary is placed in `~/.cargo/bin/`, which is on your `PATH` if you installed
Rust via rustup.

## Homebrew (macOS / Linux)

> Distribution packages are planned for v0.1.0. This section will be updated
> when they are available.

```sh
brew install sqltgen/tap/sqltgen
```

## curl / pre-built binaries

Pre-built binaries for Linux (x86\_64, aarch64), macOS (x86\_64, Apple Silicon),
and Windows are available on the
[GitHub releases page](https://github.com/sqltgen/sqltgen/releases).

```sh
# Linux x86_64 — replace VERSION with the latest release tag
curl -L https://github.com/sqltgen/sqltgen/releases/download/VERSION/sqltgen-x86_64-unknown-linux-musl.tar.gz \
  | tar xz
chmod +x sqltgen
sudo mv sqltgen /usr/local/bin/
```

## .deb / .rpm packages

> Planned for v0.1.0.

```sh
# Debian / Ubuntu
sudo dpkg -i sqltgen_VERSION_amd64.deb

# Fedora / RHEL
sudo rpm -i sqltgen-VERSION.x86_64.rpm
```

## Verify the installation

```sh
sqltgen --version
```

Expected output: `sqltgen VERSION`
