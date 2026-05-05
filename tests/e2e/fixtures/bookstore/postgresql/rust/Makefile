REPO_ROOT := $(shell git rev-parse --show-toplevel)
SQLTGEN   := $(REPO_ROOT)/target/debug/sqltgen

.PHONY: generate test clean

generate:
	rm -rf src/db
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	cargo test

clean:
	cargo clean
	rm -rf src/db
