REPO_ROOT  := $(shell git rev-parse --show-toplevel)
SQLTGEN    := $(REPO_ROOT)/target/debug/sqltgen
OUTPUT_DIR := src/db

.PHONY: generate runtime-test clean

generate:
	rm -rf $(OUTPUT_DIR)
	$(SQLTGEN) generate --config sqltgen.json

runtime-test:
	cargo test -q

clean:
	cargo clean
	rm -rf $(OUTPUT_DIR)

include $(REPO_ROOT)/tests/e2e/snapshot-gate.mk
