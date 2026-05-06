REPO_ROOT  := $(shell git rev-parse --show-toplevel)
SQLTGEN    := $(REPO_ROOT)/target/debug/sqltgen
OUTPUT_DIR := gen

.PHONY: install generate runtime-test clean

install:
	npm install

generate:
	rm -rf $(OUTPUT_DIR)
	$(SQLTGEN) generate --config sqltgen.json

runtime-test: install
	npm test

clean:
	rm -rf $(OUTPUT_DIR) node_modules

include $(REPO_ROOT)/tests/e2e/snapshot-gate.mk
