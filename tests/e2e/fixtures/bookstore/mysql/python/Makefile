REPO_ROOT  := $(shell git rev-parse --show-toplevel)
SQLTGEN    := $(REPO_ROOT)/target/debug/sqltgen
OUTPUT_DIR := gen

.PHONY: generate runtime-test clean

generate:
	rm -rf $(OUTPUT_DIR)
	$(SQLTGEN) generate --config sqltgen.json

runtime-test:
	python -m pytest -xvs

clean:
	rm -rf $(OUTPUT_DIR) __pycache__ .pytest_cache

include $(REPO_ROOT)/tests/e2e/fixtures/snapshot-gate.mk
