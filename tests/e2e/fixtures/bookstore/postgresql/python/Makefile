REPO_ROOT := $(shell git rev-parse --show-toplevel)
SQLTGEN   := $(REPO_ROOT)/target/debug/sqltgen

.PHONY: generate test clean

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	python -m pytest -xvs

clean:
	rm -rf gen __pycache__ .pytest_cache
