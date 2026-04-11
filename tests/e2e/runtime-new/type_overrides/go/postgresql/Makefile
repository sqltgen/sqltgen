REPO_ROOT := $(shell git rev-parse --show-toplevel)
SQLTGEN   := $(REPO_ROOT)/target/debug/sqltgen

.PHONY: generate test clean

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	go test -v -count=1 ./...

clean:
	rm -rf gen
