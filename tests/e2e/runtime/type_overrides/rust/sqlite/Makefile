include ../../../common.mk

.PHONY: generate test clean

generate:
	rm -rf src/db
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	cargo test -q

clean:
	cargo clean
	rm -rf src/db
