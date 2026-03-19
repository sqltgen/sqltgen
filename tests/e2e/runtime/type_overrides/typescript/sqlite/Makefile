SQLTGEN  := ../../../../../../target/debug/sqltgen

.PHONY: generate test clean install

install:
	npm install

generate:
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	npm test

clean:
	rm -rf gen node_modules
