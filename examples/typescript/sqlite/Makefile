SQLTGEN := ../../../target/debug/sqltgen

.PHONY: clean generate build run

node_modules:
	npm install --silent

clean:
	rm -rf gen node_modules

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

build: generate node_modules

run: build
	npx tsx main.ts
