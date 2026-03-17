SQLTGEN := ../../../target/debug/sqltgen

.PHONY: clean generate build run

clean:
	rm -rf gen

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

build: generate

run: build
	go run main.go
