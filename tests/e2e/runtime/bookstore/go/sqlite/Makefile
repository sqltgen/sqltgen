SQLTGEN := ../../../../../../target/debug/sqltgen

.PHONY: generate test clean

generate:
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	go test -v -count=1 ./...

clean:
	rm -rf gen
