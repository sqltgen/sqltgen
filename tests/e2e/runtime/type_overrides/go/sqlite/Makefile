include ../../../common.mk

.PHONY: generate test clean

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	go test -v -count=1 ./...

clean:
	rm -rf gen
