TEST_PKGS=$(shell go list ./... | grep -v cmd | grep -v models)

test:
	go test -race -p 1 -race -v $(TEST_PKGS)
build:
	go build -o bin/cdcingestor ./cmd/...
