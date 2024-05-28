VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-kinesis.version=${VERSION}'" -o conduit-connector-kinesis cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) -race ./...

.PHONY: test-integration
test-integration: up
	go test $(GOTEST_FLAGS) -v -race ./...; ret=$$?; \
		docker compose -f test/docker-compose.yml down; \
		exit $$ret

.PHONY: generate
generate:
	go generate ./...

.PHONY: install-paramgen
install-paramgen:
	go install github.com/conduitio/conduit-connector-sdk/cmd/paramgen@latest

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy

.PHONY: lint
lint:
	golangci-lint run -v

.PHONY: up
up:
	docker compose -f test/docker-compose.yml up --quiet-pull -d --wait 

.PHONY: down
down:
	docker compose -f test/docker-compose.yml down -v
