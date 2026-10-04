BINARY := inflora-saruman
COMMAND := ./cmd/server

.PHONY: build test run lint tidy clean

build:
	mkdir -p bin
	go build -trimpath -o bin/$(BINARY) $(COMMAND)

test:
	go test -race ./...

run:
	set -a; . ./.env.example; set +a; go run $(COMMAND)

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	rm -rf bin

.PHONY: build-migrate migrate migrate-up migrate-down migrate-seed migrate-status

build-migrate:
	mkdir -p bin
	go build -trimpath -o bin/inflora-migrate ./cmd/migrate

migrate: migrate-up

migrate-up:
	go run ./cmd/migrate -dir up -target 1

migrate-down:
	go run ./cmd/migrate -dir down

migrate-seed:
	ENV=$${ENV:-dev} go run ./cmd/migrate -dir up

migrate-status:
	go run ./cmd/migrate -dir status
