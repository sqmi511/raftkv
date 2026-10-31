BINDIR  := bin
MODULE  := github.com/sqmi511/raftkv
VERSION ?= $(shell git describe --tags --always --dirty 2>NUL || echo dev)
LDFLAGS := -X $(MODULE)/internal/version.Version=$(VERSION)

.PHONY: all build test race vet fmt cover chaos bench clean

all: vet race build

build:
	go build -ldflags "$(LDFLAGS)" -o $(BINDIR)/raftkv-server ./cmd/server
	go build -ldflags "$(LDFLAGS)" -o $(BINDIR)/raftkv-cli ./cmd/cli
	go build -ldflags "$(LDFLAGS)" -o $(BINDIR)/raftkv-bench ./cmd/bench

test:
	go test ./...

# Requires CGO + a C compiler on Windows. CI always runs this.
race:
	go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	go fmt ./...

cover:
	go test -race -coverprofile=coverage.txt ./...
	go tool cover -func=coverage.txt

chaos:
	go test -race -count=1 -v -timeout 20m ./test/chaos

# No -race here: the race detector slows everything and would skew the numbers.
bench:
	go run ./cmd/bench

ifeq ($(OS),Windows_NT)
clean:
	powershell -NoProfile -Command "Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $(BINDIR), coverage.txt"
else
clean:
	rm -rf $(BINDIR) coverage.txt
endif