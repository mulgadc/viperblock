GO_PROJECT_NAME := viperblock
SHELL := /bin/bash

build:
	$(MAKE) go_build
	$(MAKE) go_build_nbd

# GO commands
go_build:
	@echo -e "\n....Building $(GO_PROJECT_NAME)"
	go build -ldflags "-s -w" -o ./bin/sfs cmd/sfs/sfs.go
	go build -ldflags "-s -w" -o ./bin/vblock cmd/vblock/main.go

go_build_nbd:
	@echo -e "\n....Building NBD plugin"
	go build -o lib/nbdkit-viperblock-plugin.so -buildmode=c-shared nbd/viperblock.go

# Preflight — runs the same checks as GitHub Actions (format + lint + security + tests).
# Use this before committing to catch CI failures locally.
preflight: check-format vet security-check test
	@echo -e "\n ✅ Preflight passed — safe to commit."

# Run unit tests
test:
	@echo -e "\n....Running tests for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -v -timeout 300s ./...

bench:
	@echo -e "\n....Running benchmarks for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -benchmem -run=. -bench=. ./...

run:
	$(MAKE) go_build

clean:
	rm -f ./bin/sfs
	rm -f ./bin/vblock
	rm -f ./lib/nbdkit-viperblock-plugin.so

# Format all Go files in place
format:
	gofmt -w .

# Check that all Go files are formatted (CI-compatible, fails on diff)
check-format:
	@echo "Checking gofmt..."
	@UNFORMATTED=$$(gofmt -l .); \
	if [ -n "$$UNFORMATTED" ]; then \
		echo "Files not formatted:"; \
		echo "$$UNFORMATTED"; \
		echo "Run 'make format' to fix."; \
		exit 1; \
	fi
	@echo "  gofmt ok"

# Go vet (fails on issues, matches CI)
vet:
	@echo "Running go vet..."
	go vet ./...
	@echo "  go vet ok"

# Security checks — each tool fails the build on findings (matches CI).
# Reports are also saved to tests/ for review.
# Note: gosec excludes nbdkit dir due to cgo panic issue
security-check:
	@echo -e "\n....Running security checks for $(GO_PROJECT_NAME)...."
	set -o pipefail && go tool govulncheck ./... 2>&1 | tee tests/govulncheck-report.txt
	@echo "  govulncheck ok"
	set -o pipefail && go tool gosec -exclude=G104,G204,G304,G402 -exclude-dir nbd -exclude-generated ./... 2>&1 | tee tests/gosec-report.txt
	@echo "  gosec ok"
	set -o pipefail && go tool staticcheck -checks="all,-ST1000,-ST1003,-ST1016,-ST1020,-ST1021,-ST1022,-SA1019,-SA9005,-U1000,-SA4006,-SA6002" ./... 2>&1 | tee tests/staticcheck-report.txt
	@echo "  staticcheck ok"

.PHONY: build go_build go_build_nbd preflight test bench run clean \
	format check-format vet security-check
