GO_PROJECT_NAME := viperblock
SHELL := /bin/bash

# Quiet-mode filters (active when QUIET=1, set by preflight via recursive make)
# Note: grep pipelines use PIPESTATUS[0] so the exit status of `go test`
# propagates through the filter — otherwise a test failure is swallowed by
# grep's own (success) exit code and preflight prints "passed" on red.
ifdef QUIET
  _Q     = @
  _COVQ  = 2>&1 | { grep -Ev '^\s*(ok|PASS|\?|=== RUN|--- PASS:)\s' | grep -v 'coverage: 0\.0%' || true; }; exit $${PIPESTATUS[0]}
  _RACEQ = 2>&1 | { grep -Ev '^\s*(ok|PASS|\?|=== RUN|--- PASS:)\s' || true; }; exit $${PIPESTATUS[0]}
else
  _Q     =
  _COVQ  =
  _RACEQ =
endif

build:
	$(MAKE) go_build
	$(MAKE) go_build_nbd

# GO commands
go_build:
	@echo -e "\n....Building $(GO_PROJECT_NAME)"
	GOFIPS140=v1.0.0 go build -ldflags "-s -w" -o ./bin/sfs cmd/sfs/sfs.go
	GOFIPS140=v1.0.0 go build -ldflags "-s -w" -o ./bin/vblock cmd/vblock/main.go

go_build_nbd:
	@echo -e "\n....Building NBD plugin"
	GOFIPS140=v1.0.0 go build -o lib/nbdkit-viperblock-plugin.so -buildmode=c-shared nbd/viperblock.go

# Preflight — runs the same checks as GitHub Actions (lint + vuln + tests).
# Use this before committing to catch CI failures locally.
preflight:
	@$(MAKE) --no-print-directory QUIET=1 lint govulncheck test-cover diff-coverage test-race test-integration
	@echo -e "\n ✅ Preflight passed — safe to commit."

# Run unit tests
test:
	@echo -e "\n....Running tests for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -timeout 120s ./...

# Run unit tests with coverage profile
COVERPROFILE ?= coverage.out
test-cover:
	@echo -e "\n....Running tests with coverage for $(GO_PROJECT_NAME)...."
	$(_Q)LOG_IGNORE=1 go test -timeout 120s -coverprofile=$(COVERPROFILE) -covermode=atomic ./viperblock/... $(_COVQ)
	@scripts/check-coverage.sh $(COVERPROFILE) $(QUIET)

# Integration tier: tests that stand up a second VB engine over one volume, or
# drive real network-failure behaviour against a dead host. They are gated
# behind the `integration` build tag so the unit targets above stay inside
# their 120s budget; the tag is additive, so this target runs the unit tests
# too and needs a budget covering both.
test-integration:
	@echo -e "\n....Running integration tests for $(GO_PROJECT_NAME)...."
	$(_Q)LOG_IGNORE=1 go test -tags=integration -timeout 600s ./viperblock/... $(_RACEQ)

# Run unit tests with race detector
test-race:
	@echo -e "\n....Running tests with race detector for $(GO_PROJECT_NAME)...."
	$(_Q)LOG_IGNORE=1 go test -race -timeout 300s ./viperblock/... $(_RACEQ)

# Check that new/changed code meets coverage threshold (runs tests first)
diff-coverage: test-cover
	@QUIET=$(QUIET) scripts/diff-coverage.sh $(COVERPROFILE)

bench:
	@echo -e "\n....Running benchmarks for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -benchmem -run=. -bench=. ./...

run:
	$(MAKE) go_build

clean:
	rm -f ./bin/sfs
	rm -f ./bin/vblock
	rm -f ./lib/nbdkit-viperblock-plugin.so

# Lint all Go code via golangci-lint (replaces check-format, vet, gosec, staticcheck)
lint:
	@echo "Running golangci-lint..."
	$(_Q)golangci-lint run ./...
	@echo "  golangci-lint ok"

# Auto-fix all linter issues that have fixers
fix:
	golangci-lint run --fix ./...

# Govulncheck — dependency vulnerability scanning (not covered by golangci-lint)
govulncheck:
	@echo "Running govulncheck..."
	$(_Q)go tool govulncheck ./...
	@echo "  govulncheck ok"

.PHONY: build go_build go_build_nbd preflight test test-cover test-race test-integration diff-coverage bench run clean \
	lint fix govulncheck
