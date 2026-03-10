GO_PROJECT_NAME := viperblock
SHELL := /bin/bash

# Quiet-mode filters (active when QUIET=1, set by preflight via recursive make)
ifdef QUIET
  _Q     = @
  _COVQ  = 2>&1 | grep -Ev '^\s*(ok|PASS|\?|=== RUN|--- PASS:)\s' | grep -v 'coverage: 0\.0%' || true
  _RACEQ = 2>&1 | { grep -Ev '^\s*(ok|PASS|\?|=== RUN|--- PASS:)\s' || true; }; exit $${PIPESTATUS[0]}
  _SECQ  = >
else
  _Q     =
  _COVQ  = || true
  _RACEQ =
  _SECQ  = 2>&1 | tee
endif

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
preflight:
	@$(MAKE) --no-print-directory QUIET=1 check-format check-modernize vet security-check test-cover diff-coverage test-race
	@echo -e "\n ✅ Preflight passed — safe to commit."

# Run unit tests
test:
	@echo -e "\n....Running tests for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -timeout 120s ./...

# Run unit tests with coverage profile
# Note: go test may exit non-zero due to Go version mismatch in coverage instrumentation
# for packages without test files. We check actual test results + coverage threshold instead.
COVERPROFILE ?= coverage.out
test-cover:
	@echo -e "\n....Running tests with coverage for $(GO_PROJECT_NAME)...."
	$(_Q)LOG_IGNORE=1 go test -timeout 120s -coverprofile=$(COVERPROFILE) -covermode=atomic ./viperblock/... $(_COVQ)
	@scripts/check-coverage.sh $(COVERPROFILE) $(QUIET)

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
	$(_Q)go vet ./...
	@echo "  go vet ok"

# Security checks — each tool fails the build on findings (matches CI).
# Reports are also saved to tests/ for review.
# Note: gosec excludes nbdkit dir due to cgo panic issue
security-check:
	@echo -e "\n....Running security checks for $(GO_PROJECT_NAME)...."
	$(_Q)set -o pipefail && go tool govulncheck ./... $(_SECQ) tests/govulncheck-report.txt $(if $(QUIET),|| { cat tests/govulncheck-report.txt; exit 1; })
	@echo "  govulncheck ok"
	$(_Q)set -o pipefail && go tool gosec -exclude=G204,G304,G402,G117,G703,G705,G706 -exclude-dir nbd -exclude-generated ./... $(_SECQ) tests/gosec-report.txt $(if $(QUIET),|| { cat tests/gosec-report.txt; exit 1; })
	@echo "  gosec ok"
	$(_Q)set -o pipefail && go tool staticcheck -checks="all,-ST1000,-ST1003,-ST1016,-ST1020,-ST1021,-ST1022,-SA1019,-SA9005,-SA6002" ./... $(_SECQ) tests/staticcheck-report.txt $(if $(QUIET),|| { cat tests/staticcheck-report.txt; exit 1; })
	@echo "  staticcheck ok"

# Excluded: newexpr (replaces aws.String with new, not idiomatic for AWS SDK)
# Excluded: stringsbuilder (replaces string += in loops with strings.Builder, not worth the complexity for small loops)
GOFIX_EXCLUDE := -newexpr=false -stringsbuilder=false

modernize:
	@echo "Applying go fix modernizations..."
	go fix $(GOFIX_EXCLUDE) ./...
	@echo "  go fix applied"

check-modernize:
	@echo "Checking go fix modernizations..."
	@DIFF=$$(go fix $(GOFIX_EXCLUDE) -diff ./...); \
	if [ -n "$$DIFF" ]; then \
		echo "$$DIFF"; \
		echo "Run 'make modernize' to fix."; \
		exit 1; \
	fi
	@echo "  go fix ok"

.PHONY: build go_build go_build_nbd preflight test test-cover test-race diff-coverage bench run clean \
	format check-format check-modernize modernize vet security-check
