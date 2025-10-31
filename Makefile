.PHONY: help
help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: test
test: ## Run tests
	@echo "Running tests..."
	@go test -v ./...

.PHONY: test-race
test-race: ## Run tests with race detector
	@echo "Running tests with race detector..."
	@go test -v -race ./...

.PHONY: test-coverage
test-coverage: ## Run tests with coverage
	@echo "Running tests with coverage..."
	@go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

.PHONY: test-fuzz
test-fuzz: ## Run fuzz tests (30s each)
	@echo "Running fuzz tests..."
	@go test -fuzz='^FuzzConverter$$' -fuzztime=30s
	@go test -fuzz='^FuzzConverterWithFieldMappings$$' -fuzztime=30s
	@go test -fuzz='^FuzzQuoteIdentifier$$' -fuzztime=30s
	@go test -fuzz='^FuzzConverterLogicalOperators$$' -fuzztime=30s
	@go test -fuzz='^FuzzConverterComparisons$$' -fuzztime=30s

.PHONY: test-fuzz-long
test-fuzz-long: ## Run fuzz tests (5m each)
	@echo "Running fuzz tests (long)..."
	@go test -fuzz='^FuzzConverter$$' -fuzztime=5m
	@go test -fuzz='^FuzzConverterWithFieldMappings$$' -fuzztime=5m
	@go test -fuzz='^FuzzQuoteIdentifier$$' -fuzztime=5m
	@go test -fuzz='^FuzzConverterLogicalOperators$$' -fuzztime=5m
	@go test -fuzz='^FuzzConverterComparisons$$' -fuzztime=5m

.PHONY: lint
lint: ## Run golangci-lint
	@echo "Running linter..."
	@golangci-lint run --timeout=5m

.PHONY: lint-fix
lint-fix: ## Run golangci-lint with auto-fix
	@echo "Running linter with auto-fix..."
	@golangci-lint run --fix --timeout=5m

.PHONY: fmt
fmt: ## Format code with gofmt
	@echo "Formatting code..."
	@gofmt -s -w .

.PHONY: fmt-check
fmt-check: ## Check if code is formatted
	@echo "Checking code formatting..."
	@test -z "$$(gofmt -s -l . | tee /dev/stderr)" || (echo "Files not formatted correctly" && exit 1)

.PHONY: vet
vet: ## Run go vet
	@echo "Running go vet..."
	@go vet ./...

.PHONY: build
build: ## Build the package
	@echo "Building package..."
	@go build -v ./...

.PHONY: mod-download
mod-download: ## Download dependencies
	@echo "Downloading dependencies..."
	@go mod download

.PHONY: mod-tidy
mod-tidy: ## Tidy dependencies
	@echo "Tidying dependencies..."
	@go mod tidy

.PHONY: mod-verify
mod-verify: ## Verify dependencies
	@echo "Verifying dependencies..."
	@go mod verify

.PHONY: clean
clean: ## Clean build artifacts and test cache
	@echo "Cleaning..."
	@go clean -cache -testcache -modcache
	@rm -f coverage.out coverage.html

.PHONY: ci
ci: mod-verify fmt-check vet lint test-race ## Run all CI checks locally
	@echo "All CI checks passed!"

.PHONY: install-tools
install-tools: ## Install development tools
	@echo "Installing golangci-lint..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: check
check: fmt vet lint test ## Quick check (format, vet, lint, test)
	@echo "All checks passed!"

.DEFAULT_GOAL := help

