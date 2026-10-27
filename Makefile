# Format all files
fmt:
	@echo "==> Formatting source"
	@go tool golangci-lint fmt ./...
	@echo "==> Done"
.PHONY: fmt

# Tidy the go.mod file
tidy:
	@echo "==> Cleaning go.mod"
	@go mod tidy
	@echo "==> Done"
.PHONY: tidy

# Run all tests
test:
	@go test -cover -race ./...
.PHONY: test

# Lint the project
lint:
	@go tool golangci-lint run ./...
.PHONY: lint

# Run CI tasks
ci: lint test
.PHONY: ci

# Last reflect2-based commit the golden encoder outputs are generated from
GOLDEN_REF ?= c6d1db8

# Regenerate golden encoder outputs from GOLDEN_REF
golden:
	@dir=$$(mktemp -d) && git worktree add -q --detach $$dir $(GOLDEN_REF) && \
		cp codec_golden_test.go $$dir/ && (cd $$dir && go test -run TestGolden -golden.update .) && \
		rm -f testdata/golden/*.avro && cp $$dir/testdata/golden/*.avro testdata/golden/; \
		ret=$$?; git worktree remove --force $$dir; exit $$ret
.PHONY: golden
