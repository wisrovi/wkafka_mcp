# WKafka MCP Makefile
# Targets for development, testing, and publishing

.PHONY: help install test lint format build publish clean

# Default target
help:
	@echo "WKafka MCP Makefile targets:"
	@echo ""
	@echo "  install   Install package with dev dependencies"
	@echo "  test      Run pytest"
	@echo "  lint      Run ruff linter"
	@echo "  format    Run ruff formatter"
	@echo "  build     Build sdist + wheel into dist/"
	@echo "  publish   Build and upload to PyPI (needs ~/.pypirc)"
	@echo "  clean     Remove build artifacts"

# Install dependencies
install:
	pip install -e ".[dev]"

# Run tests
test:
	python -m pytest tests/ -v

# Lint code
lint:
	python -m ruff check src/ tests/

# Format code
format:
	python -m ruff format src/ tests/
	python -m ruff check src/ tests/ --fix

# Build distribution
build:
	python -m build

# Publish to PyPI
publish: clean build
	twine upload dist/*
	@echo "Published wkafka-mcp to PyPI."

# Clean build artifacts
clean:
	rm -rf build dist *.egg-info src/*.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
