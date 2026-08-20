#!/bin/bash
# Calculate code coverage for wkafka_mcp using pytest-cov
pytest --cov=wkafka_mcp tests/ --cov-report=term-missing
