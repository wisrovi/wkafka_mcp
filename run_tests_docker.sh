#!/bin/bash
# Run unit tests inside a Docker container for an isolated testing environment.
# This script uses python:3.13-slim to mount the source code, install dev dependencies, and execute pytest.

docker run --rm -v "$(pwd):/app" -w /app python:3.13-slim bash -c "
  pip install --upgrade pip &&
  pip install -e .[dev] &&
  pytest tests/
"
