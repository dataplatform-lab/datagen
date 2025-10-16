#!/usr/bin/env bash

set -e

echo "Running ruff..."
uv run ruff check --diff .

echo "Running pyright..."
uv run pyright --project .

echo "Running bandit..."
uv run bandit -c pyproject.toml -r src

echo "Running semgrep..."
uvx semgrep scan --error --config auto src

echo "Running trivy..."
uv run trivy fs -v --exit-code 1 --scanners vuln,misconfig,secret,license --severity HIGH,CRITICAL --skip-dirs .venv .

echo "Running gitleaks..."
gitleaks git -v
gitleaks dir -v
