# Changelog

## [0.1.1] - 2026-08-13

### Fixed
- **Catalog fetch error handling**: `URLError`/`HTTPError` now imported from `urllib.error` (mypy-correct).
- Formatting normalized via ruff (pre-commit green).

## [0.1.0] - 2026-08-13

### Added
- **Unified CLI**: New `wkafka-mcp` command with subcommands: `run`, `start`, `stop`, `config`, and `help`.
- **Architect's Manual**: Built-in tool that provides AI agents with professional WKafka design guidelines.
- **Expert Blueprints Tool**: `get_wkafka_architect_blueprints` providing copy-pasteable professional code for producers, consumers, streams, and schemas.
- **Advanced Scaffolding**: `deploy_wkafka_scaffolding` deploys professional project structures.
- **Enhanced Catalog**: `PatternsCatalog` with real-time synchronization to GitHub registries and offline fallbacks.
- **Automated Installer**: `installer.sh` for easy environment setup and agent integration.
- **CLI Helper Text**: The `config` command dynamically detects the current Python executable and prints ready-to-use installation commands for Gemini CLI and Claude Desktop.
