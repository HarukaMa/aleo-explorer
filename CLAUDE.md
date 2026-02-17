# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Aleo Explorer — an open-source blockchain explorer for the Aleo network. It runs a non-validating light node that connects to a trusted peer to fetch and index blockchain data into PostgreSQL. The active branch is `mainnet-newdb` (Postgres-only, no Redis).

## Build & Run Commands

```bash
# Install dependencies (requires Rust 1.81+ toolchain for aleo-explorer-rust)
uv sync

# Run the explorer
uv run python -m main

# Lint
uv run ruff check src/

# Type check (strict mode configured in pyproject.toml)
pyright

# Docker
docker-compose up -d
```

There are no tests in this repository.

## Configuration

Environment variables via `.env` file (see `.env.example`). Key vars:
- `DB_HOST`, `DB_USER`, `DB_PASS`, `DB_DATABASE`, `DB_SCHEMA` — PostgreSQL connection
- `P2P_NODE_HOST`, `P2P_NODE_PORT` — trusted Aleo node to sync from
- `HOST`/`PORT` — webui server (default 127.0.0.1:8000)
- `API_HOST`/`API_PORT` — API server
- `DEV_MODE=1` — uses dev genesis block
- `NETWORK` — network name (mainnet/testnet/canary)

Database schema is initialized from `pg_dump.sql`.

## Architecture

All source code is under `src/`. The entry point is `src/main.py` which creates an `Explorer` instance.

### Core Components

**Explorer** (`src/explorer/explorer.py`): Central orchestrator. Initializes the database, starts the node, and launches three web servers (webui, webapi, api) as concurrent async tasks. Processes blocks received from the node and manages the message queue between components.

**Node** (`src/node/node.py`): P2P light node that connects to a trusted Aleo peer via TCP, handles the handshake protocol, and syncs blocks. Network-specific parameters (genesis blocks, builtin programs) are in `src/node/{mainnet,testnet,canary}/param.py`.

**Database** (`src/db/main.py`): Uses mixin inheritance pattern — `Database` class inherits from `DatabaseAddress`, `DatabaseBlock`, `DatabaseInsert`, `DatabaseMapping`, `DatabaseMigrate`, `DatabaseProgram`, `DatabaseSearch`, `DatabaseUtil`, `DatabaseValidator`. All inherit from `DatabaseBase` (`src/db/base.py`) which manages the psycopg async connection pool. Each mixin file corresponds to a domain of database queries.

**Three web servers** (all Starlette + Uvicorn):
- `src/webui/` — Server-rendered HTML frontend (Jinja2 templates, being deprecated in favor of external frontend)
- `src/webapi/` — JSON API for the new external frontend ([aleo-explorer-frontend](https://github.com/HarukaMa/aleo-explorer-frontend))
- `src/api/` — Additional API endpoints (mappings, solutions, address staking, finalize preview)

**Aleo Types** (`src/aleo_types/`): Pure-Python serialization/deserialization of Aleo blockchain data types (blocks, transactions, transitions, VM instructions). Heavily uses star imports (`from aleo_types import *`).

**Interpreter** (`src/interpreter/`): Executes Aleo program finalizers locally to track mapping state changes during block processing.

**Disassembler** (`src/disasm/`): Converts binary Aleo program bytecode to human-readable `.aleo` format.

### Key Patterns

- Async throughout — all I/O uses `asyncio`, database via `psycopg` async pool
- Star imports from `aleo_types` are intentional and widespread (suppressed in ruff: F403, F405)
- `aleo-explorer-rust` is a required Rust extension (installed via uv from git) providing performance-critical operations
- Middleware stack includes: ASGI logging, server timing, HTMX support, HTML minification, API filtering/quota, auth
- `MappingCache` (`src/util/global_cache.py`) is a global singleton cache for on-chain mapping state

## Ruff Configuration

Target: Python 3.10. Checks: pyflakes, flake8-quotes, flake8-bugbear, flake8-builtins, flake8-import-conventions. Excludes `src/middleware/asgi_logger/`. See `pyproject.toml` for full config.

## Python Version

Requires Python 3.10+ (3.11.0-3.11.3 have a known CPython bug that breaks this project).
