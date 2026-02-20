# Technology Stack

**Analysis Date:** 2026-02-21

## Languages

**Primary:**
- Python 3.10+ - All application code under `src/`; 3.11.0–3.11.3 have a known CPython bug and must be avoided

**Secondary:**
- Rust - Used exclusively via the `aleo-explorer-rust` native extension; no Rust source in this repo
- SQL - PostgreSQL schema defined in `pg_dump.sql`; queries written as raw psycopg strings throughout `src/db/`
- Jinja2 / HTML - Server-side templates in `src/webui/templates/`
- JavaScript - Minimal client-side code in `src/webui/static/base_script_v2.js` and `src/webui/static/constraint_worker.js`

## Runtime

**Environment:**
- CPython 3.10+ (asyncio-based, all I/O is async)
- GC tuning applied at startup in `src/main.py`: threshold raised to 200K allocations, generations scaled ×5

**Package Manager:**
- `uv` (preferred) — lockfile `uv.lock` is present but excluded from version control per `.gitignore`
- `pip` — used in Docker builds via `requirements.txt`
- Build backend: `hatchling>=1.20` (configured in `pyproject.toml`)

## Frameworks

**Core Web:**
- `starlette~=0.52.1` — ASGI application framework; all three servers (`webui`, `webapi`, `api`) are `Starlette` instances
- `uvicorn~=0.40.0` — ASGI server; each web server runs its own `uvicorn.Server` instance launched as an `asyncio.Task`

**Templating:**
- `jinja2~=3.1.5` — Server-side HTML rendering for the WebUI (`src/webui/templates/`); HTMX partial-page updates layered on top

**Database Client:**
- `psycopg[binary,pool]~=3.3.2` — Async PostgreSQL client with connection pooling; `AsyncConnectionPool` with max 16 connections per server instance (each server creates its own pool)

**HTTP Client:**
- `aiohttp==3.13.3` — Used for outbound HTTP calls (Cloudflare Turnstile verification, remote node height polling, light-node peer connections)
- `requests~=2.32.3` — Synchronous HTTP; used only in `src/node/light_node.py` to detect self IP at startup via `https://api.ipify.org`

**Build/Dev:**
- `ruff>=0.6,<0.12` — Linting (dev dependency)
- `pyright` (strict mode) — Type checking; configured in `pyproject.toml`
- `sphinx` + `sphinxcontrib-httpdomain` + `sphinx-book-theme` — Optional docs group for `src/api/docs/`

## Key Dependencies

**Critical:**
- `aleo-explorer-rust` — First-party Rust extension installed from `https://github.com/HarukaMa/aleo-explorer-rust.git`; provides bech32 encode/decode, field arithmetic, hash/commit ops, cryptographic signature verification, program compilation/parsing, solution ID generation, and more. Used in 11+ source files. Requires Rust 1.81+ toolchain to build.
- `psycopg[binary,pool]~=3.3.2` — Primary data store access; async pool used throughout all `src/db/` mixins
- `starlette~=0.52.1` — All three web server applications are built on Starlette
- `uvicorn~=0.40.0` — Runtime ASGI server for all three servers

**Infrastructure:**
- `python-dotenv~=1.2.1` — Loads `.env` file at startup in `src/main.py` and each web server module
- `minify-html~=0.18.1` — HTML minification applied by `MinifyMiddleware` (`src/middleware/minify.py`) to all WebUI responses in production (skipped when `DEBUG=1`)
- `asgiref~=3.11.1` — ASGI utilities
- `simplejson~=3.20.2` — JSON serialization (used in API responses for extended precision)
- `nest_asyncio~=1.6.0` — Applied in `src/main.py` to allow nested `asyncio` event loops
- `python_multipart~=0.0.20` — Multipart form parsing for file uploads (source code upload feature)

**Optional:**
- `rocksdb-py>=0.0.6` — Optional dependency (`rocksdb` extras group); used by `src/rdb/rdb.py` to sync directly from a local Aleo node's RocksDB ledger instead of via P2P. Activated by setting `SYNC_ROCKSDB` env var.

## Configuration

**Environment:**
- All configuration via environment variables loaded from `.env` at startup
- `.env.example` documents all supported variables
- Key variables:
  - `DB_HOST`, `DB_USER`, `DB_PASS`, `DB_DATABASE`, `DB_SCHEMA` — PostgreSQL connection
  - `P2P_NODE_HOST`, `P2P_NODE_PORT` — Trusted Aleo peer for block sync
  - `HOST` / `PORT` — WebUI server (default `127.0.0.1:8000`)
  - `WEBAPI_PORT` — WebAPI server (default `8002`)
  - `API_HOST` / `API_PORT` — Public API server (default `127.0.0.1:8001`)
  - `NETWORK` — Required; one of `mainnet`, `testnet`, `canary`; controls which `Network` class is imported in `src/node/__init__.py`
  - `DEV_MODE=1` — Use dev genesis block
  - `DEBUG=1` — Enables debug mode on Starlette apps; disables HTML minification
  - `SYNC_ROCKSDB` — Path to local RocksDB ledger; switches from P2P sync to local ledger read
  - `RPC_URL_ROOT` — Local snarkOS RPC URL for node height checks (e.g. `http://127.0.0.1:3033`)
  - `REF_RPC_URL_ROOT` — Reference node URL for height comparison (e.g. `https://api.explorer.aleo.org/v1`)
  - `TURNSTILE_SITE_KEY` / `TURNSTILE_SECRET_KEY` — Cloudflare Turnstile CAPTCHA keys
  - `WEBAPI_TOKEN` — Bearer token for WebAPI auth (`AuthMiddleware`)
  - `P2P_BLOCK_BATCH_SIZE` — Number of blocks to request per batch
  - `MAINTENANCE_INFO` — Optional maintenance banner message

**Build:**
- `pyproject.toml` — Project metadata, dependencies, uv sources, hatchling build config, ruff and pyright settings
- `requirements.txt` — Flat pip-compatible requirements list (used by Docker builds)
- `Dockerfile` / `alpine.Dockerfile` / `slim.Dockerfile` — Three container build variants

## Platform Requirements

**Development:**
- Python 3.10+ (not 3.11.0–3.11.3)
- Rust 1.81+ toolchain (for building `aleo-explorer-rust`)
- `uv` package manager recommended
- PostgreSQL 15+ with schema initialized from `pg_dump.sql`

**Production:**
- Docker (Alpine or Debian-slim images) or bare-metal Python
- PostgreSQL 15 (Alpine image used in `docker-compose.yml`)
- Network connectivity to a trusted Aleo node (mainnet default port 4130/4133)
- Optional: local snarkOS RocksDB for direct ledger sync

---

*Stack analysis: 2026-02-21*
