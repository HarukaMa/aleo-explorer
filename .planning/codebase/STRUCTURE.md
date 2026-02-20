# Codebase Structure

**Analysis Date:** 2026-02-21

## Directory Layout

```
aleo-explorer/
├── src/                        # All application source code
│   ├── main.py                 # Process entry point
│   ├── aleo_types/             # Aleo binary type system (serialization/deserialization)
│   ├── api/                    # Public versioned REST API (port 8001)
│   ├── db/                     # PostgreSQL database layer (mixin-composed)
│   ├── disasm/                 # Bytecode disassembler → human-readable .aleo text
│   ├── explorer/               # Central orchestrator (Explorer class + types)
│   ├── interpreter/            # Aleo VM finalizer interpreter
│   ├── middleware/             # ASGI middleware components
│   ├── node/                   # P2P light node (TCP client + network params)
│   │   ├── mainnet/            # Mainnet genesis block, builtin programs, constants
│   │   ├── testnet/            # Testnet-specific params
│   │   └── canary/             # Canary-specific params
│   ├── rdb/                    # RocksDB sync support (optional, for local node sync)
│   ├── util/                   # Shared utilities (cache, global state, helpers)
│   ├── webapi/                 # JSON API for external frontend (port 8002)
│   └── webui/                  # Server-rendered HTML frontend (port 8000)
│       ├── static/             # Static assets (CSS, JS, thirdparty libs)
│       │   └── thirdparty/     # Vendored frontend libraries
│       └── templates/          # Jinja2 HTML templates
│           └── htmx/           # HTMX partial templates (one per full-page template)
├── pg_dump.sql                 # PostgreSQL schema definition (run to initialize DB)
├── pyproject.toml              # Python project config (uv, ruff, pyright)
├── Dockerfile                  # Standard Docker build
├── alpine.Dockerfile           # Alpine-based Docker build
├── slim.Dockerfile             # Slim Docker build
├── docker-compose.yml          # Docker Compose for explorer + PostgreSQL
├── requirements.txt            # Pinned requirements (legacy; uv is primary)
├── uv.lock                     # uv lockfile (gitignored)
├── CLAUDE.md                   # AI assistant guidance for this repo
└── .env.example                # Environment variable template
```

## Directory Purposes

**`src/aleo_types/`:**
- Purpose: Complete binary serialization layer for Aleo blockchain data structures
- Contains: `Serializable` / `JSONSerialize` protocols, primitive types, all VM types (blocks, transactions, transitions, programs, instructions)
- Key files:
  - `src/aleo_types/__init__.py` — re-exports everything via `from .os_types import *` and `from .vm_block import *`
  - `src/aleo_types/serialize.py` — `Serializable` protocol with `load(BytesIO)` / `dump()` interface
  - `src/aleo_types/basic.py` — primitive Aleo types (`u8`, `u16`, `u32`, `u64`, `u128`, `Field`, `Group`, `Scalar`, etc.)
  - `src/aleo_types/vm_block.py` — `Block`, `BlockHeader`, `Transaction`, `Transition`, `Program`, and related types
  - `src/aleo_types/vm_instruction.py` — `Instruction`, all instruction literal types
  - `src/aleo_types/cached.py` — Rust-backed cached hash computations (`cached_get_mapping_id`, `cached_get_key_id`, `cached_compute_key_to_address`)
  - `src/aleo_types/traits.py` — shared type traits
  - `src/aleo_types/generic.py` — generic container types (`Vec`, `Option`, `Data`)
  - `src/aleo_types/os_types.py` — OS-level type imports

**`src/api/`:**
- Purpose: Public versioned REST API — mappings, staking, solutions, finalize preview
- Contains: Route handlers, `api.py` (Starlette app + Uvicorn runner), OpenAPI docs stub
- Key files:
  - `src/api/api.py` — app definition, route list, `startup()`, `run()`
  - `src/api/mapping_routes.py` — `/v{N}/mapping/*` endpoints
  - `src/api/address_routes.py` — `/v{N}/address/*` endpoints
  - `src/api/solution_routes.py` — `/v{N}/solution/*` endpoint
  - `src/api/execute_routes.py` — `/v{N}/simulate_execution/finalize` endpoint
  - `src/api/utils.py` — shared helpers (e.g., `get_remote_height`)
  - `src/api/docs/` — API documentation source files

**`src/db/`:**
- Purpose: All PostgreSQL interaction; mixin-composed single `Database` class
- Contains: `DatabaseBase` (pool), nine domain mixin classes, one SQL migration file
- Key files:
  - `src/db/main.py` — `Database` class (inherits all mixins)
  - `src/db/base.py` — `DatabaseBase` — `AsyncConnectionPool` setup
  - `src/db/insert.py` — `save_block()`, `save_unconfirmed_transaction()`, `_SupplyTracker`, `BlockTimer`
  - `src/db/block.py` — block/header/transaction reconstruction from DB rows
  - `src/db/mapping.py` — mapping CRUD (`update_mapping_key_value`, `remove_mapping_key_value`, `get_mapping_cache`)
  - `src/db/migrate.py` — numbered sequential migrations (called at startup)
  - `src/db/migrate_5.sql` — SQL for migration step 5
  - `src/db/program.py` — program/edition storage, function definitions
  - `src/db/address.py` — address transaction history, balance queries
  - `src/db/search.py` — cross-entity full-text search
  - `src/db/util.py` — shared query helpers
  - `src/db/validator.py` — committee, bonded, delegated staking queries

**`src/disasm/`:**
- Purpose: Convert binary Aleo types to human-readable `.aleo` assembly text
- Contains: Disassembly functions for instructions, commands, types, registers
- Key files:
  - `src/disasm/aleo.py` — `disasm_instruction()`, `disasm_command()`, type/register stringifiers
  - `src/disasm/utils.py` — shared helpers (`value_type_to_mode_type_str`, `plaintext_type_to_str`)

**`src/explorer/`:**
- Purpose: Central process orchestrator
- Contains: `Explorer` class, message/request type definitions
- Key files:
  - `src/explorer/explorer.py` — `Explorer` class with `main_loop()`, `add_block()`, `node_request()`
  - `src/explorer/types.py` — `Message`, `ExplorerRequest`, all `Request.*` subclasses

**`src/interpreter/`:**
- Purpose: Re-execute Aleo VM finalizer logic for block ingestion
- Contains: Finalizer orchestration, VM instruction execution, register environment, state utilities
- Key files:
  - `src/interpreter/interpreter.py` — `finalize_block()`, `finalize_deploy()`, `finalize_execute()`, `execute_operations()`, `init_builtin_program()`
  - `src/interpreter/finalizer.py` — `execute_finalizer()`, `ExecuteError`, `mapping_cache_read()`
  - `src/interpreter/instruction.py` — `execute_instruction()` — per-instruction VM execution
  - `src/interpreter/environment.py` — `Registers` — VM register file
  - `src/interpreter/utils.py` — `FinalizeState`, operand/register load helpers

**`src/middleware/`:**
- Purpose: Reusable ASGI middleware components applied per-server
- Contains: Auth, HTMX detection, HTML minification, server timing, API rate limiting, access logging
- Key files:
  - `src/middleware/asgi_logger/` — third-party access log middleware (excluded from ruff linting)
  - `src/middleware/auth.py` — `AuthMiddleware` — Bearer token validation
  - `src/middleware/htmx.py` — `HtmxMiddleware` — injects `HtmxData` into scope
  - `src/middleware/minify.py` — `MinifyMiddleware` — HTML minification
  - `src/middleware/server_timing.py` — `ServerTimingMiddleware` — `Server-Timing` header
  - `src/middleware/api_filter.py` — `APIFilterMiddleware` — reject empty `User-Agent`
  - `src/middleware/api_quota.py` — `APIQuotaMiddleware` — per-IP rate limiting

**`src/node/`:**
- Purpose: P2P TCP node client + network-specific configuration
- Contains: `Node` class, network parameter classes
- Key files:
  - `src/node/node.py` — `Node` — handshake state machine, message parsing, block sync loop
  - `src/node/__init__.py` — selects `Network` alias based on `NETWORK` env var
  - `src/node/mainnet/param.py` — `Mainnet` class with genesis blocks, builtin programs, consensus heights
  - `src/node/mainnet/block.genesis` — mainnet genesis block binary
  - `src/node/mainnet/dev.genesis` — dev genesis block binary
  - `src/node/mainnet/credits.aleo` — credits program source (loaded as builtin)
  - `src/node/mainnet/credits_v1.aleo` — credits v1 program source
  - `src/node/testnet/` — testnet equivalents
  - `src/node/canary/` — canary equivalents

**`src/rdb/`:**
- Purpose: Optional RocksDB sync path (alternative to P2P node sync for local node operators)
- Contains: `RocksDB` wrapper
- Key files:
  - `src/rdb/rdb.py` — `RocksDB` class (requires `rocksdbpy` optional dependency)

**`src/util/`:**
- Purpose: Shared utilities used across the codebase
- Contains: Caches, global state, ARC standard helpers, string utilities
- Key files:
  - `src/util/global_cache.py` — `MappingCache` singleton, `MappingCacheMapping`, `global_program_cache`, `get_program()`
  - `src/util/cache.py` — generic `Cache[KT, VT]` with LRU eviction and TTL
  - `src/util/set_proc_title.py` — `set_proc_title()`, `set_thread_title()`
  - `src/util/arc0021.py` — ARC-0021 (Aleo Name Service) helpers
  - `src/util/arc0137.py` — ARC-0137 helpers
  - `src/util/aleo_strings.py` — Aleo identifier string utilities
  - `src/util/typing_exc.py` — typing exception helpers

**`src/webapi/`:**
- Purpose: JSON API backend for the external SvelteKit frontend
- Contains: Route handlers, Starlette app definition
- Key files:
  - `src/webapi/webapi.py` — app definition, `startup()`, `run()`
  - `src/webapi/chain_routes.py` — block, transaction, validator, search, summary routes
  - `src/webapi/address_routes.py` — address and ANS routes
  - `src/webapi/program_routes.py` — program listing and detail routes
  - `src/webapi/error_routes.py` — 400/404/550 error handlers
  - `src/webapi/utils.py` — `CJSONResponse`, `public_cache_seconds`, `out_of_sync_check`

**`src/webui/`:**
- Purpose: Server-rendered HTML explorer UI (being deprecated)
- Contains: Route handlers, Jinja2 templates, static files, template engine setup
- Key files:
  - `src/webui/webui.py` — Starlette app definition, route list, `startup()`, `run()`
  - `src/webui/template.py` — `@htmx_template` decorator, `Jinja2Templates` instance, Jinja2 custom filters
  - `src/webui/chain_routes.py` — block, transaction, transition, validator, search routes
  - `src/webui/program_routes.py` — program routes
  - `src/webui/proving_routes.py` — calc, incentive, address routes
  - `src/webui/classes.py` — shared dataclasses for template context
  - `src/webui/utils.py` — `out_of_sync_check`, `get_relative_time`, `function_signature`
  - `src/webui/templates/base.jinja2` — HTML base layout template
  - `src/webui/templates/macros.jinja2` — shared Jinja2 macros
  - `src/webui/templates/htmx/` — HTMX partial templates (mirror of full templates minus layout)
  - `src/webui/static/` — CSS, JS, favicon
  - `src/webui/static/thirdparty/` — vendored frontend libraries

## Key File Locations

**Entry Points:**
- `src/main.py` — process entry; run as `uv run python -m main`
- `src/explorer/explorer.py` — `Explorer` class (core orchestrator)
- `src/node/node.py` — `Node` class (P2P sync)

**Configuration:**
- `.env.example` — template for required environment variables
- `pyproject.toml` — uv dependencies, ruff linting config, pyright settings
- `src/node/__init__.py` — network selection logic
- `src/node/mainnet/param.py` — mainnet constants (consensus upgrade heights, ANS registry, etc.)

**Database:**
- `pg_dump.sql` — full schema definition (run this to initialize a new DB)
- `src/db/main.py` — `Database` class entry point
- `src/db/insert.py` — block ingestion logic (most complex DB file)
- `src/db/migrate.py` — migration runner

**HTTP Applications:**
- `src/webui/webui.py` — webui Starlette app
- `src/webapi/webapi.py` — webapi Starlette app
- `src/api/api.py` — api Starlette app

**Type System:**
- `src/aleo_types/serialize.py` — `Serializable` base protocol
- `src/aleo_types/vm_block.py` — all top-level blockchain types

**Global State:**
- `src/util/global_cache.py` — `MappingCache` singleton, `global_program_cache`

## Naming Conventions

**Files:**
- Snake_case for all Python source files: `global_cache.py`, `chain_routes.py`, `api_filter.py`
- Domain-prefixed DB mixin files: `address.py`, `block.py`, `insert.py`, `mapping.py` under `src/db/`
- Route files suffixed with `_routes.py`: `chain_routes.py`, `address_routes.py`, `program_routes.py`
- Network param files named `param.py` inside each network subdirectory

**Classes:**
- PascalCase: `Explorer`, `Node`, `Database`, `DatabaseBase`, `MappingCache`, `BlockTimer`
- DB mixins: `DatabaseAddress`, `DatabaseBlock`, `DatabaseInsert`, etc.
- Aleo types: match Aleo naming (e.g., `BlockHeader`, `TransitionOutput`, `ConfirmedTransaction`)
- Middleware: `{Name}Middleware` (e.g., `AuthMiddleware`, `HtmxMiddleware`)

**Functions:**
- Snake_case: `finalize_block()`, `execute_finalizer()`, `save_block()`, `get_latest_height()`
- DB async methods prefixed with action: `get_*`, `save_*`, `initialize_*`, `update_*`, `remove_*`
- Route handlers suffixed `_route`: `block_route`, `address_route`, `programs_route`

**Templates:**
- Lowercase with underscores: `block.jinja2`, `search_result.jinja2`, `similar_programs.jinja2`
- HTMX partials mirror full-page names under `htmx/` subdirectory

## Where to Add New Code

**New blockchain data type:**
- Implement `load(BytesIO) -> Self` and `dump() -> bytes` following `Serializable`
- Place in `src/aleo_types/vm_block.py` (if block-level) or `src/aleo_types/vm_instruction.py` (if instruction-level)
- Re-export via `src/aleo_types/__init__.py` if needed

**New database query:**
- Identify correct domain mixin (`DatabaseBlock` for block queries, `DatabaseAddress` for address queries, etc.)
- Add async method to the appropriate mixin file in `src/db/`
- No changes to `src/db/main.py` needed (inherited automatically)

**New DB migration:**
- Add a new numbered tuple to the `migrations` list in `DatabaseMigrate.migrate()` in `src/db/migrate.py`
- Implement the migration as a `@staticmethod async def migration_N_*(conn)` method on `DatabaseMigrate`

**New webapi endpoint:**
- Add route handler to the appropriate `src/webapi/*_routes.py` file (or create a new `_routes.py`)
- Register the `Route(...)` in the `routes` list in `src/webapi/webapi.py`

**New webui page:**
- Add `@htmx_template("my_page.jinja2")` handler in appropriate `src/webui/*_routes.py`
- Create `src/webui/templates/my_page.jinja2` (full page extending `base.jinja2`)
- Create `src/webui/templates/htmx/my_page.jinja2` (HTMX partial, content only)
- Register route in `src/webui/webui.py`

**New api endpoint:**
- Add handler to appropriate `src/api/*_routes.py`
- Register versioned `Route("/v{version:int}/my_endpoint", ...)` in `src/api/api.py`

**New middleware:**
- Implement ASGI middleware class in `src/middleware/my_middleware.py`
- Add `Middleware(MyMiddleware, ...)` to the appropriate server's `middleware=[]` list

**New network (e.g., testnet2):**
- Create `src/node/testnet2/` with `param.py`, `__init__.py`, `block.genesis`, `dev.genesis`, and program files
- Add branch in `src/node/__init__.py`

## Special Directories

**`src/middleware/asgi_logger/`:**
- Purpose: Third-party ASGI access logging middleware (vendored or installed)
- Generated: No
- Committed: Yes
- Note: Excluded from ruff linting in `pyproject.toml`

**`src/webui/templates/htmx/`:**
- Purpose: HTMX partial templates — same content as full templates but without the `base.jinja2` layout wrapper
- Generated: No (manually maintained)
- Committed: Yes

**`src/webui/static/thirdparty/`:**
- Purpose: Vendored frontend JavaScript/CSS libraries
- Generated: No
- Committed: Yes

**`.planning/`:**
- Purpose: GSD planning documents for AI-assisted development
- Generated: By GSD commands
- Committed: No (in `.gitignore` as `??`)

---

*Structure analysis: 2026-02-21*
