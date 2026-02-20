# Architecture

**Analysis Date:** 2026-02-21

## Pattern Overview

**Overall:** Event-driven pipeline with async message-passing between a P2P sync layer, a persistence layer, and three independent HTTP servers.

**Key Characteristics:**
- Single-process, multi-task async Python (`asyncio`) with concurrent Starlette/Uvicorn ASGI servers
- Non-validating light node — syncs blocks from a trusted Aleo peer via TCP; does not validate proofs locally
- All state is persisted to PostgreSQL; in-memory caches (`MappingCache`, `global_program_cache`) are derived from DB data
- Database layer uses mixin-based composition: one `Database` class inherits from ~9 domain-specific mixin classes
- Block processing triggers local execution of program finalizers (the interpreter) to reproduce and verify mapping state changes

## Layers

**Entry Point / Process Bootstrap:**
- Purpose: Initialize the event loop, GC tuning, and the Explorer orchestrator
- Location: `src/main.py`
- Contains: `asyncio.run(main())`, `Explorer()` construction
- Depends on: `src/explorer/explorer.py`
- Used by: Nothing — this is the process root

**Explorer Orchestrator:**
- Purpose: Central coordinator that owns the message queue, starts all subsystems, and processes blocks
- Location: `src/explorer/explorer.py`
- Contains: `Explorer` class, startup sequencing, `add_block()`, `node_request()` dispatch
- Depends on: `Database`, `Node`, `webui`, `webapi`, `api`, `interpreter`, `MappingCache`
- Used by: `src/main.py`

**P2P Node Layer:**
- Purpose: TCP client that connects to a trusted Aleo node, performs handshake, and syncs block data
- Location: `src/node/node.py`
- Contains: `Node` class, handshake state machine, block-request/response handling, ping/pong loop
- Depends on: `aleo_types` (for protocol message types), `explorer.types` (for callback contracts)
- Used by: `Explorer` (constructs `Node`, passes `explorer_message` and `explorer_request` callbacks)

**Network Configuration:**
- Purpose: Network-specific parameters (genesis blocks, builtin programs, protocol constants)
- Location: `src/node/mainnet/param.py`, `src/node/testnet/param.py`, `src/node/canary/param.py`
- Contains: `Mainnet` / `Testnet` / `Canary` class with class attributes — genesis block bytes, builtin programs list, consensus upgrade heights
- Selected by: `src/node/__init__.py` via `NETWORK` env var; aliased as `Network` throughout the codebase

**Database Layer:**
- Purpose: All PostgreSQL persistence and query logic
- Location: `src/db/`
- Contains: `Database` (composed via mixin inheritance), `DatabaseBase` (pool management)
- Mixins and their domains:
  - `src/db/address.py` — address balance and history queries
  - `src/db/block.py` — block/header loading and reconstruction into aleo_types
  - `src/db/insert.py` — block ingestion (`save_block`), supply tracking, `BlockTimer`
  - `src/db/mapping.py` — on-chain mapping state read/write
  - `src/db/migrate.py` — sequential numbered DB migrations (run at startup)
  - `src/db/program.py` — program/edition storage and lookup
  - `src/db/search.py` — cross-entity search
  - `src/db/util.py` — shared query helpers
  - `src/db/validator.py` — validator committee and staking queries
- Depends on: `psycopg` async pool (`psycopg_pool.AsyncConnectionPool`), `aleo_types`
- Used by: `Explorer`, `webui`, `webapi`, `api` (each server creates its own `Database` instance)

**Aleo Type System:**
- Purpose: Pure-Python binary serialization/deserialization of all Aleo VM data structures
- Location: `src/aleo_types/`
- Contains: `Serializable` protocol (`load`/`dump`), `JSONSerialize` protocol, all blockchain types
- Key files:
  - `src/aleo_types/serialize.py` — base protocols (`Serializable`, `JSONSerialize`)
  - `src/aleo_types/basic.py` — primitive types (`u8`, `u16`, `u32`, `u64`, `u128`, `Field`, etc.)
  - `src/aleo_types/vm_block.py` — `Block`, `BlockHeader`, `Transaction`, `Transition`, etc.
  - `src/aleo_types/vm_instruction.py` — VM instruction types
  - `src/aleo_types/cached.py` — Rust-backed cached hash computations (`cached_get_mapping_id`, `cached_get_key_id`)
- Depends on: `aleo_explorer_rust` (Rust extension) for cryptographic operations
- Used by: Everything — imported via `from aleo_types import *`

**Interpreter Layer:**
- Purpose: Re-executes program finalizers locally to produce and verify mapping state changes during block ingestion
- Location: `src/interpreter/`
- Contains:
  - `src/interpreter/interpreter.py` — top-level `finalize_block()`, `finalize_deploy()`, `finalize_execute()`, `execute_operations()`
  - `src/interpreter/finalizer.py` — `execute_finalizer()`, `ExecuteError`, `MappingCache` read helpers
  - `src/interpreter/instruction.py` — individual Aleo VM instruction execution
  - `src/interpreter/environment.py` — `Registers` — VM register file for a single finalizer invocation
  - `src/interpreter/utils.py` — `FinalizeState`, operand/register helpers
- Depends on: `Database`, `MappingCache`, `aleo_types`, `disasm`
- Used by: `DatabaseInsert.save_block()` during block ingestion

**Disassembler:**
- Purpose: Converts binary Aleo program bytecode and types to human-readable `.aleo` text
- Location: `src/disasm/`
- Contains: `disasm_instruction()`, `disasm_command()`, register/type stringification helpers
- Depends on: `aleo_types`
- Used by: `interpreter` (for error messages), `webui`/`webapi` (for program display)

**Web Servers (three independent ASGI apps):**

- `src/webui/webui.py` — Server-rendered HTML frontend (port `PORT`, default 8000)
  - Starlette + Jinja2 templates with HTMX progressive enhancement
  - Middleware: `AccessLoggerMiddleware`, `HtmxMiddleware`, `MinifyMiddleware`, `ServerTimingMiddleware`
  - Templates: `src/webui/templates/` (full pages) and `src/webui/templates/htmx/` (HTMX partials)
  - Being deprecated in favour of an external SvelteKit frontend

- `src/webapi/webapi.py` — JSON API for external frontend (port `WEBAPI_PORT`, default 8002)
  - Serves the new [aleo-explorer-frontend](https://github.com/HarukaMa/aleo-explorer-frontend)
  - Middleware: `AccessLoggerMiddleware`, `ServerTimingMiddleware`, `AuthMiddleware` (token-gated)
  - Also instantiates a `LightNodeState` connection for live node queries

- `src/api/api.py` — Public utility API (port `API_PORT`, default 8001)
  - Versioned routes (`/v{version}/...`) for mappings, solutions, staking, finalize preview
  - Middleware: `AccessLoggerMiddleware`, `CORSMiddleware`, `ServerTimingMiddleware`, `APIQuotaMiddleware`, `APIFilterMiddleware`
  - Requires non-empty `User-Agent` header; enforces rate quotas

**Middleware Layer:**
- Location: `src/middleware/`
- Contains:
  - `src/middleware/asgi_logger/` — ASGI access log middleware (excluded from ruff)
  - `src/middleware/auth.py` — `AuthMiddleware` — token-based bearer auth
  - `src/middleware/htmx.py` — `HtmxMiddleware` — injects `HtmxData` into scope; adds `Vary: HX-Request`
  - `src/middleware/minify.py` — `MinifyMiddleware` — HTML minification for webui
  - `src/middleware/server_timing.py` — `ServerTimingMiddleware` — adds `Server-Timing` headers
  - `src/middleware/api_filter.py` — `APIFilterMiddleware` — rejects requests with empty `User-Agent`
  - `src/middleware/api_quota.py` — `APIQuotaMiddleware` — per-IP rate limiting

**Utilities:**
- Location: `src/util/`
- Contains:
  - `src/util/global_cache.py` — `MappingCache` (singleton), `MappingCacheMapping`, `global_program_cache` dict, `get_program()`
  - `src/util/cache.py` — generic LRU `Cache[KT, VT]` with TTL and max-size eviction
  - `src/util/set_proc_title.py` — process/thread title helpers
  - `src/util/arc0021.py`, `src/util/arc0137.py` — ARC standard helpers
  - `src/util/aleo_strings.py` — string utilities for Aleo identifiers

## Data Flow

**Block Ingestion (primary path):**

1. `Node.worker()` receives a `BlockResponse` frame from the TCP connection
2. `Node.parse_message()` calls `explorer_request(Request.ProcessBlock(block))`
3. `Explorer.node_request()` routes to `Explorer.add_block(block)`
4. `Explorer.add_block()` validates `previous_hash` linkage, then calls `Database.save_block(block)`
5. `DatabaseInsert.save_block()` opens a DB transaction; calls `finalize_block()` from interpreter
6. `interpreter.finalize_block()` iterates confirmed transactions: calls `finalize_deploy()` or `finalize_execute()` per transaction
7. Each finalizer re-executes VM instructions via `execute_finalizer()` using `Registers` environment
8. Mapping state updates are written via `execute_operations()` — `Database.update_mapping_key_value()` / `Database.remove_mapping_key_value()`
9. `MappingCache` singleton is updated in-memory alongside DB writes
10. On success, `Explorer.latest_height` and `latest_block_hash` are updated

**HTTP Request (webui/webapi/api):**

1. Uvicorn receives HTTP request → Starlette routes to handler function
2. Handler accesses `request.app.state.db` (a `Database` instance created at server startup)
3. Handler issues async DB queries → returns dict context (webui) or dict serialized to JSON (webapi/api)
4. webui renders via `@htmx_template` decorator → `Jinja2Templates.TemplateResponse()`; selects `htmx/` partial or full template based on `HX-Request` header

**Startup Sequence:**

1. `main()` → `Explorer()` → `explorer.start()` → `asyncio.create_task(main_loop())`
2. `main_loop()`: DB connect → migrate → `MappingCache` init → builtin programs → genesis check → dirty/revert checks → cache pre-populate
3. Three web servers launched as independent asyncio tasks (each gets its own `Database` instance)
4. `Node.connect()` starts P2P TCP worker task
5. `Explorer` enters message queue loop (handles `NodeConnected`, `NodeDisconnected`, `DatabaseError`, etc.)

**State Management:**

- `Explorer.latest_height` / `latest_block_hash` — in-memory, updated after each saved block
- `MappingCache` — process-wide singleton; pre-populated with `credits.aleo` committee/delegated/bonded mappings; lazily loads other mappings from DB on access
- `global_program_cache` — module-level dict; caches deserialized `Program` objects by `(program_id, edition)`
- All other state is authoritative in PostgreSQL

## Key Abstractions

**`Serializable` Protocol:**
- Purpose: Binary serialization interface for all Aleo types
- Examples: `src/aleo_types/serialize.py`
- Pattern: Every type implements `@classmethod load(cls, data: BytesIO) -> Self` and `def dump(self) -> bytes`

**`Message` / `ExplorerRequest`:**
- Purpose: Typed message-passing contract between `Node` and `Explorer`
- Examples: `src/explorer/types.py`
- Pattern: `Message` carries fire-and-forget events (node connected/disconnected); `ExplorerRequest` subclasses carry request-response calls from Node back into Explorer

**`DatabaseBase` + Mixin Composition:**
- Purpose: Domain-partitioned database access without duplication of connection pool
- Examples: `src/db/base.py`, `src/db/main.py`
- Pattern: `DatabaseBase` owns the `AsyncConnectionPool`; each mixin (`DatabaseBlock`, `DatabaseInsert`, etc.) inherits from it and adds domain methods; `Database` inherits from all mixins

**`@htmx_template` Decorator:**
- Purpose: Unified webui route wrapping — handles HTMX partial vs full template selection, exception → HTTP 550 conversion
- Examples: `src/webui/template.py`
- Pattern: `@htmx_template("block.jinja2")` wraps an async function that returns `(context_dict, headers_dict)`; decorator selects `htmx/block.jinja2` or `block.jinja2` based on `HX-Request` header

**`MappingCache` Singleton:**
- Purpose: Process-wide in-memory view of on-chain mapping state; avoids repeated DB lookups during block finalization
- Examples: `src/util/global_cache.py`
- Pattern: `MappingCache()` with no args returns the singleton; first call must pass `db=Database(...)`; `MappingCacheMapping` lazily fetches individual keys from DB

**`Network` Alias:**
- Purpose: Runtime network selection — makes all network-specific constants available as `Network.genesis_block`, `Network.version`, etc.
- Examples: `src/node/__init__.py`, `src/node/mainnet/param.py`
- Pattern: `NETWORK` env var selects `Mainnet`/`Testnet`/`Canary`; the chosen class is re-exported as `Network`

## Entry Points

**Process Entry:**
- Location: `src/main.py`
- Triggers: `python -m main` (working directory must be `src/`)
- Responsibilities: Apply `nest_asyncio`, load `.env`, tune GC, set proc title, construct `Explorer`, run event loop

**webui Application:**
- Location: `src/webui/webui.py` — `app = Starlette(...)`
- Triggers: `uvicorn.Config("webui:app", ...)` launched by `asyncio.create_task(webui.run())`
- Responsibilities: Serve HTML explorer UI on `HOST:PORT` (default 127.0.0.1:8000)

**webapi Application:**
- Location: `src/webapi/webapi.py` — `app = Starlette(...)`
- Triggers: `asyncio.create_task(webapi.run())`
- Responsibilities: JSON API for external frontend on `HOST:WEBAPI_PORT` (default 8002), token-authenticated

**api Application:**
- Location: `src/api/api.py` — `app = Starlette(...)`
- Triggers: `asyncio.create_task(api.run())`
- Responsibilities: Public versioned REST API on `API_HOST:API_PORT` (default 127.0.0.1:8001)

## Error Handling

**Strategy:** Fail-fast in the block ingestion path (exceptions propagate and crash the ingestion loop, requiring restart); HTTP servers return structured error responses and log tracebacks.

**Patterns:**
- `Explorer.main_loop()` wraps the entire startup+sync loop in `try/except Exception` — prints traceback and re-raises (process exits)
- `@htmx_template` decorator catches all exceptions from route handlers, converts them to HTTP 550 with a detailed message including filename, line number, and local variables
- `interpreter.ExecuteError` is a typed exception carrying `transition_id`, `instruction`, `program`, and `function_name` for precise failure attribution
- DB migrations run inside explicit `conn.transaction()` blocks; failure raises and triggers `DatabaseError` message to Explorer
- `MappingCache` validation: after each finalized block, operation counts and types are compared against chain-provided `expected_operations`; mismatch raises `TypeError` and resets the cache

## Cross-Cutting Concerns

**Logging:** `print()` throughout (no structured logging framework); ASGI access logs via `AccessLoggerMiddleware` per server with ANSI-coloured format strings

**Validation:** Block hash linkage checked in `Explorer.add_block()`; finalizer output validated operation-by-operation against `confirmed_transaction.finalize` in `interpreter.py`

**Authentication:** `AuthMiddleware` (token Bearer) on `webapi`; `APIFilterMiddleware` (User-Agent check) + `APIQuotaMiddleware` (rate limiting) on `api`; `webui` is unauthenticated

**Decimal precision:** Set globally to 80 significant figures in `main.py` via `decimal.getcontext().prec = 80`

**Rust extension:** `aleo_explorer_rust` provides performance-critical cryptographic hashing and program parsing; imported in `aleo_types/cached.py` and `node/mainnet/param.py`

---

*Architecture analysis: 2026-02-21*
