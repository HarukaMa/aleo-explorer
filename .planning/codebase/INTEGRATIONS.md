# External Integrations

**Analysis Date:** 2026-02-21

## APIs & External Services

**Aleo Network (P2P):**
- Trusted Aleo peer node — Raw TCP P2P sync of blockchain blocks
  - Protocol: Custom binary framing (4-byte LE length prefix + frame body)
  - Implementation: `src/node/node.py` (`Node` class)
  - Config: `P2P_NODE_HOST`, `P2P_NODE_PORT`
  - Handshake: `ChallengeRequest` / `ChallengeResponse` messages; node identifies as `NodeType.Prover`

**Aleo RPC (HTTP):**
- Local snarkOS REST API — Used to query current node block height for sync-check
  - Endpoint pattern: `{RPC_URL_ROOT}/{NETWORK}/block/height/latest`
  - Client: `aiohttp.ClientSession` (1-second timeout)
  - Config: `RPC_URL_ROOT` env var (e.g., `http://127.0.0.1:3033`)
  - Used in: `src/api/utils.py` (`get_remote_height`)

**Aleo Reference Explorer API:**
- External reference height check — Compares local index height against a reference node
  - Endpoint pattern: `{REF_RPC_URL_ROOT}/{NETWORK}/block/height/latest`
  - Config: `REF_RPC_URL_ROOT` env var (default `https://api.explorer.aleo.org/v1`)
  - Used in: `src/api/api.py` (`status_route`)

**Cloudflare Turnstile (CAPTCHA):**
- Feedback form spam protection on the WebUI
  - Verification endpoint: `https://challenges.cloudflare.com/turnstile/v0/siteverify`
  - Method: `POST` with `secret` + `response` fields
  - Client: `aiohttp.ClientSession` (created per-request)
  - Config: `TURNSTILE_SITE_KEY` (rendered in template), `TURNSTILE_SECRET_KEY` (server-side verification)
  - Implementation: `src/webui/webui.py` (`submit_feedback_route`)

**ipify (Public IP Detection):**
- Detects the explorer's own public IP to prevent self-connection loops in the light node
  - Endpoint: `https://api.ipify.org/?format=json`
  - Client: `requests` (synchronous, called once at startup)
  - Implementation: `src/node/light_node.py` (`LightNodeState.__init__`)

**Light Node Peer REST API (optional):**
- Connects to individual Aleo peer nodes to fetch peer/node lists for display
  - Endpoint: `http://{ip}:3030` (aiohttp session base URL)
  - Used in: `src/node/light_node.py` (`LightNode`)
  - Referenced in: `src/webui/chain_routes.py` (nodes route, port 3030)

## Data Storage

**Databases:**
- PostgreSQL 15
  - Connection: `DB_HOST`, `DB_USER`, `DB_PASS`, `DB_DATABASE`, `DB_SCHEMA` env vars
  - Client: `psycopg[binary,pool]~=3.3.2` async connection pool (`AsyncConnectionPool`, max 16 connections)
  - Pool connection string format: `host=... user=... password=... dbname=... options=-csearch_path={schema} application_name=aleo-explorer-{NETWORK}`
  - Schema init: `pg_dump.sql` (loaded via Docker entrypoint or manually)
  - Migrations: version-tracked in `_migration` table; run at startup via `DatabaseMigrate.migrate()` in `src/db/migrate.py`
  - Each web server thread opens its own independent connection pool
  - Implementation: `src/db/base.py` (`DatabaseBase`); domain mixins in `src/db/address.py`, `src/db/block.py`, `src/db/insert.py`, `src/db/mapping.py`, `src/db/migrate.py`, `src/db/program.py`, `src/db/search.py`, `src/db/util.py`, `src/db/validator.py`

**File Storage:**
- Local filesystem — Genesis block files (`block.genesis`, `dev.genesis`) stored as binary files alongside network param modules in `src/node/{mainnet,testnet,canary}/`
- Optional: Local snarkOS RocksDB ledger (read-only, secondary mode) for direct block sync without P2P; activated by `SYNC_ROCKSDB` env var pointing to the RocksDB directory. Implementation: `src/rdb/rdb.py` (requires optional `rocksdb-py` package)

**Caching:**
- In-process memory only — no Redis or Memcached
  - `MappingCache` singleton (`src/util/global_cache.py`): Caches on-chain mapping key-value state for `credits.aleo` (committee, delegated, bonded mappings) pre-populated at startup; other mappings loaded lazily from DB
  - `global_program_cache` dict (`src/util/global_cache.py`): Caches deserialized `Program` objects by `(program_id, edition)`
  - `Cache` LRU class (`src/util/cache.py`): Generic LRU cache with configurable TTL and size; used by `src/api/api.py` (`app.state.program_cache`)

## Authentication & Identity

**WebAPI Auth (internal):**
- Token-based bearer authentication on the WebAPI server (`src/webapi/webapi.py`)
  - Middleware: `AuthMiddleware` (`src/middleware/auth.py`)
  - Header: `Authorization: Token {token}`
  - Config: `WEBAPI_TOKEN` env var (empty string disables auth effectively)
  - The WebAPI is intended for use by the external frontend ([aleo-explorer-frontend](https://github.com/HarukaMa/aleo-explorer-frontend))

**CAPTCHA (public WebUI):**
- Cloudflare Turnstile on the feedback form
  - Config: `TURNSTILE_SITE_KEY` / `TURNSTILE_SECRET_KEY` (set to `0x0` to disable in dev)

**No user authentication system** — The explorer has no login/session system; all pages are public read-only.

## Monitoring & Observability

**Error Tracking:**
- None — Errors are printed to stdout/stderr via `print()` and `traceback.print_exc()`

**Logs:**
- Access logs: Custom ASGI access logger middleware (`src/middleware/asgi_logger/`) with colored terminal output; separate log format strings per server (WebUI, WebAPI, API)
- Application logs: `print()` statements throughout; no structured logging framework
- Server timing: `ServerTimingMiddleware` (`src/middleware/server_timing.py`) adds `Server-Timing` response headers

## CI/CD & Deployment

**Hosting:**
- Self-hosted or any Docker-capable environment

**CI Pipeline:**
- GitHub Actions (`.github/` directory present) — contents not examined; no test suite exists

**Docker:**
- Three Dockerfiles:
  - `Dockerfile` — Debian-slim based, installs Rust at build time, pulls `aleo-explorer-rust` from GitHub
  - `alpine.Dockerfile` — Multi-stage Alpine build; separate builder stage compiles wheels, runtime stage is minimal
  - `slim.Dockerfile` — Additional slim variant
- `docker-compose.yml` — Starts `postgres:15-alpine` and the explorer container; mounts `pg_dump.sql` as init SQL
- Exposed ports: `8800` (WebUI), `8801` (API) in docker-compose default config

## Webhooks & Callbacks

**Incoming:**
- None — The explorer does not expose webhook endpoints

**Outgoing:**
- None — The explorer does not send webhook calls

## Environment Configuration

**Required env vars:**
- `NETWORK` — `mainnet`, `testnet`, or `canary`
- `DB_HOST`, `DB_USER`, `DB_PASS`, `DB_DATABASE`, `DB_SCHEMA` — PostgreSQL
- `P2P_NODE_HOST`, `P2P_NODE_PORT` — Aleo peer node

**Optional but commonly set:**
- `HOST`, `PORT` — WebUI bind address (default `127.0.0.1:8000`)
- `API_HOST`, `API_PORT` — Public API bind (default `127.0.0.1:8001`)
- `WEBAPI_PORT` — Internal WebAPI bind (default `8002`)
- `WEBAPI_TOKEN` — Bearer token for WebAPI
- `RPC_URL_ROOT` — Local snarkOS REST URL
- `REF_RPC_URL_ROOT` — Reference explorer API URL
- `TURNSTILE_SITE_KEY` / `TURNSTILE_SECRET_KEY` — Cloudflare CAPTCHA
- `DEV_MODE=1` — Use dev genesis block
- `DEBUG=1` — Debug mode
- `P2P_BLOCK_BATCH_SIZE` — Block request batch size
- `SYNC_ROCKSDB` — Path for local RocksDB sync mode
- `MAINTENANCE_INFO` — Maintenance banner text

**Secrets location:**
- `.env` file in project root (not committed; `.env.example` provides template)

---

*Integration audit: 2026-02-21*
