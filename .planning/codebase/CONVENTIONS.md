# Coding Conventions

**Analysis Date:** 2026-02-21

## Naming Patterns

**Files:**
- Snake_case for all Python modules: `chain_routes.py`, `global_cache.py`, `vm_block.py`
- Suffix-based domain grouping: `*_routes.py` for route handlers, `vm_*.py` for VM types
- Prefixed private helpers with underscore: `_load_future`, `_get_block_header`, `_get_transition_from_dict`

**Classes:**
- PascalCase for all classes: `DatabaseBlock`, `MappingCache`, `ExecuteError`, `CJSONResponse`
- Mixin classes use `Database` prefix: `DatabaseAddress`, `DatabaseBlock`, `DatabaseInsert`
- Protocol classes in PascalCase: `Serializable`, `JSONSerialize`, `IntProtocol`
- Nested enum classes named `Type` inside the enclosing class: `Message.Type`, `NodeType`

**Functions:**
- Snake_case for all functions and methods: `get_block_by_height`, `execute_finalizer`, `format_aleo_credit`
- Route handlers suffixed with `_route`: `block_route`, `address_route`, `transaction_route`
- Private/static helpers prefixed with underscore: `_insert_future`, `_load_future`
- Async functions are prefixed with `async def` — no naming distinction from sync functions

**Variables:**
- Snake_case for all variables: `mapping_cache`, `latest_height`, `block_hash`
- Module-level singletons in PascalCase: `GlobalBlockTimer`, `MappingCache`
- Type aliases in PascalCase: `MappingCacheDict`, `CacheMappingContent`, `DictList`
- Short-lived loop variables use single letters or abbreviated names: `cur`, `res`, `f`, `v`, `k`

**Constants:**
- UPPER_SNAKE_CASE for module-level constants: `PING_SLEEP_IN_SECS`, `COST_TIMEOUT`, `COST_UNKNOWN`

**Type Variables:**
- Single uppercase letters for TypeVar: `T`, `R`, `P`, `L`
- Suffixed with `_co` for covariant: `I_co`

## Code Style

**Formatting:**
- Tool: `ruff` (version `>=0.6,<0.12`)
- Target: Python 3.10 syntax
- No explicit line length configured (ruff default: 88)
- Double quotes for strings (flake8-quotes `Q` rule enforced)

**Linting (ruff rules enabled):**
- `E4`, `E7`, `E9` — pycodestyle error classes
- `F` — pyflakes (with F403/F405 star-import warnings suppressed)
- `Q` — flake8-quotes (double quotes required)
- `B` — flake8-bugbear
- `A` — flake8-builtins (no shadowing builtins)
- `ICN` — flake8-import-conventions

**Notable suppressions:**
- `F403`/`F405` suppressed — star imports from `aleo_types` are intentional throughout
- `S101` suppressed — `assert` used for type narrowing
- `E701` suppressed — multiple statements per line allowed

**Type Checking:**
- Pyright strict mode (`typeCheckingMode = "strict"`)
- `reportUnusedImport = false` (unused imports allowed)
- `# type: ignore` and `# pyright: ignore [...]` used inline where strict mode produces false positives

## Import Organization

**Order (observed pattern, isort not enforced):**
1. Standard library (`import asyncio`, `import os`, `from io import BytesIO`)
2. Third-party packages (`import psycopg`, `from starlette.requests import Request`)
3. First-party / internal (`from aleo_types import *`, `from db import Database`)
4. Relative imports (`from .base import DatabaseBase`, `from .utils import *`)

**Star imports:**
- `from aleo_types import *` is the standard pattern in all files that work with blockchain types
- `from .traits import *`, `from .utils import *` used within `aleo_types` submodules
- Star imports are intentional and the F403/F405 rules are globally suppressed

**Aliasing:**
- `from explorer.types import Message as ExplorerMessage` — alias to avoid name collision
- `from asyncio import Future as AFuture` — alias to avoid collision with blockchain `Future` type

**Path aliases:**
- `extraPaths = ["src"]` in Pyright config — all imports are relative to `src/`, e.g. `from db import Database`

**Conditional imports for optional profiling:**
```python
try:
    from line_profiler import profile  # pyright: ignore [...]
except ImportError:
    P = ParamSpec('P')
    R = TypeVar('R')
    def profile(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await func(*args, **kwargs)
        return wrapper
```
This pattern appears in `src/db/base.py`, `src/interpreter/finalizer.py`, `src/interpreter/interpreter.py`, and `src/webui/chain_routes.py`.

**TYPE_CHECKING guard:**
- Forward references use `if TYPE_CHECKING:` blocks to avoid circular imports:
  ```python
  if TYPE_CHECKING:
      from db import Database
  ```

## Serializable Type Pattern

All Aleo types implement a consistent interface defined in `src/aleo_types/serialize.py`:
- `load(cls, data: BytesIO) -> Self` — deserialize from binary stream
- `loads(cls, data: str) -> Self` — deserialize from string (bech32, hex, etc.)
- `dump(self) -> bytes` — serialize to binary
- `json(self, compatible: bool = False) -> JSONType` — JSON representation

Constructor pattern uses keyword-only arguments throughout:
```python
def __init__(self, *, size: u64, log_size_of_group: u32, ...):
    self.size = size
    ...
```

## Error Handling

**Patterns:**

- **ValueError** for invalid input data: `raise ValueError("value {value} out of range")`
- **TypeError** for type mismatches: `raise TypeError("unsupported operand type")`
- **RuntimeError** for database inconsistencies: `raise RuntimeError("failed to insert row")`
- **NotImplementedError** for stubs/unfinished branches: `raise NotImplementedError`
- **Custom exception** `ExecuteError` in `src/interpreter/finalizer.py` for interpreter errors, carries original exception, instruction string, and transition context
- **Custom exception** `QuotaExceeded` in `src/middleware/api_quota.py` for quota enforcement
- **Custom exception** `Unreachable` in `src/util/typing_exc.py` for exhaustiveness checking

**Web layer pattern:**
- Route functions raise `starlette.exceptions.HTTPException` for client errors (400, 404)
- The `@htmx_template` decorator (`src/webui/template.py`) catches all other exceptions and converts to HTTP 550 with full frame context for debugging
- API routes return `CJSONResponse({"error": "..."}, status_code=4xx)` for client errors

**Database error handling:**
- DB connection errors sent via `message_callback` to the explorer message queue
- SQL results use walrus-operator pattern: `if (res := await cur.fetchone()) is None: raise RuntimeError(...)`

**Bare except:**
- `except:` (bare) used in a few places in `src/node/node.py` for network I/O failures — not best practice

**Top-level:**
- `src/explorer/explorer.py` `main_loop` wraps everything in `try/except Exception`, prints traceback, then re-raises

## Logging

**Framework:** `print()` to stdout (no structured logging library)

**Patterns:**
- Connection events: `print("node connected")`, `print("database connected")`
- Block processing: `print(f"adding block {block}")`
- Errors: `print("explorer error:", e)` followed by `traceback.print_exc()`
- ANSI color codes used in access log format strings in server files
- Debug output gated on `os.environ.get("DEBUG")` in route handlers

## Comments

**When to Comment:**
- Section separators using `##` multiple hashes (explicitly allowed by ruff `E266` suppression)
- `# noinspection PyUnboundLocalVariable` and similar PyCharm suppression comments used
- Inline `# type: ignore` and `# pyright: ignore [specific-code]` for false-positive suppressions
- `# TODO:` for known deficiencies (rare, see CONCERNS.md for list)
- Brief explanatory comments before non-obvious logic blocks

**Docstrings:**
- Not commonly used on application code
- Present in `src/aleo_types/serialize.py` for the protocol `__default_json` method
- `src/api/docs/` uses Sphinx for external API documentation

## Function Design

**Size:** Route handlers can be long (50–200 lines) when assembling complex response dicts. DB query methods are concise (10–40 lines each).

**Parameters:** Keyword-only (`*`) for data type constructors universally. Positional for route handlers (always single `request: Request` param). DB methods receive typed parameters.

**Return Values:**
- DB query methods return `Optional[T]` when the record may not exist
- Route handlers return `Response` subclasses or `tuple[dict, dict]` (context + headers) for template routes
- Serialization methods return `bytes` (`dump`) or `JSONType` (`json`)

## Module Design

**Exports:**
- `src/aleo_types/__init__.py` re-exports via `from .os_types import *` chain
- `src/db/__init__.py` assembles `Database` class from all mixin imports
- `src/webapi/webapi.py` and `src/webui/webui.py` define the `app` Starlette application object

**Barrel files:**
- `src/db/__init__.py` — single `Database` class assembled from mixins
- `src/aleo_types/__init__.py` — star-export chain through `os_types → vm_block → vm_instruction → vm_basic → generic → basic → traits → utils`

**Singleton pattern:**
- `MappingCache` in `src/util/global_cache.py` uses `__new__` override with `_instance` class attribute

**Decorator pattern:**
- `@public_cache_seconds(n)` in `src/webapi/utils.py` — adds Cache-Control headers
- `@htmx_template("template.jinja2")` in `src/webui/template.py` — handles HTMX vs full-page rendering and exception conversion
- `@profile` imported from `line_profiler` (or no-op fallback) — applied to hot paths in DB and interpreter

---

*Convention analysis: 2026-02-21*
