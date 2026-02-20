# Testing Patterns

**Analysis Date:** 2026-02-21

## Test Framework

**Runner:** None — this project has no test suite.

As documented in `CLAUDE.md`:
> There are no tests in this repository.

No test files, test runners, or test configuration files exist anywhere in `src/`.

**Assertion Library:** Not applicable.

**Run Commands:** None defined.

## Test File Organization

**Location:** No test files exist.

**Naming:** No convention established.

## Test Structure

No tests exist. There are no `test_*.py`, `*_test.py`, or `*.spec.*` files in the repository.

## Mocking

No mocking framework is installed or used. `unittest.mock` and `pytest` are not in `pyproject.toml` dependencies or dev dependencies.

**Dev dependencies (`pyproject.toml`):**
```toml
[dependency-groups]
dev = [
  "ruff>=0.6,<0.12"
]
```
Only `ruff` is listed as a dev tool — no test framework.

## Fixtures and Factories

No test data fixtures or factory functions exist.

## Coverage

**Requirements:** None enforced.

No coverage configuration or tooling present.

## Test Types

**Unit Tests:** Not used.

**Integration Tests:** Not used.

**E2E Tests:** Not used.

## Verification Approaches Actually Used

The project uses several non-test mechanisms to verify correctness:

**Type checking (Pyright strict mode):**
- All source under `src/` checked with `pyright` in strict mode
- Configured in `pyproject.toml` under `[tool.pyright]`
- `assert` statements used for type narrowing (S101 suppressed)

**Static analysis (ruff):**
- Runs pyflakes, flake8-bugbear, flake8-builtins, flake8-import-conventions
- Command: `uv run ruff check src/`

**Runtime assertions:**
- `assert` statements throughout interpreter and type code for invariant checking
- Example in `src/aleo_types/basic.py`: overflow checks in `Int.__new__`
- Example in `src/interpreter/finalizer.py`: input count and type validation before execution

**Optional profiling instrumentation:**
- `@profile` decorator applied to hot paths via `line_profiler` (optional install)
- Falls back to a no-op pass-through when `line_profiler` not installed
- Used in: `src/db/base.py`, `src/db/insert.py`, `src/interpreter/finalizer.py`, `src/interpreter/interpreter.py`

**Debug mode:**
- `os.environ.get("DEBUG")` gates verbose output in several route handlers
- `BLOCK_TIMING` env var enables `BlockTimer` in `src/db/insert.py` for per-section timing

## Adding Tests (Guidance for Future Work)

If tests are to be introduced, the following context applies:

**Recommended framework:** `pytest` with `pytest-asyncio` (all core logic is async).

**Key challenges:**
- Heavy use of `from aleo_types import *` star imports requires the full type system to be importable
- `DatabaseBase` requires a live PostgreSQL connection — would need mocking or a test database
- `MappingCache` is a global singleton (`src/util/global_cache.py`) — requires careful reset between tests
- `aleo-explorer-rust` is a compiled Rust extension required at import time — test environment must have it built

**Recommended test targets (highest value):**
- `src/aleo_types/` — pure Python serialization/deserialization, no I/O dependencies, highly testable
- `src/disasm/` — pure functions converting binary types to strings
- `src/interpreter/instruction.py` and `src/interpreter/utils.py` — arithmetic/logic operations
- `src/webapi/utils.py` — pure utility functions (`get_relative_time`, `function_signature`, etc.)

---

*Testing analysis: 2026-02-21*
