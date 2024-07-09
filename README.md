# Aleo Explorer

[The "official" instance](https://testnet.aleoscan.io)

## Description

An open-source blockchain explorer for Aleo.

It contains a node that connects to another peer to fetch blockchain data. This node is NOT validating blocks. Only
connect to peers that you trust.

## Prerequisites

* Python 3.10 or 3.11.4+ (3.11.0 - 3.11.3 [won't work](https://github.com/python/cpython/pull/103514))
* Postgres 12+ ~~(pg_hint_plan for best performance, `shared_preload_libraries` recommended)~~ not being used right now
* Redis not too old
* Rust latest stable
* [aleo-explorer-rust](https://github.com/HarukaMa/aleo-explorer-rust)
* [aleo-explorer-wasm](https://github.com/HarukaMa/aleo-explorer-wasm)
* (optional) [`setproctitle`](https://pypi.org/project/setproctitle/) for custom process names

## Usage

1. Import the database schema from `pg_dump.sql`.
2. Configure through `.env` file. See `.env.example` for reference.
3. Install `aleo-explorer-rust` to the current Python environment.
4. Compile `aleo-explorer-wasm`, install to `webui/static/rust{.js,_bg.wasm}` with `wasm-bindgen`. 
5. Run `main.py`.

### Use in docker

```bash
docker-compose up -d
```

## A better frontend?

A new frontend is being developed in [aleo-explorer-frontend](https://github.com/HarukaMa/aleo-explorer-frontend). You can preview it if you can find the deployment URL.

As such, the current frontend code contained in this repository (webui) is planned to be deprecated. The new frontend uses webapi to communicate with the backend. 

## License

AGPL-3.0-or-later
