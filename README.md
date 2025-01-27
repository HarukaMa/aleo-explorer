# Aleo Explorer

[The "official" instance](https://aleoscan.io)

## Description

An open-source blockchain explorer for Aleo.

It contains a node that connects to another peer to fetch blockchain data. This node is NOT validating blocks. Only
connect to peers that you trust.

## `mainnet-newdb` branch

This branch uses Postgres for all data storage. Redis is no longer required, and (almost) full data history is stored in the database.  
You will need to resync from scratch if you are switching from the `mainnet` branch.

## Prerequisites

* Python 3.10 or 3.11.4+ (3.11.0 - 3.11.3 [won't work](https://github.com/python/cpython/pull/103514))
* Postgres 14+
* Rust 1.81+
* [aleo-explorer-rust](https://github.com/HarukaMa/aleo-explorer-rust)
* [aleo-explorer-wasm](https://github.com/HarukaMa/aleo-explorer-wasm) (optional for old frontend)
* (optional) [`setproctitle`](https://pypi.org/project/setproctitle/) for custom process names

## Usage

1. Import the database schema from `pg_dump.sql`.
2. Configure through `.env` file. See `.env.example` for reference.
3. Install `aleo-explorer-rust` to the current Python environment.
4. Compile `aleo-explorer-wasm`, install to `webui/static/rust{.js,_bg.wasm}` with `wasm-bindgen`. (not working right now)
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
