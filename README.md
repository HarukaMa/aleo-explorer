# Aleo Explorer

[The "official" instance](https://testnet3.aleoscan.io)

## Description

A blockchain explorer for Aleo.

It contains a node that connects to another peer to fetch blockchain data. This node is NOT validating blocks. Only
connect to peers that you trust.

## Prerequisites

* Python 3.10 or 3.11.4+ (3.11.0 - 3.11.3 [won't work](https://github.com/python/cpython/pull/103514))
* Postgres 12+ (pg_hint_plan for best performance, `shared_preload_libraries` recommended)
* Redis not too old
* Rust latest stable
* [aleo-explorer-rust](https://github.com/HarukaMa/aleo-explorer-rust)
* [aleo-explorer-wasm](https://github.com/HarukaMa/aleo-explorer-wasm)
* (optional) [`setproctitle`](https://pypi.org/project/setproctitle/) for custom process names

## Usage

1. Import the database schema from `pg_dump.sql`.
1. Configure through `.env` file. See `.env.example` for reference.
1. Install `aleo-explorer-rust` to the current Python environment.
1. Compile `aleo-explorer-wasm`, install to `webui/static/rust{.js,_bg.wasm}` with `wasm-bindgen`. 
1. Run `main.py`.

### Use in docker

```bash
docker-compose up -d
```

(currently lacking pg_hint_plan)

## A better frontend?

Yeah, I'm not really a frontend developer. I know it's ugly, but I'm focusing on features here.

You are welcome to contribute a better frontend, as long as you keep the current one as a function baseline reference.
You can also create an entirely new frontend outside this repository; all you really need is the blockchain data in the
database.

## License

AGPL-3.0-or-later
