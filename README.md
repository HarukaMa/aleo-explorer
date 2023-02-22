# Aleo Explorer

[The "official" instance](https://explorer.hamp.app)

## Description

A blockchain explorer for Aleo.

It contains a node that connects to another peer to fetch blockchain data. This node is NOT validating blocks. Only
connect to peers that you trust.

## Database update warning

The database schema has been updated recently to support the latest blockchain feature (deployment). There was also a
slight change in the `Transaction` table. It should be possible to manually fix the database schema, but you can always
just resync the database from scratch.

No migration script is provided this time.

## Prerequisites

* Postgres 12+
* Rust latest stable
* [aleo-explorer-rust](https://github.com/HarukaMa/aleo-explorer-rust)

## Usage

1. Import the database schema from `pg_dump.sql`.
2. Configure through `.env` file. See `.env.example` for reference.
3. Run `main.py`.

## A better frontend?

Yeah, I'm not really a frontend developer. I know it's ugly, but I'm focusing on features here.

You are welcome to contribute a better frontend, as long as you keep the current one as a function baseline reference.
You can also create an entirely new frontend outside this repository; all you really need is the blockchain data in the
database.

## License

AGPL-3.0-or-later
