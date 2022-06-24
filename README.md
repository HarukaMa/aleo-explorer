# Aleo Explorer

[The "official" instance](https://explorer.hamp.app)

## Description

A blockchain explorer for Aleo.

It contains a node that connects to another peer to fetch blockchain data. This node is NOT validating blocks. Only
connect
to peers that you trust.

The node requires TCP port 14132 to be open, so it can accept the incoming connection from the other node.

There is a database scheme file for Postgres. Unfortunately, you'll need to read the SQL file and manually create the
tables for other databases. Note that the code uses `INSERT ... RETURNING`, so your choice of database software will
need
to support this.

The code smells rusty as I'm recreating all types of the Rust code.

*Maybe I should separate the node part as it's complicated enough*

## A better frontend?

Yeah, I'm not really a frontend developer. I know it's ugly, but I'm focusing on features here.

You are welcome to contribute a better frontend, as long as you keep the current one as a function baseline reference.
You can also create an entirely new frontend outside this repository; all you really need is the blockchain data in the
database.

## Special note to early adopters

You might need to update some database indexes to make them usable for the prefix search. Check commit 0000030.

## License

AGPL-3.0-or-later
