# Aleo Explorer (?)

## Description

Maybe one day this could become an explorer for Aleo. (But I can't really design UI so that day is far away)

Contains a node that connects to another peer to fetch blockchain data. This node is NOT validating blocks. Only connect
to peers that you trust.

The node requires TCP port 14132 to be open in order to accept incoming connection from the other node.

There is a database scheme file for Postgres. Unfortunately you'll need to read the sql file and manually create the
tables for other databases. Note that the code uses `INSERT ... RETURNING` so your choice of database software will need
to support this.

The code smells rusty as I'm recreating all types from Rust code.

*Maybe I should separate the node part as it's complicated enough*

## License

AGPL-3.0-or-later