from __future__ import annotations

from typing import Awaitable

import psycopg
import psycopg.sql

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase
from .block import DatabaseBlock


class DatabaseMigrate(DatabaseBase):

    # migration methods
    async def migrate(self):
        migrations: list[tuple[int, Callable[[psycopg.AsyncConnection[dict[str, Any]]], Awaitable[None]]]] = [
            (1, self.migrate_1_add_rejected_original_id),
        ]
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    for migrated_id, method in migrations:
                        await cur.execute("SELECT COUNT(*) FROM _migration WHERE migrated_id = %s", (migrated_id,))
                        res = await cur.fetchone()
                        if res is None or res['count'] == 0:
                            print(f"DB migrating {migrated_id}")
                            async with conn.transaction():
                                await method(conn)
                                await cur.execute("INSERT INTO _migration (migrated_id) VALUES (%s)", (migrated_id,))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    @staticmethod
    async def migrate_1_add_rejected_original_id(conn: psycopg.AsyncConnection[dict[str, Any]]):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT ct.id, b.id as block_id FROM confirmed_transaction ct "
                "JOIN explorer.block b on ct.block_id = b.id "
                "JOIN explorer.transaction t on t.confirmed_transaction_id = ct.id "
                "WHERE ct.type in ('RejectedExecute', 'RejectedDeploy') AND t.original_transaction_id IS NULL"
            )
            rows = await cur.fetchall()
            blocks: dict[int, list[dict[str, Any]]] = {}
            for row in rows:
                if row["block_id"] not in blocks:
                    await cur.execute("SELECT * FROM get_confirmed_transactions(%s)", (row["block_id"],))
                    blocks[row["block_id"]] = await cur.fetchall()
                for tx in blocks[row["block_id"]]:
                    if tx["confirmed_transaction_id"] == row["id"]:
                        confirmed_transaction = await DatabaseBlock.get_confirmed_transaction_from_dict(conn, tx)
                        original_id = TransactionID.loads(aleo_explorer_rust.rejected_tx_original_id(confirmed_transaction.dump()))
                        await cur.execute(
                            "UPDATE transaction SET original_transaction_id = %s "
                            "WHERE confirmed_transaction_id = %s",
                            (str(original_id), row["id"])
                        )
