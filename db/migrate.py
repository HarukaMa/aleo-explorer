from __future__ import annotations

import os
from typing import Awaitable, LiteralString

import psycopg
import psycopg.sql
from psycopg.rows import DictRow

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase
from .block import DatabaseBlock


class DatabaseMigrate(DatabaseBase):

    # migration methods
    async def migrate(self):
        migrations: list[tuple[int, Callable[[psycopg.AsyncConnection[DictRow]], Awaitable[None]]]] = [
            (1, self.migrate_1_add_rejected_original_id),
            (2, self.migrate_2_set_on_delete_cascade),
            (3, self.migrate_3_fix_finalize_operation_function),
            (4, self.migrate_4_add_solution_id)
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
    async def migrate_1_add_rejected_original_id(conn: psycopg.AsyncConnection[DictRow]):
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

    @staticmethod
    async def migrate_2_set_on_delete_cascade(conn: psycopg.AsyncConnection[DictRow]):
        # https://stackoverflow.com/a/74476119
        try:
            await conn.execute("""
-- noinspection SqlResolve
WITH tables(oid) AS (
   UPDATE pg_constraint
   SET confdeltype = 'c'
   WHERE contype = 'f'
     AND confdeltype <> 'c'
     AND connamespace = %s::regnamespace
   RETURNING confrelid
)
UPDATE pg_trigger
SET tgfoid = '"RI_FKey_cascade_del"()'::regprocedure
FROM tables
WHERE tables.oid = pg_trigger.tgrelid
  AND tgtype = 9;
                """, (os.environ["DB_SCHEMA"],)
                               )
        except psycopg.errors.InsufficientPrivilege:
            print("==========================")
            print("You need superuser to run this migration")
            print("If you can't or don't want to add superuser, please run it manually")
            print("==========================")
            raise

    @staticmethod
    async def migrate_3_fix_finalize_operation_function(conn: psycopg.AsyncConnection[DictRow]):
        await conn.execute(cast(LiteralString, open("migration_3.sql").read()))


    @staticmethod
    async def migrate_4_add_solution_id(conn: psycopg.AsyncConnection[DictRow]):
        # add column
        try:
            async with conn.transaction():
                await conn.execute("""
                    ALTER TABLE solution
                    ADD COLUMN solution_id TEXT NULL
                """)
        except Exception as e:
            raise RuntimeError(f"Failed to add column 'solution_id' to 'solution': {e}")

        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id, epoch_hash, address, counter FROM solution ",
                )
                rows = await cur.fetchall()
                if not rows:
                    break

                for row in rows:
                    solution_id = SolutionID.load(
                        BytesIO(aleo_explorer_rust.solution_to_id(
                            str(row["epoch_hash"]),
                            str(row["address"]),
                            int(row["counter"])
                        )
                        )
                    )
                    await cur.execute(
                        "UPDATE solution SET solution_id = %s WHERE id = %s",
                        (str(solution_id), row["id"])
                    )
