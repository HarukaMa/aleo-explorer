from __future__ import annotations

import os
from typing import Awaitable, LiteralString

import psycopg
import psycopg.sql
from psycopg.rows import DictRow
from redis.asyncio.client import Redis

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase
from .block import DatabaseBlock


class DatabaseMigrate(DatabaseBase):

    # migration methods
    async def migrate(self):
        migrations: list[tuple[int, Callable[[psycopg.AsyncConnection[DictRow], Redis[str]], Awaitable[None]]]] = [
            (1, self.migrate_1_add_rejected_original_id),
            (2, self.migrate_2_set_on_delete_cascade),
            (3, self.migrate_3_fix_finalize_operation_function),
            (4, self.migrate_4_add_solution_id),
            (5, self.migrate_5_full_public_balance_stats_tracking),
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
                                await method(conn, self.redis)
                                await cur.execute("INSERT INTO _migration (migrated_id) VALUES (%s)", (migrated_id,))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    @staticmethod
    async def migrate_1_add_rejected_original_id(conn: psycopg.AsyncConnection[DictRow], _: Redis[str]):
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
    async def migrate_2_set_on_delete_cascade(conn: psycopg.AsyncConnection[DictRow], _: Redis[str]):
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
    async def migrate_3_fix_finalize_operation_function(conn: psycopg.AsyncConnection[DictRow], _: Redis[str]):
        await conn.execute(cast(LiteralString, open("migration_3.sql").read()))


    @staticmethod
    async def migrate_4_add_solution_id(conn: psycopg.AsyncConnection[DictRow], _: Redis[str]):
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
                    return

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

    @staticmethod
    async def migrate_5_full_public_balance_stats_tracking(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        async with conn.cursor() as cur:
            # all additional finalize executions to track
            await cur.execute(
                "SELECT b.height, f.function_name, f.id FROM future f "
                "JOIN transition_output_future tof on f.transition_output_future_id = tof.id "
                "JOIN transition_output to_ on tof.transition_output_id = to_.id "
                "JOIN transition t on to_.transition_id = t.id "
                "JOIN transaction_execute te on t.transaction_execute_id = te.id "
                "JOIN transaction tx on te.transaction_id = tx.id "
                "JOIN confirmed_transaction ct on tx.confirmed_transaction_id = ct.id "
                "JOIN block b on ct.block_id = b.id "
                "WHERE f.program_id = 'credits.aleo' "
                "AND f.function_name IN ('bond_validator', 'bond_public', 'claim_unbond_public') "
                "AND ct.type = 'AcceptedExecute'"
            )
            futures = await cur.fetchall()
            for future in futures:
                # future arguments
                await cur.execute(
                    "SELECT fa.plaintext FROM future_argument fa "
                    "JOIN explorer.future f on f.id = fa.future_id "
                    "WHERE f.id = %s "
                    "ORDER BY fa.id",
                    (future["id"],)
                )
                arguments = await cur.fetchall()
                if future["function_name"] in ("bond_validator", "bond_public"):
                    if future["function_name"] == "bond_validator":
                        # bond_validator(val, withdraw, amount, commission)
                        validator_plaintext = cast(LiteralPlaintext, Plaintext.load(BytesIO(arguments[0]["plaintext"])))
                        validator_address = str(validator_plaintext)
                        amount = int(cast(u64, cast(LiteralPlaintext, Plaintext.load(BytesIO(arguments[2]["plaintext"]))).literal.primitive))
                        await redis.hincrby("address_transfer_out", validator_address, amount)
                    elif future["function_name"] == "bond_public":
                        # bond_public(staker, val, withdraw, amount)
                        staker_plaintext = cast(LiteralPlaintext, Plaintext.load(BytesIO(arguments[0]["plaintext"])))
                        staker_address = str(staker_plaintext)
                        amount = int(cast(u64, cast(LiteralPlaintext, Plaintext.load(BytesIO(arguments[3]["plaintext"]))).literal.primitive))
                        await redis.hincrby("address_transfer_out", staker_address, amount)
                elif future["function_name"] == "claim_unbond_public":
                    # claim_unbond_public(staker)
                    staker_plaintext = cast(LiteralPlaintext, Plaintext.load(BytesIO(arguments[0]["plaintext"])))
                    staker_address = str(staker_plaintext)
                    # get last unbonding mapping value
                    unbonding_key_id = cached_get_key_id("credits.aleo", "unbonding", staker_plaintext.dump())
                    await cur.execute(
                        "SELECT value FROM mapping_history "
                        "WHERE height < %s AND key_id = %s "
                        "ORDER BY height DESC LIMIT 1",
                        (future["height"], unbonding_key_id)
                    )
                    row = await cur.fetchone()
                    if row is None:
                        raise RuntimeError(f"unbonding mapping value not found for {staker_address} at height {future['height']}")
                    unbonding_value = Value.load(BytesIO(row["value"]))
                    unbonding = cast(StructPlaintext, cast(PlaintextValue, unbonding_value).plaintext)
                    amount = int(cast(u64, cast(LiteralPlaintext, unbonding["microcredits"]).literal.primitive))
                    # get last withdraw mapping value
                    withdraw_key_id = cached_get_key_id("credits.aleo", "withdraw", staker_plaintext.dump())
                    await cur.execute(
                        "SELECT value FROM mapping_history "
                        "WHERE height < %s AND key_id = %s "
                        "ORDER BY height DESC LIMIT 1",
                        (future["height"], withdraw_key_id)
                    )
                    row = await cur.fetchone()
                    if row is None:
                        raise RuntimeError(f"withdraw mapping value not found for {staker_address} at height {future['height']}")
                    withdraw_value = Value.load(BytesIO(row["value"]))
                    withdraw_address = str(cast(LiteralPlaintext, cast(PlaintextValue, withdraw_value).plaintext))
                    await redis.hincrby("address_transfer_in", withdraw_address, amount)
