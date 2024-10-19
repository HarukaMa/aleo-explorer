from __future__ import annotations

from typing import Awaitable

import psycopg
import psycopg.sql
from psycopg.rows import DictRow
from redis.asyncio.client import Redis

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseMigrate(DatabaseBase):

    # migration methods
    async def migrate(self):
        migrations: list[tuple[int, Callable[[psycopg.AsyncConnection[DictRow], Redis[str]], Awaitable[None]]]] = [
            (1, self.migrate_1_add_block_validator_index),
            (2, self.migrate_2_add_address_tag_and_validator_table),
            (3, self.migrate_3_change_tag_validator_index_to_unique),
            (4, self.migrate_4_create_transaction_aborted_flag),
            (5, self.migrate_5_add_confirm_timestamp_to_block),
            (6, self.migrate_6_check_aborted_unconfirmed_transactions),
            (7, self.migrate_7_rebuild_solution_id_index_with_ops),
            (8, self.migrate_8_fix_missing_fee_stats),
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
    async def migrate_1_add_block_validator_index(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute("CREATE INDEX block_validator_block_id_index ON block_validator (block_id)")

    @staticmethod
    async def migrate_2_add_address_tag_and_validator_table(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute("""
create table address_tag
(
    id      serial not null
        constraint address_tag_pk
            primary key,
    address text   not null,
    tag     text   not null
)"""
        )
        await conn.execute("create index address_tag_address_index on address_tag (address)")
        await conn.execute("create index address_tag_tag_index on address_tag (tag)")
        await conn.execute("""
create table validator_info
(
    id      serial not null
        constraint validator_info_pk
            primary key,
    address text   not null,
    website text,
    logo    text
)""")
        await conn.execute("create index validator_info_address_index on validator_info (address)")

    @staticmethod
    async def migrate_3_change_tag_validator_index_to_unique(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute("drop index address_tag_address_index")
        await conn.execute("drop index address_tag_tag_index")
        await conn.execute("create unique index address_tag_address_index on address_tag (address)")
        await conn.execute("create unique index address_tag_tag_index on address_tag (tag)")

        await conn.execute("drop index validator_info_address_index")
        await conn.execute("create unique index validator_info_address_index on validator_info (address)")

    @staticmethod
    async def migrate_4_create_transaction_aborted_flag(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute("alter table transaction add column aborted boolean not null default false")

    @staticmethod
    async def migrate_5_add_confirm_timestamp_to_block(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        async with conn.cursor() as cur:
            await cur.execute("alter table block add column confirm_timestamp bigint")
            await cur.execute(
                "select height, max(s.timestamp) as timestamp from block b "
                "join authority a on a.block_id = b.id "
                "join dag_vertex s on s.authority_id = a.id "
                "group by height "
                "order by height desc"
            )
            res = await cur.fetchall()
            for row in res:
                await cur.execute("update block set confirm_timestamp = %s where height = %s", (row["timestamp"], row["height"]))
            await cur.execute("update block set confirm_timestamp = timestamp where height = 0")
            await cur.execute("alter table block alter column confirm_timestamp set not null")

    @staticmethod
    async def migrate_6_check_aborted_unconfirmed_transactions(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        async with conn.cursor() as cur:
            await cur.execute("select transaction_id from transaction where aborted = false and confirmed_transaction_id is null")
            res = await cur.fetchall()
            for row in res:
                await cur.execute("select count(*) from block_aborted_transaction_id where transaction_id = %s", (row["transaction_id"],))
                count = await cur.fetchone()
                if count is not None and count["count"] == 1:
                    await cur.execute("update transaction set aborted = true where transaction_id = %s", (row["transaction_id"],))

    @staticmethod
    async def migrate_7_rebuild_solution_id_index_with_ops(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        await conn.execute("drop index solution_puzzle_solution_id_index")
        await conn.execute("create index solution_puzzle_solution_id_index on solution (solution_id text_pattern_ops)")
        await conn.execute("create index block_aborted_solution_id_solution_id_index on block_aborted_solution_id (solution_id text_pattern_ops)")

    @staticmethod
    async def migrate_8_fix_missing_fee_stats(conn: psycopg.AsyncConnection[DictRow], redis: Redis[str]):
        async with conn.cursor() as cur:
            await cur.execute("""
                select tx.id
                from transition ts
                         join transaction_execute te on te.id = ts.transaction_execute_id
                         join transaction tx on tx.id = te.transaction_id
                where program_id = 'credits.aleo'
                  and function_name = 'unbond_public'
                  and tx.type = 'Execute'
            """)
            txs = await cur.fetchall()
            for tx_id in [x["id"] for x in txs]:
                await cur.execute("""
                    select fa.*
                    from future_argument fa
                             join future fu on fu.id = fa.future_id
                             join transition_output_future tsof on tsof.id = fu.transition_output_future_id
                             join transition_output tso on tso.id = tsof.transition_output_id
                             join transition ts on ts.id = tso.transition_id
                             join fee f on ts.fee_id = f.id
                             join transaction tx on tx.id = f.transaction_id
                    where tx.id = %s
                    order by fa.id
                """, (tx_id,))
                args = [Plaintext.load(BytesIO(x["plaintext"])) for x in await cur.fetchall()]
                if len(args) != 2:
                    raise RuntimeError(f"unexpected number of args: {args}")
                address = cast(LiteralPlaintext, args[0])
                amount = cast(LiteralPlaintext, args[1])
                await redis.hincrby("address_fee", str(address), int(str(amount).replace("u64", "")))
                print(f"added fee {amount} to {address}")
