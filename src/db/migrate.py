from __future__ import annotations

from typing import Awaitable, LiteralString

import psycopg
import psycopg.sql
from psycopg.rows import DictRow

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseMigrate(DatabaseBase):

    # migration methods
    async def migrate(self):
        migrations: list[tuple[int, Callable[[psycopg.AsyncConnection[DictRow]], Awaitable[None]]]] = [
            (1, self.migration_1_remove_value_id_column),
            (2, self.migration_2_remove_serial_id_column),
            (3, self.migration_3_remove_serial_id_column_bonded_value),
            (4, self.migration_4_recalculate_function_call_count),
            (5, self.migration_5_add_output_record_sender_ciphertext),
            (6, self.migration_6_add_program_edition),
            (7, self.migration_7_add_program_checksum),
            (8, self.migration_8_recalculate_function_call_count),
            (9, self.migration_9_deploy_function_select_program_id),
            (10, self.migration_10_add_dynamic_future_argument_type),
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
    async def migration_1_remove_value_id_column(conn: psycopg.AsyncConnection[DictRow]):
        await conn.execute("ALTER TABLE mapping_value DROP COLUMN value_id")

    @staticmethod
    async def migration_2_remove_serial_id_column(conn: psycopg.AsyncConnection[DictRow]):
        await conn.execute("alter table address_stake_reward drop constraint address_stake_reward_pk")
        await conn.execute("alter table address_stake_reward drop constraint address_stake_reward_pk_2")
        await conn.execute("alter table address_stake_reward drop column id")
        await conn.execute("alter table address_stake_reward add primary key (address)")

    @staticmethod
    async def migration_3_remove_serial_id_column_bonded_value(conn: psycopg.AsyncConnection[DictRow]):
        await conn.execute("alter table mapping_bonded_value drop constraint mapping_bonded_value_pk")
        await conn.execute("drop index mapping_bonded_value_key_id_uindex")
        await conn.execute("alter table mapping_bonded_value drop column id")
        await conn.execute("alter table mapping_bonded_value add primary key (key_id)")

    @staticmethod
    async def migration_4_recalculate_function_call_count(conn: psycopg.AsyncConnection[DictRow]):
        async with conn.cursor() as cur:
            await cur.execute("""
    SELECT
        COUNT(*),
        ts.program_id,
        function_name,
        p.id as program_db_id
    FROM
        transition ts
        JOIN transaction_execute txe ON ts.transaction_execute_id = txe.id
        JOIN transaction tx ON txe.transaction_id = tx.id
        JOIN program p ON ts.program_id = p.program_id
    WHERE
        tx.confirmed_transaction_id IS NOT NULL
    GROUP BY
        ts.program_id,
        p.id,
        ts.function_name
        """)
            result = await cur.fetchall()
            await cur.executemany(
                "UPDATE program_function SET called = %s WHERE program_id = %s AND name = %s",
                [(row["count"], row["program_db_id"], row["function_name"]) for row in result],
            )

    @staticmethod
    async def migration_5_add_output_record_sender_ciphertext(conn: psycopg.AsyncConnection[DictRow]):
        await conn.execute(cast(LiteralString, open("db/migrate_5.sql").read()))

    @staticmethod
    async def migration_6_add_program_edition(conn: psycopg.AsyncConnection[DictRow]):
        await conn.execute("alter table program add edition integer default 0 not null")
        await conn.execute("alter table program drop constraint program_pk2")
        await conn.execute("alter table program add constraint program_pk2 unique (program_id, edition)")

    @staticmethod
    async def migration_7_add_program_checksum(conn: psycopg.AsyncConnection[DictRow]):
        await conn.execute("alter table program add checksum bytea")

    @staticmethod
    async def migration_8_recalculate_function_call_count(conn: psycopg.AsyncConnection[DictRow]):
        async with conn.cursor() as cur:
            await cur.execute("UPDATE program_function SET called = 0")

            # Count from transaction_execute transitions
            await cur.execute("""
                SELECT COUNT(*) as count, p.id as program_db_id, ts.function_name
                FROM transition ts
                JOIN transaction_execute txe ON ts.transaction_execute_id = txe.id
                JOIN transaction tx ON txe.transaction_id = tx.id
                JOIN program p ON ts.program_id = p.program_id
                WHERE tx.confirmed_transaction_id IS NOT NULL
                GROUP BY p.id, ts.function_name
            """)
            result = await cur.fetchall()
            await cur.executemany(
                "UPDATE program_function SET called = called + %s WHERE program_id = %s AND name = %s",
                [(row["count"], row["program_db_id"], row["function_name"]) for row in result],
            )

            # Count from fee transitions
            await cur.execute("""
                SELECT COUNT(*) as count, p.id as program_db_id, ts.function_name
                FROM transition ts
                JOIN fee f ON ts.fee_id = f.id
                JOIN transaction tx ON f.transaction_id = tx.id
                JOIN program p ON ts.program_id = p.program_id
                WHERE tx.confirmed_transaction_id IS NOT NULL
                GROUP BY p.id, ts.function_name
            """)
            result = await cur.fetchall()
            await cur.executemany(
                "UPDATE program_function SET called = called + %s WHERE program_id = %s AND name = %s",
                [(row["count"], row["program_db_id"], row["function_name"]) for row in result],
            )

    @staticmethod
    async def migration_9_deploy_function_select_program_id(conn: psycopg.AsyncConnection[DictRow]):
        await conn.execute(cast(LiteralString, open("db/migrate_9.sql").read()))

    @staticmethod
    async def migration_10_add_dynamic_future_argument_type(conn: psycopg.AsyncConnection[DictRow]):
        await conn.execute("ALTER TYPE argument_type ADD VALUE 'DynamicFuture'")
        await conn.execute("ALTER TABLE future_argument RENAME COLUMN plaintext TO data")