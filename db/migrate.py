from __future__ import annotations

from typing import Awaitable

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