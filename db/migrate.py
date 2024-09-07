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