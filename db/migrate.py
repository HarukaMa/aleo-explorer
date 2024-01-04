from __future__ import annotations

from typing import Awaitable, cast

import psycopg
import psycopg.sql

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseMigrate(DatabaseBase):




    # migration methods
    async def migrate(self):
        migrations: list[tuple[int, Callable[[psycopg.AsyncConnection[dict[str, Any]]], Awaitable[None]]]] = [
            (1, self.migrate_1_add_dag_vertex_adjacency_index),
            (2, self.migrate_2_add_helper_functions),
            (3, self.migrate_3_set_mapping_history_key_not_null),
            (4, self.migrate_4_support_batch_certificate_v2),
            (5, self.migrate_5_fix_missing_program),
            (6, self.migrate_6_nullable_dag_vertex_id),
            (7, self.migrate_7_nullable_confirmed_transaction),
            (8, self.migrate_8_add_transaction_first_seen),
            (9, self.migrate_9_add_transaction_original_id),
            (10, self.migrate_10_add_deploy_unconfirmed_program_info),
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
    async def migrate_1_add_dag_vertex_adjacency_index(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute("create index dag_vertex_adjacency_vertex_id_index on dag_vertex_adjacency (vertex_id)")

    @staticmethod
    async def migrate_2_add_helper_functions(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute(open("migration_2.sql").read())

    @staticmethod
    async def migrate_3_set_mapping_history_key_not_null(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute("alter table mapping_history alter column key set not null")

    @staticmethod
    async def migrate_4_support_batch_certificate_v2(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute("alter table dag_vertex alter column batch_certificate_id drop not null")
        await conn.execute("alter table dag_vertex_signature alter column timestamp drop not null")

    @staticmethod
    async def migrate_5_fix_missing_program(conn: psycopg.AsyncConnection[dict[str, Any]]):
        async with conn.cursor() as cur:
            await cur.execute("select id from transaction where transaction_id = 'at1flfmj9pxkyz86zsezsdcjtc8pzr7uhx7skjzu7u7mzhkrmqh5gpqsf3gaf'")
            transaction_id = (await cur.fetchone())["id"]
            await cur.execute("select id from transaction_deploy where transaction_id = %s", (transaction_id,))
            transaction_deploy_id = (await cur.fetchone())["id"]
            program = Program.load(BytesIO(bytes.fromhex("010b76656c747a6c65656c6c3204616c656f000100040568656c6c6f0200000001000b000102000b0100000002000100000100010002010001000202000b00")))
            class x:
                owner = ProgramOwner(
                    address=Address.loads("aleo1cgxwzfrwpucrvyxgf9muu49mqnw2gmktm24kf9uvchurhyp2jggs3z4xmt"),
                    signature=Signature.loads("sign1t6szyjpa6mvqsm4uzr57zl0uzu6wnmdwasfuqpkp3ead9lvh0sphjtgadd77lp57g87mkzxep5wylye7ftxz8upgqv65wvqa5wq8wq5h6sn6z5gmu30xfgp3tk9u44kqcjalwv4fpsml3uxwdvy2larvq7tpjf0f27mlm5ckkewzzawllxhfglhgnjp6w5gyxp6ar82xjy3q66hqwt7")
                )

            from db.insert import DatabaseInsert
            await DatabaseInsert._save_program(cur, program, transaction_deploy_id, cast(DeployTransaction, x()))

    @staticmethod
    async def migrate_6_nullable_dag_vertex_id(conn: psycopg.AsyncConnection[dict[str, Any]]):
        async with conn.cursor() as cur:
            await cur.execute("alter table prover_solution alter column dag_vertex_id drop not null")

    @staticmethod
    async def migrate_7_nullable_confirmed_transaction(conn: psycopg.AsyncConnection[dict[str, Any]]):
        async with conn.cursor() as cur:
            await cur.execute("alter table transaction alter column confimed_transaction_id drop not null")

    @staticmethod
    async def migrate_8_add_transaction_first_seen(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute("alter table transaction add column first_seen bigint")
        await conn.execute("alter table transaction alter column first_seen set default extract(epoch from now())")

    @staticmethod
    async def migrate_9_add_transaction_original_id(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute("alter table transaction add column original_transaction_id text")

    @staticmethod
    async def migrate_10_add_deploy_unconfirmed_program_info(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute("alter table transaction_deploy add column program_id text, add column owner text")