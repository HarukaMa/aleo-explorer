from __future__ import annotations

import os
from typing import Awaitable

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
            (11, self.migrate_11_add_original_transaction_id_index),
            (12, self.migrate_12_set_on_delete_cascade),
            (13, self.migrate_13_add_program_address_index),
            (14, self.migrate_14_add_mapping_history_prev_pointer),
            (15, self.migrate_15_add_mapping_history_last_id),
            (16, self.migrate_16_add_rejected_deploy_support),
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
            if (res := await cur.fetchone()) is None:
                return
            transaction_id = res["id"]
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

    @staticmethod
    async def migrate_11_add_original_transaction_id_index(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute("create index transaction_original_transaction_id_index on transaction (original_transaction_id text_pattern_ops)")

    @staticmethod
    async def migrate_12_set_on_delete_cascade(conn: psycopg.AsyncConnection[dict[str, Any]]):
        # https://stackoverflow.com/a/74476119
        try:
            await conn.execute("""
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
    async def migrate_13_add_program_address_index(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute("create index program_address_index on program (address text_pattern_ops)")

    @staticmethod
    async def migrate_14_add_mapping_history_prev_pointer(conn: psycopg.AsyncConnection[dict[str, Any]]):
        print("WARNING: This migration needs 10+ GBs of RAM")
        print("Long running migration, please wait")
        await conn.execute("alter table mapping_history add previous_id bigint")
        await conn.execute(
            "alter table mapping_history "
            "add constraint mapping_history_mapping_history_id_fk "
            "foreign key (previous_id) references mapping_history "
            "on update restrict on delete restrict"
        )
        async with conn.cursor() as cur:
            await cur.execute("select count(*) from mapping_history")
            count = (await cur.fetchone())["count"]
            await cur.execute("select id, key_id from mapping_history order by id")
            n = 0
            data = await cur.fetchall()
            last_id = {}
            update_data = []

            for row in data:
                previous_id = last_id.get(row["key_id"])
                if previous_id is not None:
                    update_data.append((previous_id, row["id"]))
                last_id[row["key_id"]] = row["id"]
                n += 1
                if n % 1000000 == 0:
                    print(f"{n}/{count} rows processed")

            print("Updating database...")
            print("1/5")
            await cur.execute("create temporary table mapping_history_temp (previous_id bigint, id bigint) on commit drop")
            print("2/5")
            async with cur.copy("copy mapping_history_temp (previous_id, id) from stdin") as copy:
                for row in update_data:
                    await copy.write_row(row)
            print("3/5")
            await cur.execute("create unique index mapping_history_temp_id_uindex on mapping_history_temp (id)")
            print("4/5")
            await cur.execute("update mapping_history set previous_id = m.previous_id from mapping_history_temp m where mapping_history.id = m.id")
            print("5/5")
            await cur.execute("create unique index mapping_history_previous_id_uindex on mapping_history (previous_id)")

    @staticmethod
    async def migrate_15_add_mapping_history_last_id(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute(
            "create table mapping_history_last_id ("
            "    key_id text not null"
            "        constraint mapping_history_last_id_pk"
            "            primary key,"
            "    last_history_id bigint not null"
            ")"
        )
        await conn.execute(
            "insert into mapping_history_last_id (key_id, last_history_id) select key_id, max(id) from mapping_history group by key_id"
        )

    @staticmethod
    async def migrate_16_add_rejected_deploy_support(conn: psycopg.AsyncConnection[dict[str, Any]]):
        await conn.execute(open("migration_16.sql").read())