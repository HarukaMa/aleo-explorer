from __future__ import annotations

import psycopg
import psycopg.sql

from aleo_types import *
from aleo_types.cached import cached_get_mapping_id
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseMapping(DatabaseBase):
    async def get_mapping_cache_with_cur(self, cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                         mapping_name: str) -> dict[Field, Any]:
        if program_name == "credits.aleo" and mapping_name in ["committee", "delegated"]:
            try:
                # noinspection SqlResolve
                await cur.execute(
                    psycopg.sql.SQL(
                        "SELECT * FROM {} ORDER BY height DESC LIMIT 1"
                    ).format(psycopg.sql.Identifier(f"mapping_{mapping_name}_history"))
                )
                if (res := await cur.fetchone()) is None:
                    raise RuntimeError(f"genesis mapping values missing")
                mapping_data: dict[str, dict[str, str]] = res["content"]

                def transform_history(kv: tuple[str, dict[str, str]]):
                    k, v = kv
                    return {
                        "key_id": k,
                        "key": Plaintext.load(BytesIO(bytes.fromhex(v["key"]))),
                        "value": Value.load(BytesIO(bytes.fromhex(v["value"]))),
                    }
                return {
                    Field.loads(cast(str, v["key_id"])): {"key": v["key"], "value": v["value"]}
                    for v in map(transform_history, mapping_data.items())
                }
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise
        else:
            try:
                if program_name == "credits.aleo" and mapping_name == "bonded":
                    await cur.execute(
                        "SELECT key_id, key, value FROM mapping_bonded_value"
                    )
                else:
                    mapping_id = Field.loads(cached_get_mapping_id(program_name, mapping_name))
                    await cur.execute(
                        "SELECT key_id, key, value FROM mapping_value mv "
                        "JOIN mapping m on mv.mapping_id = m.id "
                        "WHERE m.mapping_id = %s ",
                        (str(mapping_id),)
                    )
                data = await cur.fetchall()
                def transform(d: dict[str, Any]):
                    return {
                        "key": Plaintext.load(BytesIO(d["key"])),
                        "value": Value.load(BytesIO(d["value"])),
                    }
                return {Field.loads(x["key_id"]): transform(x) for x in data}
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise

    async def get_mapping_cache(self, program_name: str, mapping_name: str) -> dict[Field, Any]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                return await self.get_mapping_cache_with_cur(cur, program_name, mapping_name)

    async def get_mapping_value(self, program_id: str, mapping: str, key_id: str) -> Optional[bytes]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if program_id == "credits.aleo" and mapping in ["committee", "delegated"]:
                        await cur.execute(
                            psycopg.sql.SQL(
                                "SELECT content #> %s as value "
                                "FROM {} ORDER BY height DESC LIMIT 1"
                            ).format(
                                psycopg.sql.Identifier(f"mapping_{mapping}_history")
                            ),
                            (f"{{{key_id}, value}}",)
                        )
                        if (res := await cur.fetchone()) is None:
                            return None
                        return res['value']
                    elif program_id == "credits.aleo" and mapping == "bonded":
                        await cur.execute(
                            "SELECT value FROM mapping_bonded_value WHERE key_id = %s",
                            (key_id,)
                        )
                        if (res := await cur.fetchone()) is None:
                            return None
                        return res['value']
                    else:
                        await cur.execute(
                            "SELECT value FROM mapping_value mv "
                            "JOIN mapping m on mv.mapping_id = m.id "
                            "WHERE m.program_id = %s AND m.mapping = %s AND mv.key_id = %s",
                            (program_id, mapping, key_id)
                        )
                        res = await cur.fetchone()
                        if res is None:
                            return None
                        return res['value']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_size(self, program_id: str, mapping: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM mapping_value mv "
                        "JOIN mapping m on mv.mapping_id = m.id "
                        "WHERE m.program_id = %s AND m.mapping = %s",
                        (program_id, mapping)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res['count']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_key_value(self, program_id: str, mapping: str, count: int, cursor: int = 0) -> tuple[dict[Field, Any], int]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if program_id == "credits.aleo" and mapping in ["committee", "delegated"]:
                        await cur.execute(
                            psycopg.sql.SQL("SELECT * FROM {} ORDER BY height DESC LIMIT 1").format(
                                psycopg.sql.Identifier(f"mapping_{mapping}_history")
                            ),
                        )
                        if (res := await cur.fetchone()) is None:
                            return {}, 0
                        mapping_data: dict[str, dict[str, str]] = res["content"]
                        mapping_tuple = tuple(mapping_data.items())
                        data = mapping_tuple[cursor:cursor + count]
                        cursor = cursor + count if len(data) else 0

                        def transform_history(d: dict[str, str]):
                            return {
                                "key": Plaintext.load(BytesIO(bytes.fromhex(d["key"]))),
                                "value": Value.load(BytesIO(bytes.fromhex(d["value"]))),
                            }

                        return {Field.loads(x[0]): transform_history(x[1]) for x in data}, cursor
                    elif program_id == "credits.aleo" and mapping == "bonded":
                        await cur.execute(
                            "SELECT key_id, key, value FROM mapping_bonded_value ORDER BY key_id LIMIT %s OFFSET %s",
                            (count, cursor)
                        )
                        data = await cur.fetchall()
                        def transform(d: dict[str, Any]):
                            return {
                                "key": Plaintext.load(BytesIO(d["key"])),
                                "value": Value.load(BytesIO(d["value"])),
                            }
                        cursor = cursor + count if len(data) else 0
                        return {Field.loads(x["key_id"]): transform(x) for x in data}, cursor
                    else:
                        cursor_clause = psycopg.sql.SQL("AND mv.id < {} ").format(psycopg.sql.Literal(cursor)) if cursor > 0 else psycopg.sql.SQL("")
                        await cur.execute(
                            psycopg.sql.Composed([
                                psycopg.sql.SQL(
                                    "SELECT mv.id, mv.key_id, mv.key, mv.value "
                                    "FROM mapping m "
                                    "CROSS JOIN LATERAL ("
                                    "    SELECT id, key_id, key, value "
                                    "    FROM mapping_value "
                                    "    WHERE mapping_id = m.id "
                                ),
                                cursor_clause,
                                psycopg.sql.SQL(
                                    "    ORDER BY id LIMIT %s"
                                    ") mv "
                                    "WHERE m.program_id = %s AND m.mapping = %s "
                                    "ORDER BY mv.id"
                                )
                            ]),
                            (count, program_id, mapping)
                        )
                        data = await cur.fetchall()
                        def transform(d: dict[str, Any]):
                            return {
                                "key": Plaintext.load(BytesIO(d["key"])),
                                "value": Value.load(BytesIO(d["value"])),
                            }
                        cursor = data[-1]["id"] if len(data) > 0 else 0
                        return {Field.loads(x["key_id"]): transform(x) for x in data}, cursor
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_key_count(self, program_id: str, mapping: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if program_id == "credits.aleo" and mapping in ["committee", "delegated"]:
                        # noinspection SqlResolve
                        await cur.execute(
                            psycopg.sql.SQL("SELECT content FROM {} ORDER BY height DESC LIMIT 1").format(
                                psycopg.sql.Identifier(f"mapping_{mapping}_history")
                            )
                        )
                        if (res := await cur.fetchone()) is None:
                            return 0
                        mapping_data: dict[str, str] = res["content"]
                        return len(mapping_data)
                    else:
                        if program_id == "credits.aleo" and mapping == "bonded":
                            await cur.execute(
                                "SELECT COUNT(*) FROM mapping_bonded_value"
                            )
                        else:
                            await cur.execute(
                                "SELECT COUNT(*) FROM mapping_value mv "
                                "JOIN mapping m on mv.mapping_id = m.id "
                                "WHERE m.program_id = %s AND m.mapping = %s",
                                (program_id, mapping)
                            )
                        if (res := await cur.fetchone()) is None:
                            return 0
                        return res['count']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    @staticmethod
    async def initialize_mapping(cur: psycopg.AsyncCursor[dict[str, Any]], mapping_id: str, program_id: str, mapping: str):
        await cur.execute(
            "INSERT INTO mapping (mapping_id, program_id, mapping) VALUES (%s, %s, %s)",
            (mapping_id, program_id, mapping)
        )

    async def initialize_builtin_mapping(self, mapping_id: str, program_id: str, mapping: str):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "INSERT INTO mapping (mapping_id, program_id, mapping) VALUES (%s, %s, %s) "
                        "ON CONFLICT DO NOTHING",
                        (mapping_id, program_id, mapping)
                    )
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    @staticmethod
    async def update_mapping_key_value(cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                       mapping_name: str, mapping_id: str, key_id: str, value_id: str,
                                       key: bytes, value: bytes, height: int, from_transaction: bool):
        limited_tracking = program_name == "credits.aleo" and mapping_name in ["committee", "bonded", "delegated"]

        if program_name == "credits.aleo" and mapping_name == "bonded":
            await cur.execute(
                "INSERT INTO mapping_bonded_value (key_id, key, value) VALUES (%s, %s, %s) "
                "ON CONFLICT (key_id) DO UPDATE SET value = excluded.value",
                (key_id, key, value)
            )

        if not limited_tracking or from_transaction:
            await cur.execute("SELECT id FROM mapping WHERE mapping_id = %s", (mapping_id,))
            mapping = await cur.fetchone()
            if mapping is None:
                raise ValueError(f"mapping {mapping_id} not found")
            mapping_id = mapping['id']


            if not limited_tracking:
                await cur.execute(
                    "INSERT INTO mapping_value (mapping_id, key_id, key, value) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (mapping_id, key_id) DO UPDATE SET value = excluded.value",
                    (mapping_id, key_id, key, value)
                )

            await cur.execute(
                "SELECT last_history_id FROM mapping_history_last_id WHERE key_id = %s",
                (key_id,)
            )
            previous_id = res['last_history_id'] if (res := await cur.fetchone()) is not None else None

            await cur.execute(
                "INSERT INTO mapping_history (mapping_id, height, key_id, key, value, from_transaction, previous_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "RETURNING id",
                (mapping_id, height, key_id, key, value, from_transaction, previous_id)
            )
            if (res := await cur.fetchone()) is None:
                raise ValueError("failed to insert mapping history")
            latest_id = res['id']
            await cur.execute(
                "INSERT INTO mapping_history_last_id (key_id, last_history_id) VALUES (%s, %s) "
                "ON CONFLICT (key_id) DO UPDATE SET last_history_id = %s",
                (key_id, latest_id, latest_id)
            )

    @staticmethod
    async def remove_mapping_key_value(cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                       mapping_name: str, mapping_id: str, key_id: str, key: bytes, height: int,
                                       from_transaction: bool):
        limited_tracking = program_name == "credits.aleo" and mapping_name in ["committee", "bonded", "delegated"]

        if program_name == "credits.aleo" and mapping_name == "bonded":
            await cur.execute(
                "DELETE FROM mapping_bonded_value WHERE key_id = %s",
                (key_id,)
            )

        if not limited_tracking or from_transaction:
            await cur.execute("SELECT id FROM mapping WHERE mapping_id = %s", (mapping_id,))
            mapping = await cur.fetchone()
            if mapping is None:
                raise ValueError(f"mapping {mapping_id} not found")
            mapping_id = mapping['id']
            if not limited_tracking:
                await cur.execute(
                    "DELETE FROM mapping_value WHERE mapping_id = %s AND key_id = %s",
                    (mapping_id, key_id)
                )

            await cur.execute(
                "SELECT last_history_id FROM mapping_history_last_id WHERE key_id = %s",
                (key_id,)
            )
            previous_id = res['last_history_id'] if (res := await cur.fetchone()) is not None else None

            await cur.execute(
                "INSERT INTO mapping_history (mapping_id, height, key_id, key, value, from_transaction, previous_id) "
                "VALUES (%s, %s, %s, %s, NULL, %s, %s) "
                "RETURNING id",
                (mapping_id, height, key_id, key, from_transaction, previous_id)
            )
            if (res := await cur.fetchone()) is None:
                raise ValueError("failed to insert mapping history")
            latest_id = res['id']
            await cur.execute(
                "INSERT INTO mapping_history_last_id (key_id, last_history_id) VALUES (%s, %s) "
                "ON CONFLICT (key_id) DO UPDATE SET last_history_id = %s",
                (key_id, latest_id, latest_id)
            )

    async def get_finalize_operations_by_height(self, height: int) -> list[FinalizeOperation]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT f.id, f.type FROM finalize_operation f "
                        "JOIN confirmed_transaction ct on ct.id = f.confirmed_transaction_id "
                        "JOIN block b on b.id = ct.block_id "
                        "WHERE b.height = %s "
                        "ORDER BY f.id",
                        (height,)
                    )
                    data = await cur.fetchall()
                    result: list[FinalizeOperation] = []
                    for d in data:
                        if d["type"] == "UpdateKeyValue":
                            await cur.execute(
                                "SELECT mapping_id, key_id, value_id FROM finalize_operation_update_kv fu "
                                "JOIN explorer.finalize_operation fo on fo.id = fu.finalize_operation_id "
                                "WHERE fo.id = %s",
                                (d["id"],)
                            )
                            u = await cur.fetchone()
                            if u is None:
                                raise ValueError(f"finalize operation {d['id']} not found")
                            result.append(UpdateKeyValue(
                                mapping_id=Field.loads(u["mapping_id"]),
                                key_id=Field.loads(u["key_id"]),
                                value_id=Field.loads(u["value_id"]),
                            ))
                        elif d["type"] == "RemoveKeyValue":
                            await cur.execute(
                                "SELECT mapping_id, key_id FROM finalize_operation_remove_kv fu "
                                "JOIN explorer.finalize_operation fo on fo.id = fu.finalize_operation_id "
                                "WHERE fo.id = %s",
                                (d["id"],)
                            )
                            u = await cur.fetchone()
                            if u is None:
                                raise ValueError(f"finalize operation {d['id']} not found")
                            result.append(RemoveKeyValue(
                                mapping_id=Field.loads(u["mapping_id"]),
                                key_id=Field.loads(u["key_id"]),
                            ))
                    return result
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_transaction_mapping_history_by_height(self, height: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT mh.id, m.program_id, m.mapping, m.mapping_id, mh.key_id, mh.key, mh.value FROM mapping_history mh "
                        "JOIN mapping m on mh.mapping_id = m.id "
                        "WHERE mh.height = %s AND mh.from_transaction = TRUE "
                        "ORDER BY mh.id",
                        (height,)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_history_previous_value(self, history_id: int, key_id: str) -> Optional[bytes]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT previous_id FROM mapping_history WHERE id = %s",
                        (history_id,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None
                    previous_id = res["previous_id"]
                    if previous_id is None:
                        return None
                    await cur.execute(
                        "SELECT key_id, value FROM mapping_history WHERE id = %s",
                        (previous_id,)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    if res["key_id"] != key_id:
                        return None
                    return res['value']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_value_at_height(self, program_id: str, mapping: str, key_id: str, height: int) -> tuple[Optional[bytes], Optional[int]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if program_id == "credits.aleo" and mapping in ["committee", "bonded", "delegated"]:
                        # noinspection SqlResolve
                        query = psycopg.sql.SQL("SELECT height, content FROM {} WHERE height <= %s ORDER BY height DESC LIMIT 1").format(psycopg.sql.Identifier(f"mapping_{mapping}_history"))
                        await cur.execute(query, (height,))
                        if (res := await cur.fetchone()) is None:
                            return None, None
                        mapping_data: dict[str, dict[str, str]] = res["content"]
                        if (data := mapping_data.get(key_id)) is None:
                            return None, None
                        return bytes.fromhex(data["value"]), res["height"]
                    await cur.execute(
                        "SELECT value FROM mapping_history mh "
                        "JOIN mapping m on mh.mapping_id = m.id "
                        "WHERE m.program_id = %s AND m.mapping = %s AND mh.key_id = %s AND mh.height <= %s "
                        "ORDER BY mh.id DESC "
                        "LIMIT 1",
                        (program_id, mapping, key_id, height)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None, None
                    return res["value"], None
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise