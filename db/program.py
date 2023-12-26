from __future__ import annotations

import psycopg
import psycopg.sql

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseProgram(DatabaseBase):


    async def get_function_definition(self, program_id: str, function_name: str) -> Optional[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT * FROM program_function "
                        "JOIN program ON program.id = program_function.program_id "
                        "WHERE program.program_id = %s AND name = %s",
                        (program_id, function_name)
                    )
                    return await cur.fetchone()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_count(self, no_helloworld: bool = False) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    if no_helloworld:
                        await cur.execute(
                            "SELECT COUNT(*) FROM program "
                            "WHERE feature_hash NOT IN (SELECT hash FROM program_filter_hash)"
                        )
                    else:
                        await cur.execute("SELECT COUNT(*) FROM program")
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res['count']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_programs(self, start: int, end: int, no_helloworld: bool = False) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    where = "WHERE feature_hash NOT IN (SELECT hash FROM program_filter_hash) " if no_helloworld else ""
                    await cur.execute(
                        "/*+ Leading(p td t ct b pf) IndexScan(t) BitmapScan(td) BitmapScan(pf) */ "
                        "SELECT p.program_id, b.height, t.transaction_id, SUM(pf.called) as called "
                        "FROM program p "
                        "JOIN transaction_deploy td on p.transaction_deploy_id = td.id "
                        "JOIN transaction t on td.transaction_id = t.id "
                        "JOIN confirmed_transaction ct on t.confimed_transaction_id = ct.id "
                        "JOIN block b on ct.block_id = b.id "
                        "JOIN program_function pf on p.id = pf.program_id "
                        f"{where}"
                        "GROUP BY p.program_id, b.height, t.transaction_id "
                        "ORDER BY b.height DESC "
                        "LIMIT %s OFFSET %s",
                        (end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_builtin_programs(self):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT p.program_id, SUM(pf.called) as called "
                        "FROM program p "
                        "JOIN program_function pf on p.id = pf.program_id "
                        "WHERE p.transaction_deploy_id IS NULL "
                        "GROUP BY p.program_id "
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_programs_with_feature_hash(self, feature_hash: bytes, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT p.program_id, b.height, t.transaction_id, SUM(pf.called) as called "
                        "FROM program p "
                        "JOIN transaction_deploy td on p.transaction_deploy_id = td.id "
                        "JOIN transaction t on td.transaction_id = t.id "
                        "JOIN confirmed_transaction ct on t.confimed_transaction_id = ct.id "
                        "JOIN block b on ct.block_id = b.id "
                        "JOIN program_function pf on p.id = pf.program_id "
                        "WHERE feature_hash = %s "
                        "GROUP BY p.program_id, b.height, t.transaction_id "
                        "ORDER BY b.height "
                        "LIMIT %s OFFSET %s",
                        (feature_hash, end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_block_by_program_id(self, program_id: str) -> Block | None:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT height FROM transaction tx "
                        "JOIN transaction_deploy td on tx.id = td.transaction_id "
                        "JOIN program p on td.id = p.transaction_deploy_id "
                        "JOIN confirmed_transaction ct on ct.id = tx.confimed_transaction_id "
                        "JOIN block b on ct.block_id = b.id "
                        "WHERE p.program_id = %s",
                        (program_id,)
                    )
                    height = await cur.fetchone()
                    if height is None:
                        return None
                    return await self.get_block_by_height(height["height"])
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_deploy_info_by_program_id(self, program_id: str) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, t.transaction_id FROM block b "
                        "JOIN confirmed_transaction ct on b.id = ct.block_id "
                        "JOIN transaction t on ct.id = t.confimed_transaction_id "
                        "JOIN transaction_deploy td on t.id = td.transaction_id "
                        "JOIN program p on td.id = p.transaction_deploy_id "
                        "WHERE p.program_id = %s",
                        (program_id,)
                    )
                    data = await cur.fetchone()
                    if data is None:
                        return None
                    return data
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_program_called_times(self, program_id: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT sum(called) FROM program_function "
                        "JOIN program ON program.id = program_function.program_id "
                        "WHERE program.program_id = %s",
                        (program_id,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res['sum']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_program_calls(self, program_id: str, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, ts.transition_id, function_name, ct.type "
                        "FROM transition ts "
                        "JOIN transaction_execute te on te.id = ts.transaction_execute_id "
                        "JOIN transaction t on te.transaction_id = t.id "
                        "JOIN confirmed_transaction ct on t.confimed_transaction_id = ct.id "
                        "JOIN block b on ct.block_id = b.id "
                        "WHERE ts.program_id = %s "
                        "ORDER BY b.height DESC "
                        "LIMIT %s OFFSET %s",
                        (program_id, end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_similar_count(self, program_id: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM program "
                        "WHERE feature_hash = (SELECT feature_hash FROM program WHERE program_id = %s)",
                        (program_id,)
                    )
                    if (res := await cur.fetchone()) is None:
                        raise ValueError(f"Program {program_id} not found")
                    return res['count'] - 1
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_feature_hash(self, program_id: str) -> Optional[bytes]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT feature_hash FROM program WHERE program_id = %s",
                        (program_id,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res['feature_hash']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_recent_programs_by_address(self, address: str) -> list[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT program_id FROM program WHERE owner = %s ORDER BY id DESC LIMIT 30", (address,)
                    )
                    return list(map(lambda x: x['program_id'], await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_count_by_address(self, address: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT COUNT(*) FROM program WHERE owner = %s", (address,))
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res['count']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program(self, program_id: str) -> Optional[bytes]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT raw_data FROM program WHERE program_id = %s", (program_id,))
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    return res['raw_data']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_mapping_cache_with_cur(self, cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                         mapping_name: str) -> dict[Field, Any]:
        if program_name == "credits.aleo" and mapping_name in ["committee", "bonded"]:
            def transform(d: dict[str, Any]):
                return {
                    "key": Plaintext.load(BytesIO(bytes.fromhex(d["key"]))),
                    "value": Value.load(BytesIO(bytes.fromhex(d["value"]))),
                }
            data = await self.redis.hgetall(f"{program_name}:{mapping_name}")
            return {Field.loads(k): transform(json.loads(v)) for k, v in data.items()}
        else:
            mapping_id = Field.loads(cached_get_mapping_id(program_name, mapping_name))
            try:
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
                    if program_id == "credits.aleo" and mapping in ["committee", "bonded"]:
                        conn = self.redis
                        data = await conn.hget(f"{program_id}:{mapping}", key_id)
                        if data is None:
                            return None
                        return bytes.fromhex(json.loads(data)["value"])
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
                    if program_id == "credits.aleo" and mapping in ["committee", "bonded"]:
                        def transform(d: dict[str, Any]):
                            return {
                                "key": Plaintext.load(BytesIO(bytes.fromhex(d["key"]))),
                                "value": Value.load(BytesIO(bytes.fromhex(d["value"]))),
                            }
                        conn = self.redis
                        data = await conn.hscan(f"{program_id}:{mapping}", cursor, count=count)
                        return {Field.loads(k): transform(json.loads(v)) for k, v in data[1].items()}, data[0]
                    else:
                        cursor_clause = psycopg.sql.SQL("AND mv.id < {} ").format(psycopg.sql.Literal(cursor)) if cursor > 0 else psycopg.sql.SQL("")
                        await cur.execute(
                            psycopg.sql.Composed([
                                psycopg.sql.SQL(
                                    "SELECT mv.id, key_id, key, value FROM mapping_value mv "
                                    "JOIN mapping m on mv.mapping_id = m.id "
                                    "WHERE m.program_id = %s AND m.mapping = %s "
                                ),
                                cursor_clause,
                                psycopg.sql.SQL(
                                    "ORDER BY mv.id DESC "
                                    "LIMIT %s"
                                )
                            ]),
                            (program_id, mapping, count)
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
                    if program_id == "credits.aleo" and mapping in ["committee", "bonded"]:
                        conn = self.redis
                        return await conn.hlen(f"{program_id}:{mapping}")
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

    async def initialize_mapping(self, cur: psycopg.AsyncCursor[dict[str, Any]], mapping_id: str, program_id: str, mapping: str):
        try:
            await cur.execute(
                "INSERT INTO mapping (mapping_id, program_id, mapping) VALUES (%s, %s, %s)",
                (mapping_id, program_id, mapping)
            )
        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

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

    async def update_mapping_key_value(self, cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                       mapping_name: str, mapping_id: str, key_id: str, value_id: str,
                                       key: bytes, value: bytes, height: int, from_transaction: bool):
        try:
            if program_name == "credits.aleo" and mapping_name in ["committee", "bonded"]:
                conn = self.redis
                data = {
                    "key": key.hex(),
                    "value": value.hex(),
                }
                await conn.hset(f"{program_name}:{mapping_name}", key_id, json.dumps(data))
            else:
                await cur.execute("SELECT id FROM mapping WHERE mapping_id = %s", (mapping_id,))
                mapping = await cur.fetchone()
                if mapping is None:
                    raise ValueError(f"mapping {mapping_id} not found")
                mapping_id = mapping['id']
                await cur.execute(
                    "INSERT INTO mapping_value (mapping_id, key_id, value_id, key, value) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (mapping_id, key_id) DO UPDATE SET value_id = %s, value = %s",
                    (mapping_id, key_id, value_id, key, value, value_id, value)
                )

                await cur.execute(
                    "INSERT INTO mapping_history (mapping_id, height, key_id, key, value, from_transaction) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (mapping_id, height, key_id, key, value, from_transaction)
                )

        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

    async def remove_mapping_key_value(self, cur: psycopg.AsyncCursor[dict[str, Any]], program_name: str,
                                       mapping_name: str, mapping_id: str, key_id: str, key: bytes, height: int,
                                       from_transaction: bool):
        try:
            if program_name == "credits.aleo" and mapping_name in ["committee", "bonded"]:
                conn = self.redis
                await conn.hdel(f"{program_name}:{mapping_name}", key_id)
            else:
                await cur.execute("SELECT id FROM mapping WHERE mapping_id = %s", (mapping_id,))
                mapping = await cur.fetchone()
                if mapping is None:
                    raise ValueError(f"mapping {mapping_id} not found")
                mapping_id = mapping['id']
                await cur.execute(
                    "DELETE FROM mapping_value WHERE mapping_id = %s AND key_id = %s",
                    (mapping_id, key_id)
                )

                await cur.execute(
                    "INSERT INTO mapping_history (mapping_id, height, key_id, key, value, from_transaction) "
                    "VALUES (%s, %s, %s, %s, NULL, %s)",
                    (mapping_id, height, key_id, key, from_transaction)
                )

        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
            raise

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
                            result.append(UpdateKeyValue(
                                mapping_id=Field.loads(u["mapping_id"]),
                                key_id=Field.loads(u["key_id"]),
                                value_id=Field.loads(u["value_id"]),
                                index=u64(),
                            ))
                        elif d["type"] == "RemoveKeyValue":
                            await cur.execute(
                                "SELECT mapping_id FROM finalize_operation_remove_kv fu "
                                "JOIN explorer.finalize_operation fo on fo.id = fu.finalize_operation_id "
                                "WHERE fo.id = %s",
                                (d["id"],)
                            )
                            u = await cur.fetchone()
                            result.append(RemoveKeyValue(
                                mapping_id=Field.loads(u["mapping_id"]),
                                index=u64()
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
                        "SELECT key, value FROM mapping_history WHERE id < %s AND key_id = %s ORDER BY id DESC LIMIT 1",
                        (history_id, key_id)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    return res['value']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_program_leo_source_code(self, program_id: str) -> Optional[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT leo_source FROM program WHERE program_id = %s", (program_id,))
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res['leo_source']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def store_program_leo_source_code(self, program_id: str, source_code: str):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "UPDATE program SET leo_source = %s WHERE program_id = %s", (source_code, program_id)
                    )
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise