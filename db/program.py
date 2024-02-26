from __future__ import annotations

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

    async def get_program_address(self, program_id: str) -> Optional[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT address FROM program WHERE program_id = %s", (program_id,))
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res['address']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_name_from_address(self, address: str) -> Optional[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT program_id FROM program WHERE address = %s", (address,))
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res['program_id']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise