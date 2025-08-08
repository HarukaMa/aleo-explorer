from __future__ import annotations

from psycopg.rows import DictRow

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseProgram(DatabaseBase):


    async def get_function_definition(self, program_id: str, function_name: str, edition: int) -> Optional[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT input, input_mode, output, output_mode, finalize FROM program_function "
                        "JOIN program ON program.id = program_function.program_id "
                        "WHERE program.program_id = %s AND name = %s AND edition = %s",
                        (program_id, function_name, edition)
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
                            "SELECT COUNT(DISTINCT program_id) FROM program "
                            "WHERE feature_hash NOT IN (SELECT hash FROM program_filter_hash)"
                        )
                    else:
                        await cur.execute("SELECT COUNT(DISTINCT program_id) FROM program")
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
                        "SELECT p.program_id, b.height, t.transaction_id, SUM(pf.called) as called, p.edition "
                        "FROM program p "
                        "JOIN ("
                        "  SELECT program_id, MAX(edition) as edition "
                        "  FROM program "
                        "  GROUP BY program_id"
                        ") p2 on p.program_id = p2.program_id AND p.edition = p2.edition "
                        "JOIN transaction_deploy td on p.transaction_deploy_id = td.id "
                        "JOIN transaction t on td.transaction_id = t.id "
                        "JOIN confirmed_transaction ct on t.confirmed_transaction_id = ct.id "
                        "JOIN block b on ct.block_id = b.id "
                        "JOIN program_function pf on p.id = pf.program_id "
                        f"{where}"
                        "GROUP BY p.program_id, b.height, p.id, t.transaction_id, p.edition "
                        "ORDER BY p.id DESC "
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
                        "SELECT p.program_id, SUM(pf.called) as called, p.edition "
                        "FROM program p "
                        "JOIN ("
                        "  SELECT program_id, MAX(edition) as edition "
                        "  FROM program "
                        "  GROUP BY program_id"
                        ") p2 on p.program_id = p2.program_id AND p.edition = p2.edition "
                        "JOIN program_function pf on p.id = pf.program_id "
                        "WHERE p.transaction_deploy_id IS NULL "
                        "GROUP BY p.program_id, p.edition "
                        "LIMIT 1"
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
                        "JOIN ("
                        "  SELECT program_id, MAX(edition) as edition "
                        "  FROM program "
                        "  GROUP BY program_id"
                        ") p2 on p.program_id = p2.program_id AND p.edition = p2.edition "
                        "JOIN transaction_deploy td on p.transaction_deploy_id = td.id "
                        "JOIN transaction t on td.transaction_id = t.id "
                        "JOIN confirmed_transaction ct on t.confirmed_transaction_id = ct.id "
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

    async def get_program_latest_edition(self, program_id: str) -> int | None:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT MAX(edition) as edition FROM program WHERE program_id = %s",
                        (program_id,)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    return res["edition"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_block_by_program_id(self, program_id: str, edition: int) -> Block | None:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT height FROM transaction tx "
                        "JOIN transaction_deploy td on tx.id = td.transaction_id "
                        "JOIN program p on td.id = p.transaction_deploy_id "
                        "JOIN confirmed_transaction ct on ct.id = tx.confirmed_transaction_id "
                        "JOIN block b on ct.block_id = b.id "
                        "WHERE p.program_id = %s and p.edition = %s",
                        (program_id, edition)
                    )
                    height = await cur.fetchone()
                    if height is None:
                        return None
                    return await cast("Database", self).get_block_by_height(height["height"])
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_deploy_info_by_program_id(self, program_id: str, edition: int) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, t.transaction_id FROM block b "
                        "JOIN confirmed_transaction ct on b.id = ct.block_id "
                        "JOIN transaction t on ct.id = t.confirmed_transaction_id "
                        "JOIN transaction_deploy td on t.id = td.transaction_id "
                        "JOIN program p on td.id = p.transaction_deploy_id "
                        "WHERE p.program_id = %s AND p.edition = %s",
                        (program_id, edition)
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
                        "JOIN confirmed_transaction ct on t.confirmed_transaction_id = ct.id "
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

    async def get_program_similar_count(self, program_id: str, edition: int) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM program "
                        "WHERE feature_hash = (SELECT feature_hash FROM program WHERE program_id = %s AND edition = %s)",
                        (program_id, edition)
                    )
                    if (res := await cur.fetchone()) is None:
                        raise ValueError(f"Program {program_id} not found")
                    return res['count'] - 1
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_program_feature_hash(self, program_id: str, edition: int) -> Optional[bytes]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT feature_hash FROM program WHERE program_id = %s AND edition = %s",
                        (program_id, edition)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res['feature_hash']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_recent_programs_by_address(self, address: str) -> list[DictRow]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT program_id, edition FROM program WHERE owner = %s ORDER BY id DESC LIMIT 30", (address,)
                    )
                    return await cur.fetchall()
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

    async def get_program(self, program_id: str, edition: int) -> Optional[bytes]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT raw_data FROM program WHERE program_id = %s AND edition = %s",
                        (program_id, edition)
                    )
                    res = await cur.fetchone()
                    if res is None:
                        return None
                    return res['raw_data']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_program_leo_source_code(self, program_id: str, edition: int) -> Optional[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT leo_source FROM program WHERE program_id = %s AND edition = %s",
                        (program_id, edition)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res['leo_source']
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def store_program_leo_source_code(self, program_id: str, edition: int, source_code: str):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "UPDATE program SET leo_source = %s WHERE program_id = %s AND edition = %s",
                        (source_code, program_id, edition)
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

    async def get_program_owner(self, program_id: str, edition: int) -> Optional[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT owner, checksum FROM program WHERE program_id = %s AND edition = %s",
                        (program_id, edition)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None
                    if res["checksum"] is None:
                        return None
                    return res["owner"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise