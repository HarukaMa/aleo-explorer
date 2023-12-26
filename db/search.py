
from __future__ import annotations

from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseSearch(DatabaseBase):


    async def search_block_hash(self, block_hash: str) -> list[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT block_hash FROM block WHERE block_hash LIKE %s", (f"{block_hash}%",))
                    result = await cur.fetchall()
                    return list(map(lambda x: x['block_hash'], result))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_transaction_id(self, transaction_id: str) -> list[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT transaction_id FROM transaction WHERE transaction_id LIKE %s", (f"{transaction_id}%",))
                    result = await cur.fetchall()
                    return list(map(lambda x: x['transaction_id'], result))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_transition_id(self, transition_id: str) -> list[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT transition_id FROM transition WHERE transition_id LIKE %s", (f"{transition_id}%",))
                    result = await cur.fetchall()
                    return list(map(lambda x: x['transition_id'], result))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def search_address(self, address: str) -> list[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT DISTINCT address FROM leaderboard WHERE address LIKE %s", (f"{address}%",)
                    )
                    res = set(map(lambda x: x['address'], await cur.fetchall()))
                    await cur.execute(
                        "SELECT DISTINCT owner FROM program WHERE owner LIKE %s", (f"{address}%",)
                    )
                    res.update(set(map(lambda x: x['owner'], await cur.fetchall())))
                    await cur.execute(
                        "SELECT DISTINCT address FROM address_transition WHERE address LIKE %s", (f"{address}%",)
                    )
                    res.update(set(map(lambda x: x['address'], await cur.fetchall())))
                    return list(res)
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def search_program(self, program_id: str) -> list[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT program_id FROM program WHERE program_id LIKE %s", (f"{program_id}%",)
                    )
                    return list(map(lambda x: x['program_id'], await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise