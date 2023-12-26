from __future__ import annotations

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseValidator(DatabaseBase):

    async def get_validator_count_at_height(self, height: int) -> Optional[int]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "WHERE ch.height = %s",
                        (height,)
                    )
                    res = await cur.fetchone()
                    if res:
                        return res["count"]
                    else:
                        return None
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_validators_range_at_height(self, height: int, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT chm.address, chm.stake FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "WHERE ch.height = %s "
                        "ORDER BY chm.stake DESC "
                        "LIMIT %s OFFSET %s",
                        (height, end - start, start)
                    )
                    validators = await cur.fetchall()
                    print(start, end)
                    await cur.execute("SELECT timestamp FROM block WHERE height = %s", (height,))
                    res = await cur.fetchone()
                    if res:
                        timestamp = res["timestamp"]
                    else:
                        return []
                    await cur.execute(
                        "WITH va AS "
                        "    (SELECT unnest(array_agg(DISTINCT d.author)) AS author "
                        "     FROM BLOCK b "
                        "     JOIN authority a ON a.block_id = b.id "
                        "     JOIN dag_vertex d ON d.authority_id = a.id "
                        "     WHERE b.timestamp > %s "
                        "     GROUP BY d.authority_id) "
                        "SELECT author, count(author) FROM va "
                        "GROUP BY author",
                        (timestamp - 86400,)
                    )
                    res = await cur.fetchall()
                    validator_counts = {v["author"]: v["count"] for v in res}
                    await cur.execute(
                        "SELECT count(*) FROM block WHERE timestamp > %s",
                        (timestamp - 86400,)
                    )
                    res = await cur.fetchone()
                    if res:
                        block_count = res["count"]
                    else:
                        return []
                    for validator in validators:
                        validator["uptime"] = validator_counts.get(validator["address"], 0) / block_count

                    return validators
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_validator_uptime(self, address: str) -> Optional[float]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT timestamp FROM block ORDER BY height DESC LIMIT 1")
                    res = await cur.fetchone()
                    if res:
                        timestamp = res["timestamp"]
                    else:
                        return None
                    await cur.execute(
                        "WITH va AS "
                        "    (SELECT unnest(array_agg(DISTINCT d.author)) AS author "
                        "     FROM BLOCK b "
                        "     JOIN authority a ON a.block_id = b.id "
                        "     JOIN dag_vertex d ON d.authority_id = a.id "
                        "     WHERE b.timestamp > %s "
                        "     GROUP BY d.authority_id) "
                        "SELECT author, count(author) FROM va "
                        "GROUP BY author",
                        (timestamp - 86400,)
                    )
                    res = await cur.fetchall()
                    validator_counts = {v["author"]: v["count"] for v in res}
                    if address not in validator_counts:
                        return 0
                    await cur.execute(
                        "SELECT count(*) FROM block WHERE timestamp > %s",
                        (timestamp - 86400,)
                    )
                    res = await cur.fetchone()
                    if res:
                        block_count = res["count"]
                    else:
                        return None
                    return validator_counts[address] / block_count
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_address_stakers(self, address: str) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT address, stake FROM staker WHERE validator = %s",
                        (address,)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_network_participation_rate(self) -> float:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT timestamp FROM block ORDER BY height DESC LIMIT 1")
                    res = await cur.fetchone()
                    if res:
                        timestamp = res["timestamp"]
                    else:
                        return 0
                    await cur.execute(
                        "WITH va AS "
                        "    (SELECT unnest(array_agg(DISTINCT d.author)) AS author "
                        "     FROM BLOCK b "
                        "     JOIN authority a ON a.block_id = b.id "
                        "     JOIN dag_vertex d ON d.authority_id = a.id "
                        "     WHERE b.timestamp > %s "
                        "     GROUP BY d.authority_id) "
                        "SELECT count(author) FROM va",
                        (timestamp - 3600,)
                    )
                    res = await cur.fetchone()
                    if res:
                        validator_count = res["count"]
                    else:
                        return 0
                    await cur.execute(
                        "SELECT count(*) FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "JOIN block b ON ch.height = b.height "
                        "WHERE b.timestamp > %s",
                        (timestamp - 3600,)
                    )
                    res = await cur.fetchone()
                    if res:
                        total_validator_count = res["count"]
                    else:
                        return 0
                    return validator_count / total_validator_count
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise