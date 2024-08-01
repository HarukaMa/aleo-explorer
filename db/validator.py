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
                        "SELECT validator, count(validator) FROM block_validator bv "
                        "JOIN block b ON bv.block_id = b.id "
                        "WHERE b.timestamp > %s "
                        "GROUP BY validator",
                        (timestamp - 86400,)
                    )
                    res = await cur.fetchall()
                    validator_counts = {v["validator"]: v["count"] for v in res}
                    await cur.execute(
                        "SELECT address, count(chm.address) FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "JOIN block b ON ch.height = b.height "
                        "WHERE b.timestamp > %s "
                        "GROUP BY address",
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
                        "SELECT count(validator) FROM block_validator bv "
                        "JOIN block b ON bv.block_id = b.id "
                        "WHERE b.timestamp > %s AND validator = %s",
                        (timestamp - 86400, address)
                    )
                    res = await cur.fetchone()
                    if res:
                        validator_counts = res["count"]
                    else:
                        validator_counts = 0
                    await cur.execute(
                        "SELECT count(chm.address) FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "JOIN block b ON ch.height = b.height "
                        "WHERE b.timestamp > %s AND chm.address = %s",
                        (timestamp - 86400, address)
                    )
                    res = await cur.fetchone()
                    if res:
                        block_count = res["count"]
                    else:
                        return None
                    return validator_counts / block_count
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_current_validator_count(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, COUNT(*) FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "JOIN block b ON ch.height = b.height "
                        "GROUP BY b.height ORDER BY b.height DESC LIMIT 1"
                    )
                    res = await cur.fetchone()
                    if res is not None:
                        return res["count"]
                    else:
                        return 0
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
                        "SELECT count(validator) FROM block_validator bv "
                        "JOIN block b ON bv.block_id = b.id "
                        "WHERE b.timestamp > %s",
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

    # returns: validators, all_validators_data
    async def get_validator_by_height(self, height: int) -> tuple[list[str], list[dict[str, Any]]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT validator FROM block_validator bv "
                        "JOIN block b ON bv.block_id = b.id "
                        "WHERE b.height = %s ",
                        (height,)
                    )
                    validators: list[str] = []
                    for row in await cur.fetchall():
                        validators.append(row["validator"])
                    await cur.execute(
                        "SELECT chm.* FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "WHERE ch.height = %s ORDER BY stake DESC",
                        (height,)
                    )
                    return validators, await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise