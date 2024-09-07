
from __future__ import annotations

import time

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseAddress(DatabaseBase):

    async def get_puzzle_reward_by_address(self, address: str) -> int:
        data = await self.redis.hget("address_puzzle_reward", address)
        if data is None:
            return 0
        return int(data)

    async def get_recent_solutions_by_address(self, address: str) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, s.counter, s.target, s.solution_id, reward, ps.target_sum "
                        "FROM solution s "
                        "JOIN puzzle_solution ps ON ps.id = s.puzzle_solution_id "
                        "JOIN block b ON b.id = ps.block_id "
                        "WHERE s.address = %s "
                        "ORDER BY ps.id DESC "
                        "LIMIT 30",
                        (address,)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_count_by_address(self, address: str) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(*) FROM solution WHERE address = %s", (address,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_by_address(self, address: str, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, s.counter, s.target, s.solution_id, reward, ps.target_sum "
                        "FROM solution s "
                        "JOIN puzzle_solution ps ON ps.id = s.puzzle_solution_id "
                        "JOIN block b ON b.id = ps.block_id "
                        "WHERE s.address = %s "
                        "ORDER BY ps.id DESC "
                        "LIMIT %s OFFSET %s",
                        (address, end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_by_height(self, height: int, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT s.address, s.counter, s.target, reward, s.solution_id "
                        "FROM solution s "
                        "JOIN puzzle_solution ps on s.puzzle_solution_id = ps.id "
                        "JOIN block b on ps.block_id = b.id "
                        "WHERE b.height = %s "
                        "ORDER BY target DESC "
                        "LIMIT %s OFFSET %s",
                        (height, end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise


    async def get_address_recent_transitions(self, address: str) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        """
WITH ats AS
    (SELECT DISTINCT transition_id
     FROM address_transition
     WHERE address = %s
     ORDER BY transition_id DESC
     LIMIT 30)
SELECT DISTINCT ts.transition_id,
                b.height,
                b.timestamp
FROM ats
JOIN transition ts ON ats.transition_id = ts.id
JOIN transaction_execute te ON te.id = ts.transaction_execute_id
JOIN transaction tx ON tx.id = te.transaction_id
JOIN confirmed_transaction ct ON ct.id = tx.confirmed_transaction_id
JOIN block b ON b.id = ct.block_id
UNION
SELECT DISTINCT ts.transition_id,
                b.height,
                b.timestamp
FROM ats
JOIN transition ts ON ats.transition_id = ts.id
JOIN fee f ON f.id = ts.fee_id
JOIN transaction tx ON tx.id = f.transaction_id
JOIN confirmed_transaction ct ON ct.id = tx.confirmed_transaction_id
JOIN block b ON b.id = ct.block_id
ORDER BY height DESC
LIMIT 30
""",
                        (address,)
                    )
                    def transform(x: dict[str, Any]):
                        return {
                            "transition_id": x["transition_id"],
                            "height": x["height"],
                            "timestamp": x["timestamp"]
                        }
                    return list(map(lambda x: transform(x), await cur.fetchall()))
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_address_stake_reward(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_stake_reward", address)
        if data is None:
            return None
        return int(data)

    async def get_address_transfer_in(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_transfer_in", address)
        if data is None:
            return None
        return int(data)

    async def get_address_transfer_out(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_transfer_out", address)
        if data is None:
            return None
        return int(data)

    async def get_address_total_fee(self, address: str) -> Optional[int]:
        data = await self.redis.hget("address_fee", address)
        if data is None:
            return None
        return int(data)

    async def get_address_speed(self, address: str) -> tuple[float, int]: # (speed, interval)
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                interval_list = [900, 1800, 3600, 14400, 43200, 86400]
                now = int(time.time())
                try:
                    for interval in interval_list:
                        await cur.execute(
                            "SELECT b.height FROM solution s "
                            "JOIN puzzle_solution ps ON s.puzzle_solution_id = ps.id "
                            "JOIN block b ON ps.block_id = b.id "
                            "WHERE address = %s AND timestamp > %s",
                            (address, now - interval)
                        )
                        partial_solutions = await cur.fetchall()
                        if len(partial_solutions) < 10:
                            continue
                        heights = list(map(lambda x: x['height'], partial_solutions))
                        ref_heights = list(map(lambda x: x - 1, set(heights)))
                        await cur.execute(
                            "SELECT height, proof_target FROM block WHERE height = ANY(%s::bigint[])", (ref_heights,)
                        )
                        ref_proof_targets = await cur.fetchall()
                        ref_proof_target_dict = dict(map(lambda x: (x['height'], x['proof_target']), ref_proof_targets))
                        total_solutions = 0
                        for height in heights:
                            total_solutions += ref_proof_target_dict[height - 1]
                        return total_solutions / interval, interval
                    return 0, 0
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_network_speed(self) -> float:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                interval = 900
                try:
                    await cur.execute(
                        "WITH last AS (SELECT timestamp FROM block ORDER BY height DESC LIMIT 1) "
                        "SELECT b.height FROM solution s "
                        "JOIN puzzle_solution ps ON s.puzzle_solution_id = ps.id "
                        "JOIN block b ON ps.block_id = b.id "
                        "WHERE timestamp > (SELECT timestamp FROM last) - %s",
                        (interval,)
                    )
                    partial_solutions = await cur.fetchall()
                    heights = list(map(lambda x: x['height'], partial_solutions))
                    ref_heights = list(map(lambda x: x - 1, set(heights)))
                    await cur.execute(
                        "SELECT height, proof_target FROM block WHERE height = ANY(%s::bigint[])", (ref_heights,)
                    )
                    ref_proof_targets = await cur.fetchall()
                    ref_proof_target_dict = dict(map(lambda x: (x['height'], x['proof_target']), ref_proof_targets))
                    total_solutions = 0
                    for height in heights:
                        total_solutions += ref_proof_target_dict[height - 1]
                    return total_solutions / interval
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_total_solution_count(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT COUNT(*) FROM solution")
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_average_solution_reward(self) -> float:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT AVG(reward) FROM solution"
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    if res["avg"] is None:
                        return 0
                    return res["avg"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_incentive_address_count(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT COUNT(DISTINCT address) FROM solution "
                        "JOIN puzzle_solution ps ON solution.puzzle_solution_id = ps.id "
                        "JOIN block b ON ps.block_id = b.id "
                        "WHERE b.timestamp > 1719849600 AND b.timestamp < 1721059200"
                    )
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_incentive_addresses(self, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT s.address, sum(s.reward) as reward FROM solution s "
                        "JOIN puzzle_solution ps ON s.puzzle_solution_id = ps.id "
                        "JOIN block b ON ps.block_id = b.id "
                        "WHERE b.timestamp > 1719849600 AND b.timestamp < 1721059200 "
                        "GROUP BY s.address "
                        "ORDER BY reward DESC "
                        "LIMIT %s OFFSET %s",
                        (end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_incentive_total_reward(self) -> Decimal:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT sum(reward) FROM solution s "
                        "JOIN puzzle_solution ps ON s.puzzle_solution_id = ps.id "
                        "JOIN block b ON ps.block_id = b.id "
                        "WHERE b.timestamp > 1719849600 AND b.timestamp < 1721059200"
                    )
                    if (res := await cur.fetchone()) is None:
                        return Decimal(0)
                    return res["sum"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_solution_by_id(self, solution_id: str) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT s.address, s.counter, s.target, s.reward, ps.target_sum, b.height, b.timestamp "
                        "FROM solution s "
                        "JOIN puzzle_solution ps ON s.puzzle_solution_id = ps.id "
                        "JOIN block b ON ps.block_id = b.id "
                        "WHERE s.solution_id = %s",
                        (solution_id,)
                    )
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_address_tag(self, address: str) -> Optional[str]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT tag FROM address_tag WHERE address = %s", (address,))
                    if (res := await cur.fetchone()) is None:
                        return None
                    return res["tag"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise
