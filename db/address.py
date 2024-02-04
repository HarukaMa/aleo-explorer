
from __future__ import annotations

import time

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseAddress(DatabaseBase):

    async def get_leaderboard_size(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT COUNT(*) FROM leaderboard")
                    if (res := await cur.fetchone()) is None:
                        return 0
                    return res["count"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_leaderboard(self, start: int, end: int) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT * FROM leaderboard "
                        "ORDER BY total_incentive DESC, total_reward DESC "
                        "LIMIT %s OFFSET %s",
                        (end - start, start)
                    )
                    return await cur.fetchall()
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_leaderboard_rewards_by_address(self, address: str) -> tuple[int, int]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT total_reward, total_incentive FROM leaderboard WHERE address = %s", (address,)
                    )
                    row = await cur.fetchone()
                    if row is None:
                        return 0, 0
                    return row["total_reward"], row["total_incentive"]
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_recent_solutions_by_address(self, address: str) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT b.height, b.timestamp, ps.nonce, ps.target, reward, cs.target_sum "
                        "FROM prover_solution ps "
                        "JOIN coinbase_solution cs ON cs.id = ps.coinbase_solution_id "
                        "JOIN block b ON b.id = cs.block_id "
                        "WHERE ps.address = %s "
                        "ORDER BY cs.id DESC "
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
                        "SELECT COUNT(*) FROM prover_solution WHERE address = %s", (address,)
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
                        "SELECT b.height, b.timestamp, ps.nonce, ps.target, reward, cs.target_sum "
                        "FROM prover_solution ps "
                        "JOIN coinbase_solution cs ON cs.id = ps.coinbase_solution_id "
                        "JOIN block b ON b.id = cs.block_id "
                        "WHERE ps.address = %s "
                        "ORDER BY cs.id DESC "
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
                        "SELECT ps.address, ps.nonce, ps.commitment, ps.target, reward "
                        "FROM prover_solution ps "
                        "JOIN coinbase_solution cs on ps.coinbase_solution_id = cs.id "
                        "JOIN block b on cs.block_id = b.id "
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
JOIN confirmed_transaction ct ON ct.id = tx.confimed_transaction_id
JOIN block b ON b.id = ct.block_id
UNION
SELECT DISTINCT ts.transition_id,
                b.height,
                b.timestamp
FROM ats
JOIN transition ts ON ats.transition_id = ts.id
JOIN fee f ON f.id = ts.fee_id
JOIN transaction tx ON tx.id = f.transaction_id
JOIN confirmed_transaction ct ON ct.id = tx.confimed_transaction_id
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
                            "SELECT b.height FROM prover_solution ps "
                            "JOIN coinbase_solution cs ON ps.coinbase_solution_id = cs.id "
                            "JOIN block b ON cs.block_id = b.id "
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
                now = int(time.time())
                interval = 900
                try:
                    await cur.execute(
                        "SELECT b.height FROM prover_solution ps "
                        "JOIN coinbase_solution cs ON ps.coinbase_solution_id = cs.id "
                        "JOIN block b ON cs.block_id = b.id "
                        "WHERE timestamp > %s",
                        (now - interval,)
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

    async def get_leaderboard_total(self) -> int:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT total_credit FROM leaderboard_total")
                    total_credit = await cur.fetchone()
                    if total_credit is None:
                        await cur.execute("INSERT INTO leaderboard_total (total_credit) VALUES (0)")
                        return 0
                    return int(total_credit["total_credit"])
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_puzzle_commitment(self, commitment: str) -> Optional[dict[str, Any]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT reward, height FROM prover_solution "
                        "JOIN coinbase_solution cs on cs.id = prover_solution.coinbase_solution_id "
                        "JOIN block b on b.id = cs.block_id "
                        "WHERE commitment = %s",
                        (commitment,)
                    )
                    row = await cur.fetchone()
                    if row is None:
                        return None
                    return {
                        'reward': row['reward'],
                        'height': row['height']
                    }
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise