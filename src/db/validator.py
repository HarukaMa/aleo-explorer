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
                        "SELECT chm.address, chm.stake, chm.commission, chm.is_open FROM committee_history_member chm "
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
                    res = await cur.fetchall()
                    validator_in_counts = {v["address"]: v["count"] for v in res}
                    for validator in validators:
                        validator["uptime"] = validator_counts.get(validator["address"], 0) / validator_in_counts.get(validator["address"], 1)

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
                        "SELECT height FROM block WHERE timestamp > %s ORDER BY timestamp LIMIT 1",
                        (timestamp - 86400,)
                    )
                    res = await cur.fetchone()
                    if res:
                        height = res["height"]
                    else:
                        return None
                    await cur.execute(
                        "SELECT count(chm.address) FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "WHERE ch.height > %s AND chm.address = %s",
                        (height, address)
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

    async def get_validator_uptime_windows(self, address: str) -> dict[str, Optional[float]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT timestamp FROM block ORDER BY height DESC LIMIT 1")
                    res = await cur.fetchone()
                    if res is None:
                        return {"24h": None, "7d": None, "30d": None}
                    now_ts = res["timestamp"]
                    await cur.execute(
                        "SELECT "
                        "(SELECT id FROM block WHERE timestamp > %s ORDER BY timestamp LIMIT 1) AS bid_24h, "
                        "(SELECT id FROM block WHERE timestamp > %s ORDER BY timestamp LIMIT 1) AS bid_7d, "
                        "(SELECT id FROM block WHERE timestamp > %s ORDER BY timestamp LIMIT 1) AS bid_30d, "
                        "(SELECT id FROM committee_history WHERE height >= "
                        "  (SELECT height FROM block WHERE timestamp > %s ORDER BY timestamp LIMIT 1) "
                        "  ORDER BY height LIMIT 1) AS chid_24h, "
                        "(SELECT id FROM committee_history WHERE height >= "
                        "  (SELECT height FROM block WHERE timestamp > %s ORDER BY timestamp LIMIT 1) "
                        "  ORDER BY height LIMIT 1) AS chid_7d, "
                        "(SELECT id FROM committee_history WHERE height >= "
                        "  (SELECT height FROM block WHERE timestamp > %s ORDER BY timestamp LIMIT 1) "
                        "  ORDER BY height LIMIT 1) AS chid_30d",
                        (now_ts - 86400, now_ts - 604800, now_ts - 2592000,
                         now_ts - 86400, now_ts - 604800, now_ts - 2592000)
                    )
                    bounds = await cur.fetchone()
                    if bounds is None or bounds["bid_30d"] is None or bounds["chid_30d"] is None:
                        return {"24h": None, "7d": None, "30d": None}
                    await cur.execute(
                        "SELECT "
                        "count(*) FILTER (WHERE block_id >= %s) AS sg_24h, "
                        "count(*) FILTER (WHERE block_id >= %s) AS sg_7d, "
                        "count(*) AS sg_30d "
                        "FROM block_validator "
                        "WHERE validator = %s AND block_id >= %s",
                        (bounds["bid_24h"], bounds["bid_7d"], address, bounds["bid_30d"])
                    )
                    sg = await cur.fetchone()
                    await cur.execute(
                        "SELECT "
                        "count(*) FILTER (WHERE committee_id >= %s) AS in_24h, "
                        "count(*) FILTER (WHERE committee_id >= %s) AS in_7d, "
                        "count(*) AS in_30d "
                        "FROM committee_history_member "
                        "WHERE address = %s AND committee_id >= %s",
                        (bounds["chid_24h"], bounds["chid_7d"], address, bounds["chid_30d"])
                    )
                    inc = await cur.fetchone()
                    if sg is None or inc is None:
                        return {"24h": None, "7d": None, "30d": None}
                    def ratio(s: int, i: int) -> Optional[float]:
                        return s / i if i > 0 else None
                    return {
                        "24h": ratio(sg["sg_24h"], inc["in_24h"]),
                        "7d": ratio(sg["sg_7d"], inc["in_7d"]),
                        "30d": ratio(sg["sg_30d"], inc["in_30d"]),
                    }
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise

    async def get_validator_participation_buckets(
        self, address: str, window_seconds: int = 86400, bucket_count: int = 144
    ) -> list[dict[str, int]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT timestamp FROM block ORDER BY height DESC LIMIT 1")
                    res = await cur.fetchone()
                    if res is None:
                        return [{"bucket": i, "total_blocks": 0, "in_committee": 0, "signed": 0} for i in range(bucket_count)]
                    now_ts = res["timestamp"]
                    start_ts = now_ts - window_seconds
                    bucket_size = window_seconds // bucket_count

                    await cur.execute(
                        "SELECT LEAST(((timestamp - %s) / %s)::int, %s) AS bucket, count(*) AS total "
                        "FROM block WHERE timestamp > %s GROUP BY bucket",
                        (start_ts, bucket_size, bucket_count - 1, start_ts)
                    )
                    totals = {row["bucket"]: row["total"] for row in await cur.fetchall()}

                    await cur.execute(
                        "SELECT id, height FROM block WHERE timestamp > %s ORDER BY timestamp LIMIT 1",
                        (start_ts,)
                    )
                    block_bound = await cur.fetchone()
                    if block_bound is None:
                        signed: dict[int, int] = {}
                        in_committee: dict[int, int] = {}
                    else:
                        min_block_id = block_bound["id"]
                        min_height = block_bound["height"]
                        await cur.execute(
                            "SELECT LEAST(((b.timestamp - %s) / %s)::int, %s) AS bucket, count(*) AS signed "
                            "FROM block_validator bv JOIN block b ON b.id = bv.block_id "
                            "WHERE bv.validator = %s AND bv.block_id >= %s "
                            "GROUP BY bucket",
                            (start_ts, bucket_size, bucket_count - 1, address, min_block_id)
                        )
                        signed = {row["bucket"]: row["signed"] for row in await cur.fetchall()}

                        await cur.execute(
                            "SELECT id FROM committee_history WHERE height >= %s ORDER BY height LIMIT 1",
                            (min_height,)
                        )
                        ch_bound = await cur.fetchone()
                        if ch_bound is None:
                            in_committee = {}
                        else:
                            await cur.execute(
                                "SELECT LEAST(((b.timestamp - %s) / %s)::int, %s) AS bucket, count(*) AS in_committee "
                                "FROM committee_history_member chm "
                                "JOIN committee_history ch ON ch.id = chm.committee_id "
                                "JOIN block b ON b.height = ch.height "
                                "WHERE chm.address = %s AND chm.committee_id >= %s "
                                "GROUP BY bucket",
                                (start_ts, bucket_size, bucket_count - 1, address, ch_bound["id"])
                            )
                            in_committee = {row["bucket"]: row["in_committee"] for row in await cur.fetchall()}

                    result: list[dict[str, int]] = []
                    for i in range(bucket_count):
                        result.append({
                            "bucket": i,
                            "total_blocks": totals.get(i, 0),
                            "in_committee": in_committee.get(i, 0),
                            "signed": signed.get(i, 0),
                        })
                    return result
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
                        "SELECT sum(stake) FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "JOIN block b ON ch.height = b.height "
                        "WHERE b.timestamp > %s",
                        (timestamp - 300,)
                    )
                    res = await cur.fetchone()
                    if res:
                        validator_total_stake_count = res["sum"]
                    else:
                        return 0
                    await cur.execute(
                        "SELECT sum(stake) FROM committee_history_member chm "
                        "JOIN committee_history ch ON chm.committee_id = ch.id "
                        "JOIN block b ON ch.height = b.height "
                        "JOIN block_validator bv ON b.id = bv.block_id and bv.validator = chm.address "
                        "WHERE b.timestamp > %s",
                        (timestamp - 300,)
                    )
                    res = await cur.fetchone()
                    if res:
                        validator_stake_count = res["sum"]
                    else:
                        return 0
                    if validator_stake_count is None or validator_total_stake_count is None:
                        return 0
                    return validator_stake_count / validator_total_stake_count
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

    async def get_validator_link_and_logo(self, address: str) -> tuple[Optional[str], Optional[str]]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("SELECT website, logo FROM validator_info WHERE address = %s", (address,))
                    res = await cur.fetchone()
                    if res:
                        return res["website"], res["logo"]
                    else:
                        return None, None
                except Exception as e:
                    await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                    raise