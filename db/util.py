from __future__ import annotations

from typing import cast

from aleo_types import *
from explorer.types import Message as ExplorerMessage
from .base import DatabaseBase


class DatabaseUtil(DatabaseBase):

    @staticmethod
    def get_addresses_from_struct(plaintext: StructPlaintext):
        addresses: set[str] = set()
        for _, p in plaintext.members:
            if isinstance(p, LiteralPlaintext) and p.literal.type == Literal.Type.Address:
                addresses.add(str(p.literal.primitive))
            elif isinstance(p, StructPlaintext):
                addresses.update(DatabaseUtil.get_addresses_from_struct(p))
        return addresses

    @staticmethod
    def get_primitive_from_argument_unchecked(argument: Argument):
        plaintext = cast(PlaintextArgument, cast(PlaintextArgument, argument).plaintext)
        literal = cast(LiteralPlaintext, plaintext).literal
        return literal.primitive

    # debug method
    async def clear_database(self):
        async with self.pool.connection() as conn:
            try:
                await conn.execute("TRUNCATE TABLE block RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE mapping RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE committee_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE committee_history_member RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE leaderboard RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE mapping_bonded_history RESTART IDENTITY CASCADE")
                await conn.execute("TRUNCATE TABLE ratification_genesis_balance RESTART IDENTITY CASCADE")
                await self.redis.flushall()
            except Exception as e:
                await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseError, e))
                raise