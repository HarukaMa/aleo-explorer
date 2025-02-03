from __future__ import annotations

import os
from asyncio import iscoroutinefunction
from typing import Awaitable, ParamSpec

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from aleo_types import *
from explorer.types import Message as ExplorerMessage

try:
    from line_profiler import profile
except ImportError:
    P = ParamSpec('P')
    R = TypeVar('R')
    def profile(func: Callable[P, Awaitable[R]] | Callable[P, R]) -> Callable[P, Awaitable[R]] | Callable[P, R]:
        if iscoroutinefunction(func):
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                return await func(*args, **kwargs)
            return async_wrapper
        else:
            func = cast(Callable[P, R], func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                return func(*args, **kwargs)
            return wrapper

class DatabaseBase:

    def __init__(self, *, server: str, user: str, password: str, database: str, schema: str,
                 message_callback: Callable[[ExplorerMessage], Awaitable[None]]):
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.message_callback = message_callback

        self.pool: AsyncConnectionPool[AsyncConnection[DictRow]]

    async def connect(self):
        try:
            self.pool = AsyncConnectionPool(
                f"host={self.server} user={self.user} password={self.password} dbname={self.database} "
                f"options=-csearch_path={self.schema} application_name=aleo-explorer-{os.environ.get('NETWORK', 'unknown')}",
                kwargs={
                    "row_factory": dict_row,
                },
                max_size=16,
            )
        except Exception as e:
            await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseConnectError, e))
            return
        await self.message_callback(ExplorerMessage(ExplorerMessage.Type.DatabaseConnected, None))

