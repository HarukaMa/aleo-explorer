import datetime
import time
from typing import Callable, Coroutine, Any, Optional

import aiohttp
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from db import Database


async def out_of_sync_check(db: Database) -> bool:
    last_timestamp = await db.get_latest_block_timestamp()
    now = int(time.time())
    if now - last_timestamp > 300:
        return True
    return False

def async_check_sync(func: Callable[..., Coroutine[Any, Any, Response]]):
    async def wrapper(*args: Any, **kwargs: Any):
        if len(args) < 1 or not isinstance(args[0], Request):
            raise TypeError("this decorator cannot be used on this function")
        request: Request = args[0]
        db: Database = request.app.state.db
        forced = request.query_params.get("outdated") == "1"
        if not forced and await out_of_sync_check(db):
            return JSONResponse({"error": "This explorer is out of sync. To ignore this and continue anyway, add ?outdated=1 to the end of URL."}, status_code=500)
        return await func(*args, **kwargs)
    return wrapper

def use_program_cache(func: Callable[..., Coroutine[Any, Any, Response]]):
    async def wrapper(*args: Any, **kwargs: Any):
        if len(args) < 1 or not isinstance(args[0], Request):
            raise TypeError("this decorator cannot be used on this function")
        request: Request = args[0]
        program_cache = request.app.state.program_cache
        kwargs["program_cache"] = program_cache
        return await func(*args, **kwargs)
    return wrapper

async def get_remote_height(session: aiohttp.ClientSession, rpc_root: str) -> Optional[int]:
    try:
        async with session.get(f"{rpc_root}/testnet3/latest/height") as resp:
            if resp.status == 200:
                remote_height = int(await resp.text())
            else:
                remote_height = None
    except:
        remote_height = None
    return remote_height

async def parse_history_params(db: Database, height_param: Optional[str], time_param: Optional[str]):
    if height_param is not None and time_param is not None:
        return JSONResponse({"error": "Only one of height or time can be specified"}, status_code=400)

    if height_param is not None:
        try:
            height = int(height_param)
        except ValueError:
            return JSONResponse({"error": "Invalid height"}, status_code=400)
        block = await db.get_block_by_height(height)
        if block is None:
            return JSONResponse({"error": "Invalid height"}, status_code=400)
        block_timestamp = block.header.metadata.timestamp
    elif time_param is not None:
        try:
            timestamp = int(time_param)
        except ValueError:
            try:
                timestamp = int(datetime.datetime.fromisoformat(time_param).timestamp())
            except ValueError:
                return JSONResponse({"error": "Invalid time"}, status_code=400)
        block = await db.get_block_from_timestamp(timestamp)
        if block is None:
            return JSONResponse({"error": "No block found for the specified time"}, status_code=404)
        height = block.height
        block_timestamp = block.header.metadata.timestamp
    else:
        height = await db.get_latest_height()
        if height is None:
            return JSONResponse({"error": "Database uninitialized"}, status_code=500)
        block = await db.get_block_by_height(height)
        if block is None:
            return JSONResponse({"error": "Database uninitialized"}, status_code=500)
        block_timestamp = block.header.metadata.timestamp

    return height, block, block_timestamp