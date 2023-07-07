import time

from starlette.requests import Request
from starlette.responses import JSONResponse

from db import Database


async def out_of_sync_check(db: Database) -> bool:
    last_timestamp = await db.get_latest_block_timestamp()
    now = int(time.time())
    if now - last_timestamp > 120:
        return True
    return False

def async_check_sync(func):
    async def wrapper(*args, **kwargs):
        if len(args) < 1 or not isinstance(args[0], Request):
            raise TypeError("this decorator cannot be used on this function")
        request: Request = args[0]
        db: Database = request.app.state.db
        forced = request.query_params.get("outdated") == "1"
        if not forced and await out_of_sync_check(db):
            return JSONResponse({"error": "This explorer is out of sync. To ignore this and continue anyway, add ?outdated=1 to the end of URL."}, status_code=500)
        return await func(*args, **kwargs)
    return wrapper

def use_program_cache(func):
    async def wrapper(*args, **kwargs):
        if len(args) < 1 or not isinstance(args[0], Request):
            raise TypeError("this decorator cannot be used on this function")
        request: Request = args[0]
        program_cache = request.app.state.program_cache
        kwargs["program_cache"] = program_cache
        return await func(*args, **kwargs)
    return wrapper