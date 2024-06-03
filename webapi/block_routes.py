from starlette.requests import Request

from db import Database
from webapi.utils import SJSONResponse


async def recent_blocks_route(request: Request):
    db: Database = request.app.state.db
    recent_blocks = await db.get_recent_blocks_fast(10)
    return SJSONResponse(recent_blocks)