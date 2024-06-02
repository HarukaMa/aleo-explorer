from starlette.requests import Request
from starlette.responses import JSONResponse

from .utils import out_of_sync_check


async def bad_request(request: Request, exc: Exception):
    db = request.app.state.db
    sync_info = await out_of_sync_check(request.app.state.session, db)
    return JSONResponse({"exc": str(exc), "sync_info": sync_info}, status_code=400)


async def not_found(request: Request, exc: Exception):
    db = request.app.state.db
    sync_info = await out_of_sync_check(request.app.state.session, db)
    return JSONResponse({"exc": str(exc), "sync_info": sync_info}, status_code=404)


async def internal_error(request: Request, exc: Exception):
    db = request.app.state.db
    sync_info = await out_of_sync_check(request.app.state.session, db)
    return JSONResponse({"exc": str(exc), "sync_info": sync_info}, status_code=500)

