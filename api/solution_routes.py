
from starlette.requests import Request
from starlette.responses import JSONResponse

from db import Database


async def solution_by_id_route(request: Request):
    db: Database = request.app.state.db
    solution_id = request.path_params["solution_id"]
    data = await db.get_solution_by_id(solution_id)
    if data is None:
        return JSONResponse({"error": "Solution not found"}, status_code=404)
    return JSONResponse(data)