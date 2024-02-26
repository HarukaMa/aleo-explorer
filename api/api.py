import asyncio
import logging
import multiprocessing
import os
import time
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from api.execute_routes import preview_finalize_route
from api.mapping_routes import mapping_route, mapping_list_route, mapping_value_list_route, mapping_key_count_route
from db import Database
from middleware.api_filter import APIFilterMiddleware
from middleware.api_quota import APIQuotaMiddleware
from middleware.asgi_logger import AccessLoggerMiddleware
from middleware.server_timing import ServerTimingMiddleware
from util.cache import Cache
from util.set_proc_title import set_proc_title


class UvicornServer(multiprocessing.Process):

    def __init__(self, config: uvicorn.Config):
        super().__init__()
        self.server = uvicorn.Server(config=config)
        self.config = config

    def stop(self):
        self.terminate()

    def run(self, *args: Any, **kwargs: Any):
        self.server.run()

async def commitment_route(request: Request):
    db: Database = request.app.state.db
    if time.time() >= 1675209600:
        return JSONResponse(None)
    commitment = request.query_params.get("commitment")
    if not commitment:
        return HTTPException(400, "Missing commitment")
    return JSONResponse(await db.get_puzzle_commitment(commitment))


routes = [
    Route("/commitment", commitment_route),
    Route("/v{version:int}/mapping/get_value/{program_id}/{mapping}/{key}", mapping_route),
    Route("/v{version:int}/mapping/list_program_mappings/{program_id}", mapping_list_route),
    Route("/v{version:int}/mapping/list_program_mapping_values/{program_id}/{mapping}", mapping_value_list_route),
    Route("/v{version:int}/mapping/get_key_count/{program_id}/{mapping}", mapping_key_count_route),
    Route("/v{version:int}/simulate_execution/finalize", preview_finalize_route, methods=["POST"]),
]

async def startup():
    async def noop(_: Any): pass

    # different thread so need to get a new database instance
    db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                  database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                  redis_server=os.environ["REDIS_HOST"], redis_port=int(os.environ["REDIS_PORT"]),
                  redis_db=int(os.environ["REDIS_DB"]),
                  message_callback=noop)
    await db.connect()
    app.state.db = db
    app.state.program_cache = Cache()
    set_proc_title("aleo-explorer: api")

log_format = '\033[92mAPI\033[0m: \033[94m%(client_addr)s\033[0m - - %(t)s \033[96m"%(request_line)s"\033[0m \033[93m%(s)s\033[0m %(B)s "%(f)s" "%(a)s" %(L)s'
# noinspection PyTypeChecker
app = Starlette(
    debug=True if os.environ.get("DEBUG") else False,
    routes=routes,
    on_startup=[startup],
    middleware=[
        Middleware(AccessLoggerMiddleware, format=log_format),
        Middleware(CORSMiddleware, allow_origins=['*']),
        Middleware(ServerTimingMiddleware),
        Middleware(APIQuotaMiddleware),
        Middleware(APIFilterMiddleware),
    ]
)


async def run():
    host = os.environ.get("API_HOST", "127.0.0.1")
    port = int(os.environ.get("API_PORT", 8001))
    config = uvicorn.Config("api:app", reload=True, log_level="info", host=host, port=port)
    logging.getLogger("uvicorn.access").handlers = []
    server = UvicornServer(config=config)

    server.start()
    while True:
        await asyncio.sleep(3600)
