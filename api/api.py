import asyncio
import logging
import multiprocessing
import os
import time
from typing import Any

import aiohttp
import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from db import Database
from middleware.api_filter import APIFilterMiddleware
from middleware.api_quota import APIQuotaMiddleware
from middleware.asgi_logger import AccessLoggerMiddleware
from middleware.server_timing import ServerTimingMiddleware
from util.cache import Cache
from util.set_proc_title import set_proc_title
from .address_routes import address_staking_route, address_delegated_route
from .execute_routes import preview_finalize_route
from .mapping_routes import mapping_route, mapping_list_route, mapping_value_list_route, mapping_key_count_route
from .solution_routes import solution_by_id_route
from .utils import get_remote_height


class UvicornServer(multiprocessing.Process):

    def __init__(self, config: uvicorn.Config):
        super().__init__()
        self.server = uvicorn.Server(config=config)
        self.config = config

    def stop(self):
        self.terminate()

    def run(self, *args: Any, **kwargs: Any):
        self.server.run()

async def status_route(request: Request):
    session = request.app.state.session
    db: Database = request.app.state.db
    version = request.path_params["version"]
    if version < 2:
        return JSONResponse({"error": "This endpoint is not supported in this version"}, status_code=400)
    node_height = None
    reference_height = None
    if rpc_root := os.environ.get("RPC_URL_ROOT"):
        node_height = await get_remote_height(session, rpc_root)
    if ref_rpc_root := os.environ.get("REF_RPC_URL_ROOT"):
        reference_height = await get_remote_height(session, ref_rpc_root)
    latest_block_height = await db.get_latest_height()
    latest_block_timestamp = await db.get_latest_block_timestamp()
    res = {
        "server_time": int(time.time()),
        "latest_block_height": latest_block_height,
        "latest_block_timestamp": latest_block_timestamp,
        "node_height": node_height,
        "reference_height": reference_height,
    }
    return JSONResponse(res)


routes = [
    Route("/v{version:int}/address/staking_info/{address}", address_staking_route),
    Route("/v{version:int}/address/delegated/{address}", address_delegated_route),
    Route("/v{version:int}/mapping/get_value/{program_id}/{mapping}/{key}", mapping_route),
    Route("/v{version:int}/mapping/list_program_mappings/{program_id}", mapping_list_route),
    Route("/v{version:int}/mapping/list_program_mapping_values/{program_id}/{mapping}", mapping_value_list_route),
    Route("/v{version:int}/mapping/get_key_count/{program_id}/{mapping}", mapping_key_count_route),
    Route("/v{version:int}/simulate_execution/finalize", preview_finalize_route, methods=["POST"]),
    Route("/v{version:int}/solution/{solution_id}", solution_by_id_route),
    Route("/v{version:int}/status", status_route),

]

async def startup():
    async def noop(_: Any): pass

    # different thread so need to get a new database instance
    db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                  database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                  redis_server=os.environ["REDIS_HOST"], redis_port=int(os.environ["REDIS_PORT"]),
                  redis_db=int(os.environ["REDIS_DB"]), redis_user=os.environ.get("REDIS_USER"),
                  redis_password=os.environ.get("REDIS_PASS"),
                  message_callback=noop)
    await db.connect()
    app.state.db = db
    app.state.program_cache = Cache()
    app.state.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1))
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
    config = uvicorn.Config(
        "api:app", reload=True, log_level="info", host=host, port=port,
        forwarded_allow_ips=["127.0.0.1", "::1", "10.0.4.1", "10.0.5.1"]
    )
    logging.getLogger("uvicorn.access").handlers = []
    server = UvicornServer(config=config)

    server.start()
    while True:
        await asyncio.sleep(3600)
