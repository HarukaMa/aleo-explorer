import asyncio
import logging
import multiprocessing
import os
import time

import uvicorn
from asgi_logger import AccessLoggerMiddleware
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from db import Database


class UvicornServer(multiprocessing.Process):

    def __init__(self, config: uvicorn.Config):
        super().__init__()
        self.server = uvicorn.Server(config=config)
        self.config = config

    def stop(self):
        self.terminate()

    def run(self, *args, **kwargs):
        self.server.run()

async def commitment_route(request: Request):
    if time.time() >= 1675209600:
        return JSONResponse(None)
    commitment = request.query_params.get("commitment")
    if not commitment:
        return HTTPException(400, "Missing commitment")
    return JSONResponse(await db.get_puzzle_commitment(commitment))

routes = [
    Route("/commitment", commitment_route),
]

async def startup():
    async def noop(_): pass

    global db
    # different thread so need to get a new database instance
    db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                  database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                  message_callback=noop)
    await db.connect()


log_format = '\033[92mAPI\033[0m: \033[94m%(client_addr)s\033[0m - - %(t)s \033[96m"%(request_line)s"\033[0m \033[93m%(s)s\033[0m %(B)s "%(f)s" "%(a)s" %(L)s'
# noinspection PyTypeChecker
app = Starlette(
    debug=True if os.environ.get("DEBUG") else False,
    routes=routes,
    on_startup=[startup],
    middleware=[Middleware(AccessLoggerMiddleware, format=log_format)]
)


async def run():
    config = uvicorn.Config("api:app", reload=True, log_level="info", port=int(os.environ.get("API_PORT", 8001)))
    logging.getLogger("uvicorn.access").handlers = []
    server = UvicornServer(config=config)

    server.start()
    while True:
        await asyncio.sleep(3600)
