import asyncio
import contextlib
import logging
import os
import threading
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


# from node.light_node import LightNodeState


class Server(uvicorn.Server):
    # https://stackoverflow.com/a/64521239
    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        thread = threading.Thread(target=self.run)
        thread.start()
        try:
            while not self.started:
                time.sleep(1e-3)
            yield
        finally:
            self.should_exit = True
            thread.join()

async def commitment_route(request: Request):
    commitment = request.query_params.get("commitment")
    if not commitment:
        return HTTPException(400, "Missing commitment")
    reward = await db.get_puzzle_commitment_reward(commitment)
    if reward is None:
        return JSONResponse(None)
    return JSONResponse(reward)

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


AccessLoggerMiddleware.DEFAULT_FORMAT = '\033[92mAPI\033[0m: \033[94m%(client_addr)s\033[0m - - %(t)s \033[96m"%(request_line)s"\033[0m \033[93m%(s)s\033[0m %(B)s "%(f)s" "%(a)s" %(L)s'
# noinspection PyTypeChecker
app = Starlette(
    debug=True if os.environ.get("DEBUG") else False,
    routes=routes,
    on_startup=[startup],
    middleware=[Middleware(AccessLoggerMiddleware)]
)
db: Database


async def run():
    config = uvicorn.Config("webui:app", reload=True, log_level="info", port=int(os.environ.get("API_PORT", 8001)))
    logging.getLogger("uvicorn.access").handlers = []
    server = Server(config=config)

    with server.run_in_thread():
        while True:
            await asyncio.sleep(3600)
