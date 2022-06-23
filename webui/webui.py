import asyncio
import contextlib
import os
import threading
import time

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from db import Database


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


templates = Jinja2Templates(directory='webui/templates')


async def index(request: Request):
    recent_blocks = await db.get_recent_canonical_blocks_fast()
    data = []
    for block in recent_blocks:
        b = {
            "timestamp": block["timestamp"],
            "height": block["height"],
            "transactions": block["transaction_count"],
            "transitions": block["transition_count"],
            "owner": await db.get_miner_from_block_hash(block["block_hash"])
        }
        data.append(b)
    ctx = {
        "latest_block": await db.get_latest_canonical_block_fast(),
        "request": request,
        "recent_blocks": data,
    }
    return templates.TemplateResponse('index.jinja2', ctx)


routes = [
    Route("/", index),
    # Route("/miner", miner_stats),
    # Route("/calc", calc),
]


async def startup():
    async def noop(_): pass

    global db
    # different thread so need to get a new database instance
    db = Database(server=os.environ["DB_HOST"], user=os.environ["DB_USER"], password=os.environ["DB_PASS"],
                  database=os.environ["DB_DATABASE"], schema=os.environ["DB_SCHEMA"],
                  message_callback=noop)
    await db.connect()


app = Starlette(debug=True, routes=routes, on_startup=[startup])
db: Database


async def run():
    config = uvicorn.Config("webui:app", reload=True, log_level="info")
    server = Server(config=config)

    with server.run_in_thread():
        while True:
            await asyncio.sleep(3600)
