import asyncio
import contextlib
import threading
import time

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Route
from starlette.templating import Jinja2Templates


# https://stackoverflow.com/a/64521239

class Server(uvicorn.Server):
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


def index(request: Request):
    return templates.TemplateResponse('index.jinja2', {'request': request})


routes = [
    Route("/", index),
    # Route("/miner", miner_stats),
    # Route("/calc", calc),
]

app = Starlette(debug=True, routes=routes)


async def run():
    config = uvicorn.Config("webui:app", reload=True, log_level="info")
    server = Server(config=config)

    with server.run_in_thread():
        while True:
            await asyncio.sleep(3600)
